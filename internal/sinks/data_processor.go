package sinks

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"my-cdc/internal/config"
	"my-cdc/internal/models"
	"my-cdc/internal/pb"
	"my-cdc/internal/utils"
)

// DataProcessor manages the entire data processing flow for a specific destination.
type DataProcessor struct {
	Name          string                      // Identifier for the Sink.
	Config        *config.AppConfig           // Application configuration.
	Builder       QueryBuilder                // Block for converting ChangeEvent to SQL.
	Executor      DatabaseExecutor            // Block for executing SQL commands.
	EventChan     chan *models.SharedEventBag // Channel for receiving packaged event bags.
	stopChan      chan struct{}               // Signal channel to request a stop.
	ctx           context.Context             // Main context of the processor, canceled on Stop().
	cancel        context.CancelFunc          // Function to cancel the above context.
	GlobalState   *models.GlobalState         // Reference to the global state for reporting Checkpoints.
	wg            sync.WaitGroup              // WaitGroup to wait for the workerLoop to finish before shutting down.
	isActive      atomic.Bool                 // Live/dead state of the Sink.
	pendingEvents atomic.Int64
}

func NewDataProcessor(name string, cfg *config.AppConfig, builder QueryBuilder, executor DatabaseExecutor, globalState *models.GlobalState) *DataProcessor {
	ctx, cancel := context.WithCancel(context.Background())
	dp := &DataProcessor{
		Name:        name,
		Config:      cfg,
		Builder:     builder,
		Executor:    executor,
		EventChan:   make(chan *models.SharedEventBag, cfg.Pipeline.PipelineMaxSize.Load()),
		stopChan:    make(chan struct{}),
		ctx:         ctx,
		cancel:      cancel,
		GlobalState: globalState,
	}
	dp.isActive.Store(true)
	return dp
}

// WriteBatch is a method of the Pipeline interface. It is used in a
// one-to-one scenario (1 producer -> 1 consumer). It will automatically package the event bag with
// a reference count of 1 and send it to the processing channel.
func (dp *DataProcessor) WriteBatch(events []*pb.ChangeEvent) error { // Implements Pipeline interface
	if len(events) > 0 {
		// Automatically package with a reference count of 1, as this is the only consumer.
		dp.WriteShared(models.NewSharedEventBag(events, 1))
	}
	return nil
}

// WriteShared is the method called by MultiSink to send a packaged event bag
// (with a reference counter) to the processing channel.
func (dp *DataProcessor) WriteShared(bag *models.SharedEventBag) {
	dp.pendingEvents.Add(int64(len(bag.Events)))
	dp.EventChan <- bag
}

// Start starts the main processing goroutine (workerLoop).
func (dp *DataProcessor) Start() error {
	dp.wg.Add(1)
	go dp.workerLoop()
	return nil
}

func (dp *DataProcessor) Stop() error {
	dp.cancel()        // Cancel the context, signaling child operations (like flush) to stop.
	close(dp.stopChan) // Send a stop signal to the workerLoop.
	dp.wg.Wait()       // Wait for the workerLoop to flush remaining data and finish.

	// Flush any remaining event bags in the channel to avoid memory leaks.
	close(dp.EventChan)
	for sharedBag := range dp.EventChan {
		sharedBag.Done() // Call Done() to decrement the refCount and possibly return the bag to the pool.
	}

	return dp.Executor.Close() // Finally, close the physical connection.
}

// IsActive returns the operational state of the DataProcessor.
func (dp *DataProcessor) IsActive() bool { // Implements Pipeline interface
	return dp.isActive.Load()
}

func (dp *DataProcessor) PendingEvents() int64 {
	return dp.pendingEvents.Load()
}

// workerLoop is the main goroutine that handles event processing, batching, and writing to the destination.
// workerLoop is the main goroutine that handles event processing, batching, and writing to the destination.
func (dp *DataProcessor) workerLoop() {
	var currentQueries []string
	var currentArgs [][]any

	defer dp.wg.Done()

	// Track the largest Checkpoint in the batch to update GlobalState.
	var currentLastCheckpoint uint64

	ticker := time.NewTicker(time.Duration(dp.Config.Batch.BatchTimeout.Load()) * time.Millisecond)
	defer ticker.Stop()

	// flushExact flushes exactly the first 'n' elements and retains the remainder.
	flushExact := func(n int64, reason string) {
		if n <= 0 || int64(len(currentQueries)) < n {
			// Vẫn cần cập nhật checkpoint nếu batch hoàn toàn rỗng nhưng có dummy events.
			// Still need to update checkpoint if the batch is completely empty but has dummy events.
			if len(currentQueries) == 0 && currentLastCheckpoint > 0 {
				dp.GlobalState.UpdateCheckpoint(dp.Name, currentLastCheckpoint)
				currentLastCheckpoint = 0
			}
			return
		}

		queries := currentQueries[:n]
		args := currentArgs[:n]
		// Only attach the checkpoint to the LAST flush containing the final event
		// of the current buffer part — if there's a remainder after 'n', the checkpoint
		// (corresponding to the last event of the bag) should not be committed yet because
		// that remainder corresponds to events AFTER the held checkpoint, which will be sent
		// in the next flush cycle.
		remaining := int64(len(currentQueries)) - n
		var ckpt uint64
		if remaining == 0 {
			ckpt = currentLastCheckpoint
		}

		slog.Info("Flushing batch to destination",
			"sink", dp.Name, "reason", reason,
			"query_count", len(queries), "checkpoint_lsn", ckpt,
		)

		flushTimeout := time.Duration(dp.Config.Batch.FlushTimeoutMs.Load()) * time.Millisecond
		execCtx, execCancel := context.WithTimeout(dp.ctx, flushTimeout)
		err := utils.DoWithRetry(
			dp.Config.Retry.MaxRetries,
			time.Duration(dp.Config.Retry.BaseDelayMs)*time.Millisecond,
			time.Duration(dp.Config.Retry.MaxDelayTimeMs)*time.Millisecond,
			func() error { return dp.Executor.ExecuteBatch(execCtx, queries, args) },
		)
		execCancel()

		if err != nil {
			slog.Error("Disconnecting sink due to critical error", "sink", dp.Name, "error", err)
			dp.isActive.Store(false)
			dp.GlobalState.RemoveSink(dp.Name)
		} else {
			if ckpt > 0 {
				dp.GlobalState.UpdateCheckpoint(dp.Name, ckpt)
				currentLastCheckpoint = 0
			}

			var nextTarget int64
			switch reason {
			case "Batch full":
				nextTarget = dp.GlobalState.Probe.RecordFullFlush(n)
			case "Timeout":
				nextTarget = dp.GlobalState.Probe.RecordTimeoutFlush(n)
			default:
				nextTarget = dp.Config.Batch.BatchMaxSize.Load()
			}
			dp.Config.Batch.BatchMaxSize.Store(nextTarget)
		}

		// Truncate the flushed portion, keep the remainder for the next cycle.
		currentQueries = append(currentQueries[:0], currentQueries[n:]...)
		currentArgs = append(currentArgs[:0], currentArgs[n:]...)
	}

	for {
		select {
		case <-dp.stopChan:
			// Flush all remaining buffered events on Shutdown
			flushExact(int64(len(currentQueries)), "Shutdown")
			return

		case sharedBag := <-dp.EventChan:
			dp.pendingEvents.Add(-int64(len(sharedBag.Events)))
			if !dp.isActive.Load() {
				sharedBag.Done()
				continue
			}

			eventsBuffer := sharedBag.Events

			activeMaxSize := dp.Config.Batch.BatchMaxSize.Load()
			numWorkers := int(dp.Config.DataProcessing.DataProcessingWorkerCount.Load())

			if len(eventsBuffer) > 0 {
				lastEvent := eventsBuffer[len(eventsBuffer)-1]
				lastCheckPoint := lastEvent.GetOffset().GetLsn()

				if lastCheckPoint > currentLastCheckpoint {
					currentLastCheckpoint = lastCheckPoint
				}
			}

			var wg sync.WaitGroup
			workerQueries := make([][]string, numWorkers)
			workerArgs := make([][][]any, numWorkers)
			chunkSize := (len(eventsBuffer) + numWorkers - 1) / numWorkers

			for w := 0; w < numWorkers; w++ {
				wg.Add(1)
				go func(workerID int) {
					defer wg.Done()
					wStart := workerID * chunkSize
					wEnd := wStart + chunkSize
					if wStart >= len(eventsBuffer) {
						return
					}
					if wEnd > len(eventsBuffer) {
						wEnd = len(eventsBuffer)
					}
					subChunk := eventsBuffer[wStart:wEnd]

					localQueries := models.QueryPool.Get().([]string)[:0]
					localArgs := models.ArgsPool.Get().([][]any)[:0]

					for _, e := range subChunk {
						q, a := dp.Builder.BuildQuery(e)

						if q != "" {
							localQueries = append(localQueries, q)
							localArgs = append(localArgs, a)
						}
					}
					workerQueries[workerID] = localQueries
					workerArgs[workerID] = localArgs
				}(w)
			}

			wg.Wait()

			for i := 0; i < numWorkers; i++ {
				currentQueries = append(currentQueries, workerQueries[i]...)
				currentArgs = append(currentArgs, workerArgs[i]...)

				models.QueryPool.Put(workerQueries[i])
				models.ArgsPool.Put(workerArgs[i])
			}

			// Handle buffer overflow with a loop, utilizing the new BatchMaxSize after each flushExact call
			if int64(len(currentQueries)) >= activeMaxSize {
				ticker.Reset(time.Duration(dp.Config.Batch.BatchTimeout.Load()) * time.Millisecond)
			}
			for int64(len(currentQueries)) >= dp.Config.Batch.BatchMaxSize.Load() {
				n := dp.Config.Batch.BatchMaxSize.Load()
				flushExact(n, "Batch full")
			}
			sharedBag.Done()

		case <-ticker.C:
			if dp.isActive.Load() { // Timeout will also flush all remaining buffered events
				flushExact(int64(len(currentQueries)), "Timeout")
			}
			ticker.Reset(time.Duration(dp.Config.Batch.BatchTimeout.Load()) * time.Millisecond)
		}
	}
}
