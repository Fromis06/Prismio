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
	Name        string                      // Identifier for the Sink.
	Config      *config.AppConfig           // Application configuration.
	Builder     QueryBuilder                // Block for converting ChangeEvent to SQL.
	Executor    DatabaseExecutor            // Block for executing SQL commands.
	EventChan   chan *models.SharedEventBag // Channel for receiving packaged event bags.
	stopChan    chan struct{}               // Signal channel to request a stop.
	ctx         context.Context             // Main context of the processor, canceled on Stop().
	cancel      context.CancelFunc          // Function to cancel the above context.
	GlobalState *models.GlobalState         // Reference to the global state for reporting Checkpoints.
	wg          sync.WaitGroup              // WaitGroup to wait for the worker to finish before shutting down.
	isActive    atomic.Bool                 // Live/dead state of the Sink.
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
func (dp *DataProcessor) WriteBatch(events []*pb.ChangeEvent) error {
	if len(events) > 0 {
		// Automatically package with a reference count of 1, as this is the only consumer.
		dp.WriteShared(models.NewSharedEventBag(events, 1))
	}
	return nil
}

// WriteShared is the method called by MultiSink to send a packaged event bag
// (with a reference counter) to the processing channel.
func (dp *DataProcessor) WriteShared(bag *models.SharedEventBag) { // Implements Pipeline interface
	// No need to check len, as MultiSink has already done it.
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

// workerLoop is the main goroutine that handles event processing, batching, and writing to the destination.
func (dp *DataProcessor) workerLoop() {
	var currentQueries []string
	var currentArgs [][]any

	defer dp.wg.Done()

	// Track the largest Checkpoint in the batch to update GlobalState.
	var CurrentLastCheckpoint uint64

	initialTimeout := dp.Config.Batch.BatchTimeout.Load()
	ticker := time.NewTicker(time.Duration(initialTimeout) * time.Millisecond)
	defer ticker.Stop()

	// flush is an internal function to perform writing a batch of data to the destination DB.
	flush := func(reason string) {
		if len(currentQueries) == 0 {
			// Still update the checkpoint if there is one, even if there are no SQL statements (e.g., only a dummy event).
			if CurrentLastCheckpoint > 0 {
				dp.GlobalState.UpdateCheckpoint(dp.Name, CurrentLastCheckpoint)
				CurrentLastCheckpoint = 0
			}
			return
		}
		slog.Info("Flushing batch to destination",
			"sink", dp.Name,
			"reason", reason,
			"query_count", len(currentQueries),
			"checkpoint_lsn", CurrentLastCheckpoint,
		)
		// Use the processor's context (dp.ctx) so that if Stop() is called, this context will be canceled,
		// helping to interrupt any running or waiting ExecuteBatch commands.
		flushTimeout := time.Duration(dp.Config.Batch.FlushTimeoutMs.Load()) * time.Millisecond
		execCtx, execCancel := context.WithTimeout(dp.ctx, flushTimeout)
		defer execCancel()

		err := utils.DoWithRetry(
			dp.Config.Retry.MaxRetries,
			time.Duration(dp.Config.Retry.BaseDelayMs)*time.Millisecond,
			time.Duration(dp.Config.Retry.MaxDelayTimeMs)*time.Millisecond,
			func() error {
				return dp.Executor.ExecuteBatch(execCtx, currentQueries, currentArgs)
			},
		)
		if err != nil {
			// Critical error, disconnect this sink to not affect other sinks.
			slog.Error("Disconnecting sink due to critical error", "sink", dp.Name, "error", err)
			dp.isActive.Store(false)
			dp.GlobalState.RemoveSink(dp.Name)
		} else {
			// Write successful, update the checkpoint in GlobalState.
			if CurrentLastCheckpoint > 0 {
				dp.GlobalState.UpdateCheckpoint(dp.Name, CurrentLastCheckpoint)
			}
		}

		// Reset the buffer for the next batch.
		currentQueries = currentQueries[:0]
		currentArgs = currentArgs[:0]
		CurrentLastCheckpoint = 0
	}

	for {
		select {
		case <-dp.stopChan:
			flush("Shutdown") // Before exiting, flush any remaining data.
			return

		case sharedBag := <-dp.EventChan:
			// If the sink has been disabled, discard the event and call Done()
			// to not block other sinks and to avoid memory leaks.
			if !dp.isActive.Load() {
				sharedBag.Done()
				continue
			}

			eventsBuffer := sharedBag.Events

			activeMaxSize := dp.Config.Batch.BatchMaxSize.Load()
			numWorkers := int(dp.Config.DataProcessing.DataProcessingWorkerCount.Load())

			// Get the Checkpoint from the last event in the bag.
			if len(eventsBuffer) > 0 {
				lastEvent := eventsBuffer[len(eventsBuffer)-1]
				LastCheckPoint := lastEvent.GetOffset().GetLsn()

				if LastCheckPoint > CurrentLastCheckpoint {
					CurrentLastCheckpoint = LastCheckPoint
				}
			}

			var wg sync.WaitGroup
			workerQueries := make([][]string, numWorkers)
			workerArgs := make([][][]any, numWorkers)
			chunkSize := (len(eventsBuffer) + numWorkers - 1) / numWorkers

			// Divide the event bag into smaller chunks for parallel processing by workers (Fan-Out).
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

					// Reuse a slice from the pool to reduce memory allocation.
					localQueries := models.QueryPool.Get().([]string)[:0]
					localArgs := models.ArgsPool.Get().([][]any)[:0]

					for _, e := range subChunk {
						// Convert ChangeEvent to SQL statement.
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

			// Aggregate the results (SQL queries) from the workers into the main buffer (Fan-In).
			for i := 0; i < numWorkers; i++ {
				currentQueries = append(currentQueries, workerQueries[i]...)
				currentArgs = append(currentArgs, workerArgs[i]...)

				// Return the used slices to the pool.
				models.QueryPool.Put(workerQueries[i])
				models.ArgsPool.Put(workerArgs[i])
			}
			if int64(len(currentQueries)) >= activeMaxSize {
				flush("Batch full")
				ticker.Reset(time.Duration(dp.Config.Batch.BatchTimeout.Load()) * time.Millisecond)
			}

			// Notify the SharedEventBag that this sink has finished processing the event bag.
			sharedBag.Done()

		case <-ticker.C:
			if dp.isActive.Load() {
				flush("Timeout")
			}
			ticker.Reset(time.Duration(dp.Config.Batch.BatchTimeout.Load()) * time.Millisecond)
		}
	}
}
