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

// flushPipelineDepth is the capacity of readyChan — the number of fully-built
// batches allowed to sit "ready to flush" at once between collectorLoop and
// flusherLoop.
//
// Kept deliberately small (classic double buffer): while flusherLoop is
// blocked on network RTT executing one readyBatch, collectorLoop keeps
// gathering events and building the NEXT batch instead of sitting idle
// waiting for the flush to return (which was the previous architecture's
// core problem — a single goroutine did both, so RTT directly stalled event
// gathering, preventing batch size from ever growing large enough to
// amortize a high-RTT destination).
//
// A depth of 1 is enough: by the time flusherLoop finishes flushing batch N,
// collectorLoop has typically already finished building batch N+1 and is
// waiting to hand it off (building SQL is far cheaper than a network round
// trip). If collectorLoop tries to hand off batch N+2 before flusherLoop
// has drained N+1, the send on readyChan blocks; this is the desired
// backpressure, propagating naturally up into EventChan / the bag pool,
// exactly like a full BatchMaxSize used to. Memory usage is therefore capped
// at "at most flushPipelineDepth+1 batches in flight" regardless of traffic
// level, instead of growing unbounded if the destination falls behind.
//
// Raising this value trades a bit more memory for tolerance of gathering
// jitter — but it should not be raised without also keeping flusherLoop
// concurrency at exactly 1 (see flusherLoop doc-comment for why).
const flushPipelineDepth = 1

// readyBatch is a fully-built batch of SQL statements, ready to be executed.
// It is the unit handed off from collectorLoop to flusherLoop over readyChan.
type readyBatch struct {
	queries    []string
	args       [][]any
	reason     string // "Batch full" | "Timeout" | "Shutdown"
	checkpoint uint64 // 0 means "no checkpoint to commit with this flush"
}

// DataProcessor manages the entire data processing flow for a specific destination.
//
// Architecture: "1 gather – 1 flush" double-buffer pipeline.
//   - collectorLoop: receives events from EventChan, builds SQL (still
//     fanned out across DataProcessingWorkerCount goroutines like before),
//     and cuts batches according to BatchMaxSize/BatchTimeout. It NEVER
//     blocks on network I/O — cutting a batch just hands it off through
//     readyChan and immediately continues gathering the next one.
//   - flusherLoop: the only goroutine that talks to the destination. It pulls
//     ready batches one at a time and executes them, updating FlushProbe /
//     BatchMaxSize / the checkpoint after each one.
//
// Exactly one flusherLoop must run per DataProcessor — running several in
// parallel would let checkpoints commit out of the order events actually
// occurred in (a later LSN could finish before an earlier one that's
// retrying), and would feed FlushProbe overlapping/interleaved eps samples,
// breaking the single-stable-signal assumption the P&O algorithm depends on.
type DataProcessor struct {
	Name          string                      // Identifier for the Sink.
	Config        *config.AppConfig           // Application configuration.
	Builder       QueryBuilder                // Block for converting ChangeEvent to SQL.
	Executor      DatabaseExecutor            // Block for executing SQL commands.
	EventChan     chan *models.SharedEventBag // Channel for receiving packaged event bags.
	readyChan     chan *readyBatch            // Bounded handoff between collectorLoop and flusherLoop (see flushPipelineDepth).
	stopChan      chan struct{}               // Signal channel to request a stop.
	ctx           context.Context             // Main context of the processor, canceled on Stop().
	cancel        context.CancelFunc          // Function to cancel the above context.
	GlobalState   *models.GlobalState         // Reference to the global state for reporting Checkpoints.
	wg            sync.WaitGroup              // Waits for BOTH collectorLoop and flusherLoop to finish before shutting down.
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
		readyChan:   make(chan *readyBatch, flushPipelineDepth),
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

// Start launches both halves of the pipeline: collectorLoop (gathers/builds)
// and flusherLoop (executes against the destination).
func (dp *DataProcessor) Start() error {
	dp.wg.Add(2)
	go dp.collectorLoop()
	go dp.flusherLoop()
	return nil
}

func (dp *DataProcessor) Stop() error {
	dp.cancel()        // Cancel the context, signaling child operations (like flush) to stop.
	close(dp.stopChan) // Send a stop signal to collectorLoop.
	dp.wg.Wait()       // Wait for collectorLoop (flushes remainder, closes readyChan) AND flusherLoop (drains readyChan until closed).

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

// collectorLoop receives events, fans out SQL-building across
// DataProcessingWorkerCount goroutines (unchanged from before), and cuts
// batches according to BatchMaxSize/BatchTimeout. It NEVER performs network
// I/O itself — enqueue() just hands a fully-built batch off to flusherLoop
// through readyChan and returns immediately, so gathering the next batch is
// never blocked behind a flush's RTT.
func (dp *DataProcessor) collectorLoop() {
	var currentQueries []string
	var currentArgs [][]any

	defer dp.wg.Done()
	defer close(dp.readyChan) // collectorLoop is the sole writer of readyChan — it is responsible for closing it.

	// Track the largest Checkpoint in the batch to update GlobalState.
	var currentLastCheckpoint uint64

	ticker := time.NewTicker(time.Duration(dp.Config.Batch.BatchTimeout.Load()) * time.Millisecond)
	defer ticker.Stop()

	// enqueue cuts exactly the first 'n' elements into a FRESH pool-backed
	// copy (never sharing a backing array with the remainder that stays in
	// currentQueries/currentArgs, which collectorLoop keeps mutating right
	// after this call — sharing would race with flusherLoop reading it) and
	// hands it off through readyChan. The send blocking when flusherLoop is
	// still busy with a previous batch is the intended backpressure — it
	// naturally propagates up into EventChan / the bag pool, the same way a
	// full BatchMaxSize used to.
	enqueue := func(n int64, reason string) {
		if n <= 0 || int64(len(currentQueries)) < n {
			// Update the checkpoint even when the batch is empty but contains dummy events.
			if len(currentQueries) == 0 && currentLastCheckpoint > 0 {
				dp.GlobalState.UpdateCheckpoint(dp.Name, currentLastCheckpoint)
				currentLastCheckpoint = 0
			}
			return
		}

		flushQ := models.QueryPool.Get().([]string)[:0]
		flushA := models.ArgsPool.Get().([][]any)[:0]
		flushQ = append(flushQ, currentQueries[:n]...)
		flushA = append(flushA, currentArgs[:n]...)

		// Only attach the checkpoint to the LAST cut containing the final
		// event of the current buffer part — if there's a remainder after
		// 'n', the checkpoint (corresponding to the last event of the bag)
		// should not be committed yet because that remainder corresponds to
		// events AFTER the held checkpoint, which will be sent in a later cut.
		remaining := int64(len(currentQueries)) - n
		var ckpt uint64
		if remaining == 0 {
			ckpt = currentLastCheckpoint
			currentLastCheckpoint = 0
		}

		// Shrink the accumulation buffer AFTER copying the flushed part out —
		// safe now since flushQ/flushA own independent backing arrays and
		// won't be touched by the append below.
		currentQueries = append(currentQueries[:0], currentQueries[n:]...)
		currentArgs = append(currentArgs[:0], currentArgs[n:]...)

		select {
		case dp.readyChan <- &readyBatch{queries: flushQ, args: flushA, reason: reason, checkpoint: ckpt}:
		case <-dp.ctx.Done():
			// Shutting down mid-send: nobody will consume this batch now,
			// so return its slices to the pool ourselves to avoid a leak.
			models.QueryPool.Put(flushQ[:0])
			models.ArgsPool.Put(flushA[:0])
		}
	}

	for {
		select {
		case <-dp.stopChan:
			// Hand off all remaining buffered events on Shutdown.
			enqueue(int64(len(currentQueries)), "Shutdown")
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

			// Handle buffer overflow with a loop, utilizing the new BatchMaxSize after each enqueue call.
			if int64(len(currentQueries)) >= activeMaxSize {
				ticker.Reset(time.Duration(dp.Config.Batch.BatchTimeout.Load()) * time.Millisecond)
			}
			for int64(len(currentQueries)) >= dp.Config.Batch.BatchMaxSize.Load() {
				n := dp.Config.Batch.BatchMaxSize.Load()
				enqueue(n, "Batch full")
			}
			sharedBag.Done()

		case <-ticker.C:
			if dp.isActive.Load() { // Timeout will also hand off all remaining buffered events.
				enqueue(int64(len(currentQueries)), "Timeout")
			}
			ticker.Reset(time.Duration(dp.Config.Batch.BatchTimeout.Load()) * time.Millisecond)
		}
	}
}

// flusherLoop is the only goroutine per DataProcessor that talks to the
// destination. It drains readyChan strictly in order — one batch at a time,
// never concurrently — so checkpoints always commit in the order events
// actually occurred, and FlushProbe only ever sees one clean, sequential
// stream of eps samples (running several of these in parallel per sink would
// interleave samples from overlapping flushes and break the P&O algorithm's
// single-stable-signal assumption).
//
// Because gathering (collectorLoop) and executing (flusherLoop) now run
// concurrently, RTT spent here blocked on the destination no longer stalls
// event gathering — collectorLoop keeps building the next batch in the
// meantime, which is what allows batch size to actually grow large enough
// to amortize a high-RTT destination instead of staying stuck small.
func (dp *DataProcessor) flusherLoop() {
	defer dp.wg.Done()

	for rb := range dp.readyChan { // Exits cleanly once collectorLoop closes readyChan.
		n := int64(len(rb.queries))

		flushTimeout := time.Duration(dp.Config.Batch.FlushTimeoutMs.Load()) * time.Millisecond
		execCtx, execCancel := context.WithTimeout(dp.ctx, flushTimeout)
		startedAt := time.Now()
		err := utils.DoWithRetry(
			dp.Config.Retry.MaxRetries,
			time.Duration(dp.Config.Retry.BaseDelayMs)*time.Millisecond,
			time.Duration(dp.Config.Retry.MaxDelayTimeMs)*time.Millisecond,
			func() error { return dp.Executor.ExecuteBatch(execCtx, rb.queries, rb.args) },
		)
		execCancel()
		execDuration := time.Since(startedAt)

		if err != nil {
			slog.Error("Disconnecting sink due to critical error", "sink", dp.Name, "error", err)
			dp.isActive.Store(false)
			dp.GlobalState.RemoveSink(dp.Name)
		} else {
			if rb.checkpoint > 0 {
				dp.GlobalState.UpdateCheckpoint(dp.Name, rb.checkpoint)
			}

			var nextTarget int64
			switch rb.reason {
			case "Batch full":
				nextTarget = dp.GlobalState.Probe.RecordFullFlush(n)
			case "Timeout":
				nextTarget = dp.GlobalState.Probe.RecordTimeoutFlush(n)
			default:
				nextTarget = dp.Config.Batch.BatchMaxSize.Load()
			}
			dp.Config.Batch.BatchMaxSize.Store(nextTarget)
		}

		// Diagnostic logging: raw vs smoothed eps and current vs stable
		// batch, so tuning behavior (e.g. under injected network RTT) can be
		// verified from logs instead of guessed at. Kept as a single Info
		// line per flush — cheap relative to the flush itself.
		stats := dp.GlobalState.Probe.LastFlushStats()
		slog.Info("Flushing batch to destination",
			"sink", dp.Name, "reason", rb.reason,
			"query_count", n, "checkpoint_lsn", rb.checkpoint,
			"exec_ms", execDuration.Milliseconds(),
			"raw_eps", stats.RawEPS, "smoothed_eps", stats.SmoothedEPS,
			"batch_current", stats.CurrentSize, "batch_stable", stats.StableSize,
			"next_batch_target", dp.Config.Batch.BatchMaxSize.Load(),
			"error", err,
		)

		models.QueryPool.Put(rb.queries[:0])
		models.ArgsPool.Put(rb.args[:0])
	}
}