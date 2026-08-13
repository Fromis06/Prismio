package tuning

import (
	"log/slog"
	"runtime"
	"time"

	"my-cdc/internal/config"
	"my-cdc/internal/models"
	"my-cdc/internal/sinks"

	"github.com/shirou/gopsutil/v3/mem"
)

const (
	ramCeilingPercent    = 95.0
	ramSafeResumePercent = 85.0

	backlogHighWatermarkSec = 1.5
	backlogLowWatermarkSec  = 0.1
	minWorkers              = 1

	minTimeoutMs int64 = 20
	maxTimeoutMs int64 = 5_000
)

type ramState int

const (
	ramNormal ramState = iota
	ramThrottled
)

// AutoTuner now has only 3 responsibilities, each using its specific signal:
//
//  1. RAM guard (gopsutil) -> signals FlushProbe to back off, immediately cutting BatchMaxSize
//     if an emergency ceiling is exceeded.
//  2. Actual backlog (events waiting in EventChan, measured by event count, not bag count) -> Worker Count.
//  3. Smoothed EPS + CONVERGED batch (not the currently probing batch) from
//     FlushProbe -> Batch Timeout.
//
// The Batch Size PROBING is no longer here — it runs real-time within
// FlushProbe.RecordFlush (models/flush_probe.go), called by DataProcessor
// on each flush instead of waiting for AutoTuner's 10s tick.
type AutoTuner struct {
	// Configuration and state references
	Config      *config.AppConfig
	Counts      *models.EventsCount
	GlobalState *models.GlobalState
	Pipeline    sinks.Pipeline

	ramState ramState
}

func NewAutoTuner(cfg *config.AppConfig, counts *models.EventsCount, state *models.GlobalState, pipeline sinks.Pipeline) *AutoTuner {
	return &AutoTuner{
		Config:      cfg,
		Counts:      counts,
		GlobalState: state,
		Pipeline:    pipeline,
		ramState:    ramNormal,
	}
}

func (at *AutoTuner) Start() {
	slog.Info("AUTO-TUNER: Starting...")
	go at.runLoop()
}

func (at *AutoTuner) runLoop() {
	interval := time.Duration(at.Config.Monitor.MonitorIntervalSec*2) * time.Second
	if interval <= 0 {
		interval = 10 * time.Second
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for range ticker.C {
		at.checkRAMGuard()
		at.tuneWorkerCount()
		at.tuneTimeout()

		slog.Info("AUTO-TUNER",
			"ram_state", at.ramStateLabel(),
			"pending_events", at.Pipeline.PendingEvents(),
			"workers", at.Config.DataProcessing.DataProcessingWorkerCount.Load(),
			"batch_size", at.Config.Batch.BatchMaxSize.Load(), // updated by FlushProbe in real-time, read here for logging only
			"batch_timeout_ms", at.Config.Batch.BatchTimeout.Load(),
			"smoothed_eps", at.GlobalState.Probe.SmoothedEPS(),
		)
	}
}

func (at *AutoTuner) checkRAMGuard() {
	v, err := mem.VirtualMemory()
	if err != nil {
		slog.Warn("AUTO-TUNER: Failed to read system RAM, skipping RAM guard tick", "error", err)
		return
	}

	switch at.ramState {
	case ramNormal:
		if v.UsedPercent >= ramCeilingPercent {
			at.ramState = ramThrottled
			at.GlobalState.Probe.SetRAMThrottled(true)
			cur := at.Config.Batch.BatchMaxSize.Load()
			next := cur / 2
			if next < models.SafeMinBatch {
				next = models.SafeMinBatch
			}
			at.Config.Batch.BatchMaxSize.Store(next)
			slog.Warn("AUTO-TUNER: RAM ceiling reached, immediately cutting batch size",
				"ram_used_percent", v.UsedPercent, "batch_size_before", cur, "batch_size_after", next)
		}
	case ramThrottled:
		if v.UsedPercent < ramSafeResumePercent {
			at.ramState = ramNormal
			at.GlobalState.Probe.SetRAMThrottled(false)
			slog.Info("AUTO-TUNER: RAM is below safe threshold, allowing FlushProbe to increase batch again",
				"ram_used_percent", v.UsedPercent)
		}
	}
}

func (at *AutoTuner) ramStateLabel() string {
	if at.ramState == ramThrottled {
		return "throttled"
	}
	return "normal"
}

func (at *AutoTuner) tuneWorkerCount() {
	pending := at.Pipeline.PendingEvents()
	eps := at.GlobalState.Probe.SmoothedEPS()
	backlogSec := float64(pending) / max(eps, 1)

	workers := at.Config.DataProcessing.DataProcessingWorkerCount.Load()
	switch {
	case backlogSec > backlogHighWatermarkSec && int(workers) < runtime.NumCPU():
		at.Config.DataProcessing.DataProcessingWorkerCount.Store(workers + 1)
		slog.Info("AUTO-TUNER: Backlog exceeded threshold, increasing worker count",
			"backlog_sec", backlogSec, "pending_events", pending, "workers", workers+1)

	case backlogSec < backlogLowWatermarkSec && workers > minWorkers:
		at.Config.DataProcessing.DataProcessingWorkerCount.Store(workers - 1)
		slog.Info("AUTO-TUNER: Backlog low, decreasing worker count",
			"backlog_sec", backlogSec, "workers", workers-1)
	}
}

// tuneTimeout DELIBERATELY does not read the current BatchMaxSize (which fluctuates
// continuously because FlushProbe is probing), but instead uses StableBatch() (the CONVERGED estimate)
// along with SmoothedEPS() (instead of instantaneous EPS) — separating these two quantities from
// the batch probing loop, solving the "alternating large/small batch" problem encountered previously.
func (at *AutoTuner) tuneTimeout() {
	eps := at.GlobalState.Probe.SmoothedEPS()
	b, ok := at.GlobalState.Probe.StableBatch()
	if !ok || eps <= 0 {
		return // not enough stable data, keep current timeout instead of guessing
	}

	timeoutMs := int64(float64(b) / eps * 1000.0)
	if timeoutMs < minTimeoutMs {
		timeoutMs = minTimeoutMs
	}
	if timeoutMs > maxTimeoutMs {
		timeoutMs = maxTimeoutMs
	}
	at.Config.Batch.BatchTimeout.Store(timeoutMs)
}
