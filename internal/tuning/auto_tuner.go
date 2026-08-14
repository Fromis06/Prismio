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
//  1. RAM guard (gopsutil) -> signals GlobalState.SetRAMThrottled, pausing the
//     Listener from pulling new WAL data from the source (see
//     internal/capture/postgres/listener.go) until RAM drops back to a safe
//     level. Deliberately does NOT touch BatchMaxSize anymore: cutting batch
//     size only hurts flush throughput (especially over high-RTT links to
//     the sink) without addressing the actual cause of RAM growth (ingest
//     rate outpacing flush rate) — over high RTT it made backlog/RAM grow
//     even faster, which is what this replaces.
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

// checkRAMGuard implements RAM backpressure at the INPUT side instead of
// cutting BatchMaxSize at the output side (see the AutoTuner doc-comment and
// GlobalState.SetRAMThrottled for the full rationale). When RAM crosses the
// ceiling, the Listener is signaled to pause pulling new WAL data; when it
// drops back below the (lower, hysteresis) resume threshold, the Listener
// resumes. Flush batch size / throughput at the sink is never touched here.
func (at *AutoTuner) checkRAMGuard() {
	v, err := mem.VirtualMemory()
	if err != nil {
		slog.Warn("AUTO-TUNER: Không đọc được RAM hệ thống, bỏ qua RAM guard tick này", "error", err)
		return
	}

	switch at.ramState {
	case ramNormal:
		if v.UsedPercent >= ramCeilingPercent {
			at.ramState = ramThrottled
			at.GlobalState.SetRAMThrottled(true)
			slog.Warn("AUTO-TUNER: Chạm trần RAM, tạm dừng nạp dữ liệu mới từ nguồn (Listener) cho tới khi RAM về ngưỡng an toàn — batch size và tốc độ xả sink KHÔNG bị ảnh hưởng",
				"ram_used_percent", v.UsedPercent)
		}

	case ramThrottled:
		if v.UsedPercent < ramSafeResumePercent {
			at.ramState = ramNormal
			at.GlobalState.SetRAMThrottled(false)
			slog.Info("AUTO-TUNER: RAM đã về dưới ngưỡng an toàn, cho phép Listener tiếp tục nạp dữ liệu từ nguồn",
				"ram_used_percent", v.UsedPercent)
		}
		// Vẫn trên trần: giữ nguyên trạng thái throttled, không cần làm gì
		// thêm mỗi tick — Listener đã tự dừng nạp (xem waitForRAMRecovery
		// trong internal/capture/postgres/listener.go). Backlog sẽ tự vơi
		// dần vì các sink vẫn tiếp tục xả với BatchMaxSize/throughput
		// nguyên vẹn, không bị cắt giảm như cơ chế cũ.
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