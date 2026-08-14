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

	// idleStaleFactor: nếu KHÔNG có lần flush thực sự nào (Full hoặc
	// Timeout) xảy ra trong vòng idleStaleFactor lần chu kỳ tick gần nhất,
	// coi hệ thống đang idle (không có traffic) thay vì "traffic đang thấp
	// thật". Trước khi có cờ này, tuneTimeout không phân biệt được 2 trường
	// hợp: khi hệ thống rảnh hoàn toàn (không có flush nào), nó vẫn tính
	// lại timeout từ SmoothedEPS/StableBatch ĐÃ ĐÓNG BĂNG từ lần flush cuối
	// cùng trước đó — và vì cả 2 giá trị đó không đổi trong khi thời gian
	// trôi qua, công thức b/eps*1000 luôn cho ra một con số bị đẩy thẳng
	// lên trần maxTimeoutMs, dù nó không phản ánh gì về traffic hiện tại.
	idleStaleFactor = 2
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

	// tickInterval is set once in runLoop and reused by tuneTimeout to
	// decide the idle-detection window (idleStaleFactor * tickInterval).
	tickInterval time.Duration
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
	at.tickInterval = interval

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
		slog.Warn("AUTO-TUNER: Không đọc được RAM hệ thống, bỏ qua RAM guard tick này", "error", err)
		return
	}

	switch at.ramState {
	case ramNormal:
		if v.UsedPercent >= ramCeilingPercent {
			at.ramState = ramThrottled
			at.GlobalState.Probe.SetRAMThrottled(true) // đóng băng probe NGAY — mọi lần flush tiếp theo sẽ không tự giảm thêm nữa
			at.haltBatch(v.UsedPercent, "Chạm trần RAM lần đầu, cắt batch size ngay lập tức")
		}

	case ramThrottled:
		if v.UsedPercent < ramSafeResumePercent {
			at.ramState = ramNormal
			at.GlobalState.Probe.SetRAMThrottled(false)
			slog.Info("AUTO-TUNER: RAM đã về dưới ngưỡng an toàn, cho phép FlushProbe hoạt động trở lại",
				"ram_used_percent", v.UsedPercent)
		} else if v.UsedPercent >= ramCeilingPercent {
			// Vẫn còn trên trần sau ÍT NHẤT MỘT CHU KỲ TICK (~10s) kể từ
			// lần cắt trước — đủ thời gian để backlog có cơ hội xả bớt
			// trước khi cắt thêm. Đây là điểm khác biệt cốt lõi so với
			// trước: cắt tối đa 1 lần MỖI TICK AUTOTUNER, không phải mỗi
			// lần flush (có thể xảy ra hàng chục/hàng trăm lần trong cùng
			// khoảng thời gian đó) — chính là nguyên nhân vòng xoáy tụt
			// batch đã quan sát.
			at.haltBatch(v.UsedPercent, "Vẫn trên trần RAM sau 1 chu kỳ, cắt thêm")
		}
	}
}

// haltBatch cắt nửa batch size hiện tại, chặn ở RAMEmergencyMinBatch (cao
// hơn sàn dò thường của probe) để tránh rơi vào vùng chi phí cố định mỗi
// lần flush chiếm ưu thế — làm throughput sụp thêm thay vì phục hồi.
func (at *AutoTuner) haltBatch(ramPercent float64, note string) {
	cur := at.Config.Batch.BatchMaxSize.Load()
	next := cur / 2
	if next < models.RAMEmergencyMinBatch {
		next = models.RAMEmergencyMinBatch
	}
	at.Config.Batch.BatchMaxSize.Store(next)
	at.GlobalState.Probe.ForceSet(next) // đồng bộ lại probe theo giá trị vừa ép — probe không "cãi lại" ở lần flush kế tiếp
	slog.Warn("AUTO-TUNER: "+note,
		"ram_used_percent", ramPercent, "batch_size_before", cur, "batch_size_after", next)
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
//
// Trước khi retune, kiểm tra xem có flush nào xảy ra gần đây không — nếu hệ
// thống đang idle (không có traffic, xem idleStaleFactor ở trên), giữ
// nguyên timeout hiện tại thay vì tính lại từ SmoothedEPS/StableBatch đã bị
// đóng băng từ lần flush cuối cùng, vốn luôn cho ra kết quả vô nghĩa bị đẩy
// lên trần maxTimeoutMs.
func (at *AutoTuner) tuneTimeout() {
	if at.tickInterval > 0 {
		idleFor := time.Since(at.GlobalState.Probe.LastFlushAt())
		if idleFor > at.tickInterval*idleStaleFactor {
			return
		}
	}

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