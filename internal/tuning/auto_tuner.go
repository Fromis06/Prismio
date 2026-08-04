package tuning

import (
	"log/slog"
	"time"

	"my-cdc/internal/config"
	"my-cdc/internal/models"
)

// AutoTuner chịu trách nhiệm tự động điều chỉnh các tham số hệ thống dựa trên số liệu hiệu năng.
type AutoTuner struct {
	Config *config.AppConfig
	Counts *models.EventsCount
}

// NewAutoTuner tạo một instance mới của AutoTuner.
func NewAutoTuner(cfg *config.AppConfig, counts *models.EventsCount) *AutoTuner {
	return &AutoTuner{
		Config: cfg,
		Counts: counts,
	}
}

// Start khởi chạy tiến trình tự động điều chỉnh trong một goroutine riêng.
func (at *AutoTuner) Start() {
	slog.Info("AUTO-TUNER: Starting...")
	go at.runLoop()
}

// runLoop là vòng lặp chính, định kỳ kiểm tra số liệu và điều chỉnh cấu hình.
func (at *AutoTuner) runLoop() {
	// Khoảng thời gian điều chỉnh nên dài hơn khoảng thời gian giám sát
	// để tránh các thay đổi quá thường xuyên.
	interval := time.Duration(at.Config.Monitor.MonitorIntervalSec*2) * time.Second
	if interval <= 0 {
		interval = 10 * time.Second // Giá trị dự phòng
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	var lastInsert, lastUpdate, lastDelete int64

	for range ticker.C {
		// 1. Tính toán EPS hiện tại (logic tương tự monitor)
		currentInsert := at.Counts.InsertCount.Load()
		currentUpdate := at.Counts.UpdateCount.Load()
		currentDelete := at.Counts.DeleteCount.Load()

		totalDelta := (currentInsert - lastInsert) + (currentUpdate - lastUpdate) + (currentDelete - lastDelete)
		eps := float64(totalDelta) / interval.Seconds()

		lastInsert = currentInsert
		lastUpdate = currentUpdate
		lastDelete = currentDelete

		// 2. Áp dụng logic điều chỉnh (hiện tại là placeholder)
		at.applyTuningLogic(eps)
	}
}

// applyTuningLogic chứa logic placeholder để điều chỉnh tham số.
// Hàm này sẽ nhận EPS đã tính và quyết định cần làm gì.
func (at *AutoTuner) applyTuningLogic(eps float64) {
	// Đây là một placeholder. Logic thực tế có thể phức tạp hơn nhiều.
	// Ý tưởng là hàm này sẽ tính toán các giá trị tối ưu mới
	// và sau đó áp dụng chúng bằng các phương thức atomic Store.

	highTrafficThreshold := 10000.0 // Ngưỡng này cũng nên được đưa vào config

	if eps > highTrafficThreshold {
		slog.Info("AUTO-TUNER: Phát hiện lưu lượng cao", "eps", eps, "action", "sử dụng cấu hình cho lưu lượng cao (placeholder)")
	} else {
		slog.Info("AUTO-TUNER: Phát hiện lưu lượng bình thường", "eps", eps, "action", "sử dụng cấu hình chuẩn (placeholder)")
	}
}
