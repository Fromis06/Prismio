package utils

import (
	"fmt"
	"log/slog"
	"net/http"
	_ "net/http/pprof" // [Thêm] Import gói pprof để tự động đăng ký các route /debug/pprof
	"runtime"
	"time"

	"my-cdc/internal/api"
	"my-cdc/internal/config"
	"my-cdc/internal/models"
)

// StartAdaptiveMonitor khởi động một goroutine để theo dõi hiệu năng hệ thống
// và tự động điều chỉnh các tham số cấu hình (auto-tuning).
func StartAdaptiveMonitor(cfg *config.AppConfig, counts *models.EventsCount, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// Bật PPROF HTTP Server ngầm
	go func() {
		port := cfg.Monitor.HttpPort
		if port == 0 {
			port = 8080 // Mặc định nếu chưa cấu hình
		}
		listenAddr := cfg.Monitor.ListenAddress

		// Đăng ký handler cho PPROF và API cấu hình
		configHandler := api.NewConfigHandler(cfg)
		http.Handle("/config", configHandler)

		slog.Info("MONITOR: Bật PPROF", "url", fmt.Sprintf("http://%s:%d/debug/pprof/", listenAddr, port))
		slog.Info("API: Bật endpoint quản lý cấu hình", "url", fmt.Sprintf("http://%s:%d/config", listenAddr, port), "methods", "GET, POST")

		// Dùng slog thay vì log.Println để lỗi (nếu có) của ListenAndServe cũng
		// đi qua cùng handler (stdout + panel log dashboard) như mọi log khác,
		// thay vì ghi thẳng ra os.Stderr và có thể đè lên màn hình TUI.
		if err := http.ListenAndServe(fmt.Sprintf("%s:%d", listenAddr, port), nil); err != nil {
			slog.Error("MONITOR: PPROF/API HTTP server dừng với lỗi", "error", err)
		}
	}()

	var lastInsert, lastUpdate, lastDelete int64
	slog.Info("MONITOR: Đã khởi động", "interval", interval)

	for range ticker.C {
		currentInsert := counts.InsertCount.Load()
		currentUpdate := counts.UpdateCount.Load()
		currentDelete := counts.DeleteCount.Load()

		deltaInsert := currentInsert - lastInsert
		deltaUpdate := currentUpdate - lastUpdate
		deltaDelete := currentDelete - lastDelete

		totalDelta := deltaInsert + deltaUpdate + deltaDelete
		eps := float64(totalDelta) / interval.Seconds()

		lastInsert = currentInsert
		lastUpdate = currentUpdate
		lastDelete = currentDelete

		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		allocMB := m.Alloc / 1024 / 1024
		Sys := m.Sys / 1024 / 1024

		// In ra các thông số hiệu năng hiện tại.
		// Các giá trị này có thể được thay đổi "nóng" từ bên ngoài vì chúng là kiểu atomic.
		liveWorkers := cfg.DataProcessing.DataProcessingWorkerCount.Load()
		liveBatchSize := cfg.Batch.BatchMaxSize.Load()
		liveBatchTimeout := cfg.Batch.BatchTimeout.Load()

		slog.Info("MONITOR",
			"eps", eps,
			"ram_go_mb", allocMB,
			"ram_sys_mb", Sys,
			"workers", liveWorkers,
			"batch_size", liveBatchSize,
			"batch_timeout_ms", liveBatchTimeout,
			"total_events", currentInsert+currentUpdate+currentDelete,
		)
	}
}