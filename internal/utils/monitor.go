package utils

import (
	"fmt"
	"log/slog"
	"net/http"
	_ "net/http/pprof" // Import for side-effect: registers pprof handlers.
	"runtime"
	"time"

	"my-cdc/internal/api"
	"my-cdc/internal/config"
	"my-cdc/internal/models"
)

// StartAdaptiveMonitor starts a background goroutine to monitor system performance.
// It logs key metrics at a regular interval and exposes HTTP endpoints for
// pprof and live configuration changes.
func StartAdaptiveMonitor(cfg *config.AppConfig, counts *models.EventsCount, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// Start the HTTP server in a separate goroutine.
	go func() {
		port := cfg.Monitor.HttpPort
		if port == 0 {
			port = 8080 // Default port if not configured.
		}
		listenAddr := cfg.Monitor.ListenAddress

		configHandler := api.NewConfigHandler(cfg)
		http.Handle("/config", configHandler)

		slog.Info("MONITOR: Bật PPROF", "url", fmt.Sprintf("http://%s:%d/debug/pprof/", listenAddr, port))
		slog.Info("API: Bật endpoint quản lý cấu hình", "url", fmt.Sprintf("http://%s:%d/config", listenAddr, port), "methods", "GET, POST")

		// Use slog so that any server errors are routed through the same logging
		// pipeline as the rest of the application (e.g., to the TUI panel),
		// instead of writing to stderr and potentially corrupting the UI.
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

		// These configuration values are atomic, so they can be changed live
		// via the /config API endpoint.
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
