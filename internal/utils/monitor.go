package utils

import (
	"log/slog"
	"runtime"
	"time"

	"my-cdc/internal/config"
	"my-cdc/internal/models"
)

func StartAdaptiveMonitor(cfg *config.AppConfig, counts *models.EventsCount, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// ---- DISABLED: remote HTTP server (pprof + /config) ----
	//
	// go func() {
	// 	port := cfg.Monitor.HttpPort
	// 	if port == 0 {
	// 		port = 8080 // Default port if not configured.
	// 	}
	// 	listenAddr := cfg.Monitor.ListenAddress
	//
	// 	configHandler := api.NewConfigHandler(cfg)
	// 	http.Handle("/config", configHandler)
	//
	// 	slog.Info("MONITOR: Enabling PPROF", "url", fmt.Sprintf("http://%s:%d/debug/pprof/", listenAddr, port))
	// 	slog.Info("API: Enabling config management endpoint", "url", fmt.Sprintf("http://%s:%d/config", listenAddr, port), "methods", "GET, POST")
	//
	// 	if err := http.ListenAndServe(fmt.Sprintf("%s:%d", listenAddr, port), nil); err != nil {
	// 		slog.Error("MONITOR: PPROF/API HTTP server stopped with error", "error", err)
	// 	}
	// }()

	var lastInsert, lastUpdate, lastDelete int64
	slog.Info("MONITOR: Started", "interval", interval)

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

		// These values are atomic so they can still be adjusted "live" — but
		// currently they can only be adjusted via the TUI (cmd/cli/config_form.go),
		// no longer via HTTP /config until that feature is re-enabled.
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