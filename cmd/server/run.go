package server

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"my-cdc/internal/app"
	"my-cdc/internal/logger"
	"my-cdc/internal/utils"

	// Đăng ký các Driver (Provider và Consumer)
	_ "my-cdc/internal/capture/postgres"
	_ "my-cdc/internal/sinks/postgres"
)

// Run starts the application in headless server mode.
func Run() {
	logger.Initialize()
	slog.Info("Starting application in Headless Server mode...")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cdcApp := app.Initialize(ctx)

	cdcApp.MultiSink.Start()
	defer cdcApp.MultiSink.Stop()

	go utils.StartAdaptiveMonitor(cdcApp.Config, cdcApp.EventsCount, time.Duration(cdcApp.Config.Monitor.MonitorIntervalSec)*time.Second)
	cdcApp.AutoTuner.Start()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		if err := cdcApp.Listener.Start(ctx, cdcApp.Config.Provider.Source.URL, cdcApp.GlobalState); err != nil && err != context.Canceled {
			slog.Error("Capture stream unexpectedly interrupted", "error", err)
			sigChan <- syscall.SIGINT
		}
	}()

	<-sigChan

	slog.Info("Received stop signal, starting shutdown process")
	cancel()

	cdcApp.Shutdown()

	slog.Info("Shutdown complete. Goodbye!")
}
