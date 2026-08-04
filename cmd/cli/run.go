package cli

import (
	"log/slog"

	"my-cdc/internal/logger"
)

// Run starts the application with the Terminal User Interface.
func Run() {
	logger.Initialize()
	slog.Info("Starting application with Terminal UI...")
	slog.Info("TUI is not implemented yet. This is a placeholder.")

	// In the future, this is where the tview application would be initialized and run.
	// For now, we just block to simulate a running TUI app.
	select {}
}
