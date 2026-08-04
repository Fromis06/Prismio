package app

import (
	"log/slog"
	"my-cdc/internal/logger"
)

// Run starts the application with the GUI.
func Run() {
	logger.Initialize()
	slog.Info("Starting application with GUI...")
	slog.Info("GUI is not implemented yet. This is a placeholder.")
}
