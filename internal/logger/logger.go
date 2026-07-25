package logger

import (
	"log/slog"
	"os"
)

// Initialize sets up the global structured logger for the application.
// It configures slog to output JSON to stdout with an Info level.
func Initialize() {
	// For production, a JSON handler is best for machine readability.
	// For development, you could switch to NewTextHandler.
	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo, // Can be configured to slog.LevelDebug for more verbose output.
	})
	slog.SetDefault(slog.New(handler))
}