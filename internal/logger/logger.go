package logger

import (
	"io"
	"log/slog"
	"os"
)

// Initialize sets up the global structured logger for the application.
// It always writes to stdout, and additionally to any extraWriters given
// (e.g. a TUI log panel), so logs are visible live wherever they're needed.
//
// Uses a TextHandler instead of JSON here: when logs are streamed into a
// TUI panel, human readability matters more than machine-parseable output.
func Initialize(extraWriters ...io.Writer) {
	writers := append([]io.Writer{os.Stdout}, extraWriters...)
	out := io.MultiWriter(writers...)

	handler := slog.NewTextHandler(out, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})
	slog.SetDefault(slog.New(handler))
}