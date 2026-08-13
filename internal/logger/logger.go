package logger

import (
	"context"
	"io"
	"log/slog"
	"os"
	"sync"
)

// tuiHandler wraps the default slog.Handler to customize time formatting
type tuiHandler struct {
	slog.Handler
	out io.Writer
	mu  *sync.Mutex
}

func (h *tuiHandler) Handle(ctx context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	timeStr := r.Time.Format("2006-01-02 15:04:05.000 ")
	h.out.Write([]byte(timeStr))

	return h.Handler.Handle(ctx, r)
}

func (h *tuiHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &tuiHandler{
		Handler: h.Handler.WithAttrs(attrs),
		out:     h.out,
		mu:      h.mu,
	}
}

func (h *tuiHandler) WithGroup(name string) slog.Handler {
	return &tuiHandler{
		Handler: h.Handler.WithGroup(name),
		out:     h.out,
		mu:      h.mu,
	}
}

// Initialize sets up the global structured logger for the application.
// It always writes to stdout, and additionally to any extraWriters given
// (e.g. a TUI log panel), so logs are visible live wherever they're needed.
//
// Uses a custom wrapper over TextHandler to ensure human readability
// without the visual clutter of standard slog time keys.
func Initialize(extraWriters ...io.Writer) {
	var out io.Writer
	if len(extraWriters) > 0 {
		out = io.MultiWriter(extraWriters...)
	} else {
		out = os.Stdout
	}

	mu := &sync.Mutex{}

	textHandler := slog.NewTextHandler(out, &slog.HandlerOptions{
		Level: slog.LevelInfo,
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			if a.Key == slog.TimeKey {
				return slog.Attr{}
			}
			return a
		},
	})

	handler := &tuiHandler{
		Handler: textHandler,
		out:     out,
		mu:      mu,
	}

	slog.SetDefault(slog.New(handler))
}
