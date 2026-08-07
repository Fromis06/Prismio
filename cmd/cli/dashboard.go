package cli

import (
	"context"
	"fmt"
	"time"

	"my-cdc/internal/app"

	"github.com/rivo/tview"
)

// Dashboard encapsulates the TUI layout and references to live-updating panels.
// This allows StartLiveUpdates to write to the correct views without needing to know
// the external Flex layout structure.
type Dashboard struct {
	Layout      *tview.Flex
	statusPanel *tview.TextView
	logPanel    *tview.TextView
}

// NewDashboard creates the main dashboard layout. It is called before the application
// bootstrap is complete, so it doesn't need an *app.Application instance yet.
// Live data will be fed in via StartLiveUpdates.
func NewDashboard(tuiApp *tview.Application) *Dashboard {
	statusPanel := tview.NewTextView().
		SetDynamicColors(true).
		SetText("Initializing...")
	statusPanel.SetBorder(true).SetTitle(" System Status ")

	logPanel := tview.NewTextView().
    SetDynamicColors(true).
    SetScrollable(true).
    SetChangedFunc(func() {
        tuiApp.QueueUpdateDraw(func() {})
    })
	logPanel.SetBorder(true).SetTitle(" Logs ")

	layout := tview.NewFlex().SetDirection(tview.FlexRow). // Fixed height of 5 rows, enough for the status metrics.
								AddItem(statusPanel, 5, 0, false). // The rest of the space is for logs, which can be focused.
								AddItem(logPanel, 0, 1, true)

	return &Dashboard{
		Layout:      layout,
		statusPanel: statusPanel,
		logPanel:    logPanel,
	}
}

// LogWriter returns an io.Writer that directs output to the logPanel.
// This is used to ensure logs are displayed within the TUI instead of interfering
// with it by writing to the standard output, which tview controls.
func (d *Dashboard) LogWriter() *tview.TextView {
	return d.logPanel
}

// StartLiveUpdates launches a background goroutine that periodically calculates
// EPS, runtime, and total event counts, then updates the status panel.
// It stops when the provided context is canceled.
func (d *Dashboard) StartLiveUpdates(ctx context.Context, tuiApp *tview.Application, cdcApp *app.Application, interval time.Duration) {
	startTime := time.Now()

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		var lastInsert, lastUpdate, lastDelete int64

		for {
			select {
			case <-ctx.Done():
				return

			case <-ticker.C:
				insert := cdcApp.EventsCount.InsertCount.Load()
				update := cdcApp.EventsCount.UpdateCount.Load()
				del := cdcApp.EventsCount.DeleteCount.Load()
				total := insert + update + del

				deltaTotal := (insert - lastInsert) + (update - lastUpdate) + (del - lastDelete)
				eps := float64(deltaTotal) / interval.Seconds()

				lastInsert, lastUpdate, lastDelete = insert, update, del
				elapsed := time.Since(startTime).Round(time.Second)

				// All UI updates must be queued via QueueUpdateDraw because this code
				// runs in a background goroutine, not the main tview event loop.
				tuiApp.QueueUpdateDraw(func() {
					d.statusPanel.SetText(fmt.Sprintf(
						"[yellow]EPS:[-]           %.0f\n"+
							"[yellow]Runtime:[-]       %s\n"+
							"[yellow]Total Events:[-]  %d  [gray](Insert: %d | Update: %d | Delete: %d)[-]",
						eps, elapsed, total, insert, update, del,
					))
				})
			}
		}
	}()
}
