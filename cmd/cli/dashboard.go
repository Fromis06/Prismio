package cli

import (
	"context"
	"fmt"
	"io"
	"time"

	"my-cdc/internal/app"

	"github.com/rivo/tview"
)
// Dashboard giữ tham chiếu tới các panel để có thể cập nhật "sống" (live)
// từ một goroutine nền, thay vì chỉ là layout tĩnh.
type Dashboard struct {
	Layout *tview.Flex

	statusPanel     *tview.TextView
	connectionPanel *tview.TextView
	tuningPanel     *tview.Table
	logPanel        *tview.TextView

	tuiApp *tview.Application

	startedAt                          time.Time
	lastInsert, lastUpdate, lastDelete int64
}

// NewDashboard chỉ dựng layout tĩnh, chưa cần Application vì lúc TUI khởi
// động app chưa Bootstrap xong. Dữ liệu thật được đổ vào sau qua StartLiveUpdates.
func NewDashboard(tuiApp *tview.Application) *Dashboard {
	d := &Dashboard{tuiApp: tuiApp}

	d.statusPanel = tview.NewTextView().SetDynamicColors(true).
		SetText("Waiting for pipeline to start...")
	d.statusPanel.SetBorder(true).SetTitle(" System Status ")

	d.connectionPanel = tview.NewTextView().SetDynamicColors(true)
	d.connectionPanel.SetBorder(true).SetTitle(" Connectivity ")

	d.tuningPanel = tview.NewTable().SetBorders(true)
	d.tuningPanel.SetBorder(true).SetTitle(" Live Tuning ")
	d.tuningPanel.SetCell(0, 0, tview.NewTableCell("Parameter").SetSelectable(false))
	d.tuningPanel.SetCell(0, 1, tview.NewTableCell("Value").SetSelectable(false))

	d.logPanel = tview.NewTextView().SetDynamicColors(true).SetScrollable(true)
	d.logPanel.SetBorder(true).SetTitle(" Activity / Logs ")

	// TextView.Write is safe to call from any goroutine (unlike most other
	// tview methods). SetChangedFunc fires after each write and is the
	// documented way to trigger a redraw from a background goroutine —
	// e.g. slog writing here from the Bootstrap goroutine.
	d.logPanel.SetChangedFunc(func() {
		d.logPanel.ScrollToEnd()
		d.tuiApp.Draw()
	})

	leftColumn := tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(d.statusPanel, 0, 1, false).
		AddItem(d.connectionPanel, 0, 1, false)

	mainContent := tview.NewFlex().
		AddItem(leftColumn, 0, 1, false).
		AddItem(d.tuningPanel, 0, 1, false)

	d.Layout = tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(mainContent, 0, 3, false).
		AddItem(d.logPanel, 0, 1, true)

	return d
}

// StartLiveUpdates chạy một goroutine nền đọc số liệu từ Application theo
// chu kỳ `interval` và vẽ lại UI qua QueueUpdateDraw (bắt buộc phải dùng
// hàm này khi cập nhật tview từ goroutine khác UI thread). Tự dừng khi ctx
// bị cancel (app shutdown).
func (d *Dashboard) StartLiveUpdates(ctx context.Context, tuiApp *tview.Application, cdcApp *app.Application, interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				ins := cdcApp.EventsCount.InsertCount.Load()
				upd := cdcApp.EventsCount.UpdateCount.Load()
				del := cdcApp.EventsCount.DeleteCount.Load()
				minLSN := cdcApp.GlobalState.GetMinCheckpoint()
				tuiApp.QueueUpdateDraw(func() {
					d.statusPanel.SetText(fmt.Sprintf("Insert: %d\nUpdate: %d\nDelete: %d\nMinLSN: %d", ins, upd, del, minLSN))
				})
			}
		}
	}()
}

func (d *Dashboard) refresh(cdcApp *app.Application, interval time.Duration) {
	insert := cdcApp.EventsCount.InsertCount.Load()
	update := cdcApp.EventsCount.UpdateCount.Load()
	del := cdcApp.EventsCount.DeleteCount.Load()
	total := insert + update + del

	deltaTotal := (insert - d.lastInsert) + (update - d.lastUpdate) + (del - d.lastDelete)
	eps := float64(deltaTotal) / interval.Seconds()
	d.lastInsert, d.lastUpdate, d.lastDelete = insert, update, del

	minLSN := cdcApp.GlobalState.GetMinCheckpoint()
	activeSinks := cdcApp.GlobalState.ActiveSinks()
	uptime := time.Since(d.startedAt).Round(time.Second)

	statusText := fmt.Sprintf(
		"[green]Running[-]\nUptime: %s\nEPS: %.0f\nTotal events: %d (I:%d U:%d D:%d)\nMin checkpoint (LSN): %d",
		uptime, eps, total, insert, update, del, minLSN,
	)

	connText := fmt.Sprintf("Source: %s\n\nActive sinks (%d):\n",
		cdcApp.Config.Provider.Source.Name, len(activeSinks))
	for _, s := range activeSinks {
		connText += fmt.Sprintf(" - %s\n", s)
	}

	d.tuiApp.QueueUpdateDraw(func() {
		d.statusPanel.SetText(statusText)
		d.connectionPanel.SetText(connText)

		d.tuningPanel.SetCell(1, 0, tview.NewTableCell("Workers"))
		d.tuningPanel.SetCell(1, 1, tview.NewTableCell(fmt.Sprintf("%d", cdcApp.Config.DataProcessing.DataProcessingWorkerCount.Load())))
		d.tuningPanel.SetCell(2, 0, tview.NewTableCell("Batch Size"))
		d.tuningPanel.SetCell(2, 1, tview.NewTableCell(fmt.Sprintf("%d", cdcApp.Config.Batch.BatchMaxSize.Load())))
		d.tuningPanel.SetCell(3, 0, tview.NewTableCell("Batch Timeout (ms)"))
		d.tuningPanel.SetCell(3, 1, tview.NewTableCell(fmt.Sprintf("%d", cdcApp.Config.Batch.BatchTimeout.Load())))
		d.tuningPanel.SetCell(4, 0, tview.NewTableCell("Bag Max Size"))
		d.tuningPanel.SetCell(4, 1, tview.NewTableCell(fmt.Sprintf("%d", cdcApp.Config.Bag.BagMaxSize.Load())))

		fmt.Fprintf(d.logPanel, "[gray]%s[-] eps=%.0f total=%d min_lsn=%d\n",
			time.Now().Format("15:04:05"), eps, total, minLSN)
	})
}
// Writer exposes the log panel as an io.Writer so slog (or anything else)
// can stream output directly into the TUI.
func (d *Dashboard) Writer() io.Writer {
	return d.logPanel
}