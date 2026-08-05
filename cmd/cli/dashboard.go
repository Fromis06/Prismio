package cli

import (
	"context"
	"fmt"
	"time"

	"my-cdc/internal/app"

	"github.com/rivo/tview"
)

// Dashboard đóng gói layout cùng tham chiếu tới các panel cần cập nhật live,
// để StartLiveUpdates có thể ghi vào đúng chỗ mà không cần biết cấu trúc Flex bên ngoài.
type Dashboard struct {
	Layout      *tview.Flex
	statusPanel *tview.TextView
	logPanel    *tview.TextView
}

// NewDashboard tạo layout dashboard. Được gọi trước khi Bootstrap xong,
// nên chưa cần *app.Application ở đây — dữ liệu sống sẽ được nạp qua StartLiveUpdates.
func NewDashboard(tuiApp *tview.Application) *Dashboard {
	statusPanel := tview.NewTextView().
		SetDynamicColors(true).
		SetText("Đang khởi động...")
	statusPanel.SetBorder(true).SetTitle(" System Status ")

	logPanel := tview.NewTextView().
		SetDynamicColors(true).
		SetScrollable(true).
		SetChangedFunc(func() { tuiApp.Draw() })
	logPanel.SetBorder(true).SetTitle(" Logs ")

	layout := tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(statusPanel, 5, 0, false). // Chiều cao cố định 5 dòng, đủ cho 3 chỉ số
		AddItem(logPanel, 0, 1, true)      // Phần còn lại dành cho log, có focus

	return &Dashboard{
		Layout:      layout,
		statusPanel: statusPanel,
		logPanel:    logPanel,
	}
}

// LogWriter trả về io.Writer để logger có thể ghi trực tiếp vào logPanel
// thay vì os.Stdout (vốn bị tview chiếm dụng).
func (d *Dashboard) LogWriter() *tview.TextView {
	return d.logPanel
}

// StartLiveUpdates chạy một goroutine nền, định kỳ tính EPS, runtime, tổng số event
// từ EventsCount và cập nhật statusPanel. Dừng khi ctx bị cancel (graceful shutdown).
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

				// Mọi thay đổi UI phải qua QueueUpdateDraw vì đang ở goroutine nền,
				// không phải main loop của tview.
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