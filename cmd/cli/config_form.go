package cli

import (
	"my-cdc/internal/config"
	"strconv"

	"github.com/rivo/tview"
)

// NewConfigForm tạo một form để chỉnh sửa các cấu hình chính trước khi chạy.
// Nó trả về layout để hiển thị, và một hàm `lock` mà caller có thể gọi
// sau khi CDC đã khởi chạy để khoá form lại, tránh việc người dùng
// sửa cấu hình (URL, worker count, ...) trong khi pipeline đang chạy
// và đọc các giá trị đó (race condition trên cfg.Provider.Source.URL, v.v.).
func NewConfigForm(tuiApp *tview.Application, cfg *config.AppConfig, runCallback func()) (*tview.Flex, func()) {
	form := tview.NewForm().
		SetFieldBackgroundColor(tview.Styles.PrimitiveBackgroundColor).
		SetFieldTextColor(tview.Styles.PrimaryTextColor)

	// Thêm các trường vào form, bind với giá trị từ cfg
	form.AddInputField("Source URL", cfg.Provider.Source.URL, 0, nil, func(text string) {
		cfg.Provider.Source.URL = text
	})

	// Giả sử chỉ có 1 consumer để đơn giản hóa
	if len(cfg.Consumers.List) > 0 {
		form.AddInputField("Destination URL", cfg.Consumers.List[0].URL, 0, nil, func(text string) {
			cfg.Consumers.List[0].URL = text
		})
	}

	// statusView hiển thị lỗi validate (vd: nhập số không hợp lệ) thay vì
	// âm thầm ghi đè giá trị cấu hình bằng 0.
	statusView := tview.NewTextView().
		SetDynamicColors(true).
		SetText("")

	form.AddInputField("Worker Count", strconv.Itoa(int(cfg.DataProcessing.DataProcessingWorkerCount.Load())), 10, tview.InputFieldInteger, func(text string) {
		val, err := strconv.ParseInt(text, 10, 32)
		if err != nil || val <= 0 {
			statusView.SetText("[red]Worker Count phải là số nguyên dương.[-]")
			return // KHÔNG ghi đè giá trị hiện tại bằng 0
		}
		statusView.SetText("")
		cfg.DataProcessing.DataProcessingWorkerCount.Store(int32(val))
	})

	form.AddInputField("Batch Size", strconv.Itoa(int(cfg.Batch.BatchMaxSize.Load())), 10, tview.InputFieldInteger, func(text string) {
		val, err := strconv.ParseInt(text, 10, 64)
		if err != nil || val <= 0 {
			statusView.SetText("[red]Batch Size phải là số nguyên dương.[-]")
			return
		}
		statusView.SetText("")
		cfg.Batch.BatchMaxSize.Store(val)
	})

	form.AddInputField("Batch Timeout (ms)", strconv.Itoa(int(cfg.Batch.BatchTimeout.Load())), 10, tview.InputFieldInteger, func(text string) {
		val, err := strconv.ParseInt(text, 10, 64)
		if err != nil || val <= 0 {
			statusView.SetText("[red]Batch Timeout phải là số nguyên dương.[-]")
			return
		}
		statusView.SetText("")
		cfg.Batch.BatchTimeout.Store(val)
	})

	// running ngăn người dùng bấm "Run CDC" nhiều lần liên tiếp
	// (mỗi lần bấm sẽ spawn thêm goroutine Bootstrap).
	running := false

	runButtonLabel := "Run CDC"
	form.AddButton(runButtonLabel, func() {
		if running {
			return
		}
		running = true
		// Đổi label để người dùng biết đã bấm và không cần bấm lại.
		if btn := form.GetButton(form.GetButtonIndex(runButtonLabel)); btn != nil {
			btn.SetLabel("Starting...")
		}
		runCallback()
	})
	form.AddButton("Quit", func() {
		tuiApp.Stop()
	})

	form.SetBorder(true).SetTitle("Configuration").SetTitleAlign(tview.AlignLeft)

	formArea := tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(form, 0, 1, true).
		AddItem(statusView, 1, 0, false)

	// Center the form
	flex := tview.NewFlex().
		AddItem(nil, 0, 1, false).
		AddItem(tview.NewFlex().SetDirection(tview.FlexRow).
			AddItem(nil, 0, 1, false).
			AddItem(formArea, 0, 8, true).
			AddItem(nil, 0, 1, false), 0, 3, true).
		AddItem(nil, 0, 1, false)

	// lock disable toàn bộ các trường input để tránh sửa cấu hình
	// (đặc biệt là 2 URL, vốn là string thường, không atomic) sau khi
	// pipeline đã bắt đầu đọc chúng từ goroutine khác.
	lock := func() {
		for i := 0; i < form.GetFormItemCount(); i++ {
			if field, ok := form.GetFormItem(i).(*tview.InputField); ok {
				field.SetDisabled(true)
			}
		}
	}

	return flex, lock
}