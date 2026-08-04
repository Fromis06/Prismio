package cli

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"time"

	"my-cdc/internal/api"
	"my-cdc/internal/app"
	"my-cdc/internal/config"
	"my-cdc/internal/logger"
	"my-cdc/internal/utils"

	"github.com/atotto/clipboard"
	"github.com/rivo/tview"
)

func Run() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tuiApp := tview.NewApplication()
	dashboard := NewDashboard(tuiApp)

	// Ghi log ra cả stdout lẫn panel log của dashboard, để bấm "Run CDC"
	// là log Bootstrap/checkpoint/sink hiện ngay trong TUI, không cần
	// chờ tick refresh hay xem terminal ngầm (bị TUI chiếm màn hình).
	logger.Initialize(dashboard.Writer())

	const configPath = "config.yaml"

	// --- PHA 1: Chỉ load cấu hình, không có side-effect ---
	cfg := config.NewDefaultConfig()

	// Ghi đè HashedAPIKey từ file config.yaml nếu có
	if overrides, err := config.LoadOverrides(configPath); err == nil && len(overrides.Monitor.HashedAPIKeys) > 0 {
		absPath, _ := filepath.Abs(configPath)
		slog.Info("Loaded API keys from override file", "path", absPath)
		cfg.Monitor.HashedAPIKeys = overrides.Monitor.HashedAPIKeys
	} else {
		// Nếu file config.yaml không tồn tại hoặc rỗng, tạo file mới với key mặc định
		absPath, _ := filepath.Abs(configPath)
		slog.Info("config.yaml not found or empty, creating with default key.", "path", absPath)
		var newOverrides config.OverrideConfig
		newOverrides.Monitor.HashedAPIKeys = make(map[string]string)
		// Copy default keys to new overrides
		newOverrides.Monitor.HashedAPIKeys = cfg.Monitor.HashedAPIKeys
		if saveErr := config.SaveOverrides(configPath, &newOverrides); saveErr != nil {
			slog.Error("Failed to save initial config.yaml", "error", saveErr)
		}
	}

	// cdcAppRef holds *app.Application behind an atomic pointer because it's
	// written once from the Bootstrap goroutine but read concurrently from
	// the OS-signal shutdown goroutine (and potentially from dashboard live
	// updates). A plain `var cdcApp *app.Application` is a data race.
	var cdcAppRef atomic.Pointer[app.Application]

	pages := tview.NewPages()

	errorModal := tview.NewModal().
		SetText("Invalid API Key. Please try again.").
		AddButtons([]string{"OK"}).
		SetDoneFunc(func(buttonIndex int, buttonLabel string) {
			pages.HidePage("error")
		})

	// Modal hiển thị khi Bootstrap thất bại
	bootstrapErrorModal := tview.NewModal().AddButtons([]string{"OK"})
	bootstrapErrorModal.SetDoneFunc(func(buttonIndex int, buttonLabel string) {
		pages.SwitchToPage("config") // Quay lại trang config để sửa
	})

	// Modal hiển thị khi username đã tồn tại
	usernameExistsModal := tview.NewModal().
		SetText("Error: Username already exists. Please choose a different username.").
		AddButtons([]string{"OK"}).
		SetDoneFunc(func(buttonIndex int, buttonLabel string) { pages.HidePage("usernameExists") })
	usernameExistsModal.SetTitle("Username Conflict")

	loginAttemptCallback := func(username, apiKey string) {
		if apiKey == "" || username == "" {
			pages.ShowPage("error")
			return
		}

		// Băm API Key và kiểm tra xem nó có tồn tại và khớp với username không
		hashedInput := api.HashAPIKey(apiKey)
		storedUsername, exists := cfg.Monitor.HashedAPIKeys[hashedInput]

		if exists && storedUsername == username {
			slog.Info("TUI: API Key validated successfully.", "username", username)
			pages.SwitchToPage("config") // Chuyển đến trang config sau khi login thành công
		} else {
			slog.Warn("TUI: Failed login attempt.", "username", username)
			pages.ShowPage("error")
		}
	}

	// Form để tạo user mới
	createUserForm := tview.NewForm().
		AddInputField("Username", "", 40, nil, nil).
		SetFieldBackgroundColor(tview.Styles.PrimitiveBackgroundColor).
		SetFieldTextColor(tview.Styles.PrimaryTextColor)

	// Callback khi người dùng bấm nút "Create" trên form tạo user
	createUserAndKey := func() {
		username := createUserForm.GetFormItem(0).(*tview.InputField).GetText()
		if username == "" {
			// Có thể thêm modal báo lỗi ở đây nếu muốn
			return
		}

		// Kiểm tra xem username đã tồn tại chưa.
		// LƯU Ý: hàm này (createUserAndKey) là callback của nút "Create",
		// nên nó đã chạy TRÊN chính UI goroutine (main loop của tview).
		// Gọi tuiApp.QueueUpdateDraw() ở đây sẽ deadlock: QueueUpdateDraw
		// đẩy việc vào queue rồi chờ main loop xử lý, nhưng main loop lúc
		// này đang bận chạy chính callback đang gọi nó -> tự khoá chính mình,
		// modal không bao giờ hiện và cả app bị đơ. Vì đã ở sẵn UI goroutine,
		// chỉ cần gọi thẳng pages.ShowPage(...).
		for _, existingUsername := range cfg.Monitor.HashedAPIKeys {
			if existingUsername == username {
				pages.ShowPage("usernameExists")
				return
			}
		}

		rawKey, hashedKey, err := api.GenerateNewAPIKey()
		if err != nil {
			slog.Error("FATAL: Failed to generate new API key", "error", err)
			return
		}

		var newOverrides config.OverrideConfig
		if existingOverrides, err := config.LoadOverrides(configPath); err == nil {
			newOverrides = *existingOverrides
		}
		if newOverrides.Monitor.HashedAPIKeys == nil {
			newOverrides.Monitor.HashedAPIKeys = make(map[string]string)
		}

		// Lưu username làm value cho key đã hash
		newOverrides.Monitor.HashedAPIKeys[hashedKey] = username

		absPath, _ := filepath.Abs(configPath)
		if err := config.SaveOverrides(configPath, &newOverrides); err != nil {
			slog.Error("Failed to save new API key to config file", "path", absPath, "error", err)
			return
		}

		cfg.Monitor.HashedAPIKeys = newOverrides.Monitor.HashedAPIKeys
		slog.Info("Successfully generated and saved new API key", "path", absPath, "username", username)

		// Hiển thị key (mật khẩu) cho người dùng
		keyDisplayForm := tview.NewForm().
			AddTextView("Username", username, 0, 1, true, false).
			AddInputField("API Key (Password)", rawKey, len(rawKey)+5, nil, nil)

		keyDisplayForm.AddButton("Copy Key", func() {
			if err := clipboard.WriteAll(rawKey); err != nil {
				slog.Error("Failed to copy API key to clipboard", "error", err)
			}
		}).AddButton("OK, I have saved my key", func() {
			pages.SwitchToPage("login")
			pages.RemovePage("keyDisplay")
		})

		keyDisplayForm.SetBorder(true).SetTitle("IMPORTANT: Save your API Key!")
		centeredKeyDisplay := tview.NewFlex().AddItem(nil, 0, 1, false).AddItem(tview.NewFlex().SetDirection(tview.FlexRow).AddItem(nil, 0, 1, false).AddItem(keyDisplayForm, 0, 3, true).AddItem(nil, 0, 1, false), 0, 1, true).AddItem(nil, 0, 1, false)
		pages.AddPage("keyDisplay", centeredKeyDisplay, true, true)
	}

	createUserForm.AddButton("Create", createUserAndKey)
	createUserForm.AddButton("Cancel", func() { pages.SwitchToPage("login") })
	createUserForm.SetBorder(true).SetTitle("Create New User")
	centeredCreateUserForm := tview.NewFlex().AddItem(nil, 0, 1, false).AddItem(tview.NewFlex().SetDirection(tview.FlexRow).AddItem(nil, 0, 1, false).AddItem(createUserForm, 0, 2, true).AddItem(nil, 0, 1, false), 0, 1, true).AddItem(nil, 0, 1, false)

	createKeyCallback := func() {
		createUserForm.GetFormItem(0).(*tview.InputField).SetText("") // Xóa username cũ
		pages.SwitchToPage("createUser")
	}

	// configFormLock được gán bên dưới sau khi NewConfigForm được gọi.
	var configFormLock func()

	runCdcCallback := func() {
		// Hiển thị một modal "đang xử lý" để người dùng biết
		modal := tview.NewModal().SetText("Initializing CDC... Please wait.")
		pages.AddPage("running", modal, true, true)
		pages.ShowPage("running")

		// Khoá form cấu hình ngay lập tức: từ giờ Source URL / Destination URL
		// sẽ được đọc bởi goroutine Bootstrap/Listener, không nên còn cho phép
		// chỉnh sửa đồng thời từ UI thread.
		if configFormLock != nil {
			configFormLock()
		}

		// --- PHA 2: Chạy Bootstrap trong một goroutine để không block UI ---
		go func() {
			newApp, err := app.Bootstrap(ctx, cfg)
			if err != nil {
				// Nếu lỗi, cập nhật UI trên main thread để báo lỗi
				tuiApp.QueueUpdateDraw(func() {
					bootstrapErrorModal.SetText(fmt.Sprintf("Initialization Failed:\n\n%v", err))
					pages.HidePage("running")
					pages.ShowPage("bootstrap_error")
				})
				return
			}
			cdcAppRef.Store(newApp)

			// Bootstrap thành công, khởi chạy các tiến trình nền
			newApp.MultiSink.Start()
			go utils.StartAdaptiveMonitor(newApp.Config, newApp.EventsCount, time.Duration(newApp.Config.Monitor.MonitorIntervalSec)*time.Second)
			newApp.AutoTuner.Start()

			go func() {
				if err := newApp.Listener.Start(ctx, newApp.Config.Provider.Source.URL, newApp.GlobalState); err != nil && err != context.Canceled {
					slog.Error("Capture stream unexpectedly interrupted", "error", err)
					// Có thể gửi tín hiệu để dừng ứng dụng ở đây nếu muốn
				}
			}()

			// Cập nhật UI để chuyển sang dashboard và bắt đầu cập nhật dữ liệu live
			tuiApp.QueueUpdateDraw(func() {
				pages.HidePage("running")
				dashboard.StartLiveUpdates(ctx, newApp, time.Second)
				pages.SwitchToPage("dashboard")
			})
		}()
	}

	loginForm := NewLoginForm(tuiApp, loginAttemptCallback, createKeyCallback)
	configForm, lockConfigForm := NewConfigForm(tuiApp, cfg, runCdcCallback)
	configFormLock = lockConfigForm

	pages.AddPage("login", loginForm, true, true)
	pages.AddPage("createUser", centeredCreateUserForm, true, false)
	pages.AddPage("config", configForm, true, false)
	pages.AddPage("dashboard", dashboard.Layout, true, false)
	pages.AddPage("error", errorModal, false, false)
	pages.AddPage("usernameExists", usernameExistsModal, false, false)
	pages.AddPage("bootstrap_error", bootstrapErrorModal, false, false)

	// Xử lý graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		slog.Info("Received stop signal, starting shutdown process")
		cancel() // Ra hiệu lệnh dừng cho các goroutine
		if runningApp := cdcAppRef.Load(); runningApp != nil {
			runningApp.Shutdown() // Lưu checkpoint
		}
		tuiApp.Stop() // Dừng TUI
	}()

	if err := tuiApp.SetRoot(pages, true).EnableMouse(true).Run(); err != nil {
		panic(err)
	}
}