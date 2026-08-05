package cli

import (
	"context"
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

	// Đăng ký các Driver (Provider và Consumer) — không phụ thuộc vào
	// việc cmd/server có được import cùng lúc hay không.
	_ "my-cdc/internal/drivers"
)

func Run() {
	logger.Initialize()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tuiApp := tview.NewApplication()
	pages := tview.NewPages()

	// Ghi log ra cả stdout lẫn panel log của dashboard
	dashboard := NewDashboard(tuiApp)

	const configPath = "config.yaml"

	// --- PHA 1: Chỉ load cấu hình, không có side-effect ---
	cfg := config.NewDefaultConfig()

	// Nạp toàn bộ cấu hình (nguồn, danh sách đích, hiệu năng, API key...) từ config.yaml nếu có.
	if overrides, err := config.LoadOverrides(configPath); err == nil {
		absPath, _ := filepath.Abs(configPath)
		slog.Info("Loaded configuration from file", "path", absPath)
		overrides.ApplyTo(cfg)

		// Ghi lại theo format đầy đủ mới — tự động migrate file cũ (chỉ có hashed_api_keys)
		// sang schema mới ngay lần chạy đầu tiên.
		if saveErr := config.SaveFullConfig(configPath, cfg); saveErr != nil {
			slog.Warn("Failed to normalize config.yaml to new format", "error", saveErr)
		}
	} else {
		// File chưa tồn tại (lần chạy đầu) hoặc không đọc được: tạo file mới từ giá trị mặc định.
		absPath, _ := filepath.Abs(configPath)
		slog.Info("config.yaml not found or unreadable, creating with defaults.", "path", absPath, "error", err)
		if saveErr := config.SaveFullConfig(configPath, cfg); saveErr != nil {
			slog.Error("Failed to save initial config.yaml", "error", saveErr)
		}
	}

	var cdcAppRef atomic.Pointer[app.Application]

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
			pages.SwitchToPage("config")
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
			return
		}

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

		// Chụp snapshot từ cfg đang sống — để không bỏ sót các đích vừa thêm qua UI mà chưa kịp lưu
		newOverrides := config.FromAppConfig(cfg)
		if newOverrides.Monitor.HashedAPIKeys == nil {
			newOverrides.Monitor.HashedAPIKeys = make(map[string]string)
		}
		newOverrides.Monitor.HashedAPIKeys[hashedKey] = username

		absPath, _ := filepath.Abs(configPath)
		if err := config.SaveOverrides(configPath, newOverrides); err != nil {
			slog.Error("Failed to save new API key to config file", "path", absPath, "error", err)
			return
		}

		cfg.Monitor.HashedAPIKeys = newOverrides.Monitor.HashedAPIKeys
		slog.Info("Successfully generated and saved new API key", "path", absPath, "username", username)

		// Hiển thị key cho người dùng
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
		createUserForm.GetFormItem(0).(*tview.InputField).SetText("")
		pages.SwitchToPage("createUser")
	}

	var configFormLock func()

	runCdcCallback := func() {
		modal := tview.NewModal().SetText("Initializing CDC... Please wait.")
		pages.AddPage("running", modal, true, true)
		pages.ShowPage("running")

		if configFormLock != nil {
			configFormLock()
		}

		// --- PHA 2: Chạy Bootstrap trong goroutine ---
		go func() {
			newApp, err := app.Bootstrap(ctx, cfg)
			if err != nil {
				tuiApp.QueueUpdateDraw(func() {
					pages.HidePage("running")
					dashboard.StartLiveUpdates(ctx, tuiApp, newApp, time.Second)
					pages.SwitchToPage("dashboard")
				})
				return
			}
			cdcAppRef.Store(newApp)

			newApp.MultiSink.Start()
			go utils.StartAdaptiveMonitor(newApp.Config, newApp.EventsCount, time.Duration(newApp.Config.Monitor.MonitorIntervalSec)*time.Second)
			newApp.AutoTuner.Start()

			go func() {
				if err := newApp.Listener.Start(ctx, newApp.Config.Provider.Source.URL, newApp.GlobalState); err != nil && err != context.Canceled {
					slog.Error("Capture stream unexpectedly interrupted", "error", err)
				}
			}()

			tuiApp.QueueUpdateDraw(func() {
				pages.HidePage("running")
				dashboard.StartLiveUpdates(ctx, tuiApp, newApp, time.Second)
				pages.SwitchToPage("dashboard")
			})
		}()
	}

	loginForm := NewLoginForm(tuiApp, loginAttemptCallback, createKeyCallback)
	// Truyền thêm configPath vào NewConfigForm
	configForm, lockConfigForm := NewConfigForm(tuiApp, cfg, configPath, runCdcCallback)
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
		cancel()
		if runningApp := cdcAppRef.Load(); runningApp != nil {
			runningApp.Shutdown()
		}
		tuiApp.Stop()
	}()

	if err := tuiApp.SetRoot(pages, true).EnableMouse(true).Run(); err != nil {
		panic(err)
	}
}