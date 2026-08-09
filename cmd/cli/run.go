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

	// Import drivers to ensure they are registered. This makes them available
	// to the application's factory functions.
	_ "my-cdc/internal/drivers"
)

const (
	accountsPath = "accounts.yaml" // SHARED accounts file, read before login to authenticate users.
	configsDir   = "configs"       // Directory for PER-ACCOUNT operational configs: configs/<username>.yaml
)

func Run() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tuiApp := tview.NewApplication()
	pages := tview.NewPages()

	// The Dashboard must be created BEFORE initializing the logger.
	// The logger needs a writer from the dashboard (dashboard.LogWriter()) to pipe logs
	// into the TUI log panel. If initialized first, logs would go to os.Stdout,
	// corrupting the tview display.
	dashboard := NewDashboard(tuiApp)
	logger.Initialize(dashboard.LogWriter())

	// Load the shared accounts file. This is the ONLY data needed before login,
	// as it's required to authenticate the user and determine which per-account
	// configuration file (configs/<username>.yaml) to load next.
	accounts, err := config.LoadAccounts(accountsPath)
	if err != nil {
		absPath, _ := filepath.Abs(accountsPath)
		slog.Info("accounts.yaml chưa tồn tại, tạo bảng tài khoản mới (rỗng).", "path", absPath, "error", err)
		accounts = &config.AccountsFile{HashedAPIKeys: make(map[string]string)}
		if saveErr := config.SaveAccounts(accountsPath, accounts); saveErr != nil {
			slog.Error("Failed to save initial accounts.yaml", "error", saveErr)
		}
	}

	var cdcAppRef atomic.Pointer[app.Application]

	// listenerDone giữ channel được đóng khi goroutine của Listener thực sự
	// return. Shutdown path cần ĐỢI channel này trước khi flush sink / lưu
	// checkpoint — nếu không, MultiSink.Stop() và Shutdown() có thể chạy
	// trong lúc Listener vẫn còn đang đẩy event mới vào pipeline, dẫn tới
	// mất dữ liệu hoặc checkpoint bị lưu "chậm" hơn batch cuối cùng thực sự
	// đã ghi xuống sink. Dùng atomic.Pointer vì goroutine tạo ra nó (bên
	// trong runCdcCallback) và goroutine đọc nó (shutdown handler) chạy độc
	// lập với nhau.
	var listenerDone atomic.Pointer[chan struct{}]

	var configFormLock func()

	errorModal := tview.NewModal().
		SetText("Invalid API Key. Please try again.").
		AddButtons([]string{"OK"}).
		SetDoneFunc(func(buttonIndex int, buttonLabel string) {
			pages.HidePage("error")
		})

	// Modal for displaying bootstrap errors.
	bootstrapErrorModal := tview.NewModal().AddButtons([]string{"OK"})
	bootstrapErrorModal.SetDoneFunc(func(buttonIndex int, buttonLabel string) {
		pages.SwitchToPage("config") // Quay lại trang config để sửa
	})

	usernameExistsModal := tview.NewModal().
		SetText("Error: Username already exists. Please choose a different username.").
		AddButtons([]string{"OK"}).
		SetDoneFunc(func(buttonIndex int, buttonLabel string) { pages.HidePage("usernameExists") })
	usernameExistsModal.SetTitle("Username Conflict")

	// loadUserConfig nạp (hoặc khởi tạo mới nếu chưa có) cấu hình vận hành RIÊNG của
	// 1 tài khoản. Mỗi tài khoản có Source/Consumers/Batch/Worker... độc lập, không
	// còn dùng chung 1 config.yaml như trước.
	loadUserConfig := func(username string) (*config.AppConfig, string) {
		userConfigPath := filepath.Join(configsDir, username+".yaml")
		cfg := config.NewDefaultConfig()

		// Cô lập thư mục checkpoint theo từng tài khoản để tránh đụng LSN của nhau
		// khi đổi tài khoản (đặt trước khi ApplyTo, để user vẫn override được nếu muốn).
		cfg.SaveDestination.Path = filepath.Join("local_checkpoints", username)

		// HashedAPIKeys dùng cho xác thực API HTTP (/config) vẫn lấy từ bảng tài khoản
		// dùng chung — không phải thứ "riêng của từng user", nên không nằm trong
		// configs/<username>.yaml.
		cfg.Monitor.HashedAPIKeys = accounts.HashedAPIKeys

		if overrides, loadErr := config.LoadOverrides(userConfigPath); loadErr == nil {
			absPath, _ := filepath.Abs(userConfigPath)
			slog.Info("Loaded per-account configuration", "username", username, "path", absPath)
			overrides.ApplyTo(cfg)
			// ApplyTo không đụng tới Monitor.HashedAPIKeys (đã bỏ khỏi OverrideConfig),
			// nên giá trị gán ở trên vẫn giữ nguyên sau bước này.
		} else {
			absPath, _ := filepath.Abs(userConfigPath)
			slog.Info("Chưa có cấu hình riêng cho tài khoản này, tạo mới với giá trị mặc định.", "username", username, "path", absPath, "error", loadErr)
			if mkErr := os.MkdirAll(configsDir, 0755); mkErr != nil {
				slog.Error("Failed to create configs directory", "error", mkErr)
			}
			if saveErr := config.SaveFullConfig(userConfigPath, cfg); saveErr != nil {
				slog.Error("Failed to save initial per-account config", "username", username, "error", saveErr)
			}
		}

		return cfg, userConfigPath
	}

	// enterWorkspace is called after a successful login. It loads the user-specific
	// configuration, rebuilds the configuration form page with that config,
	// and switches the TUI to that page.
	enterWorkspace := func(username string) {
		cfg, userConfigPath := loadUserConfig(username)

		runCdcCallback := func() {
			modal := tview.NewModal().SetText("Initializing CDC... Please wait.")
			pages.AddPage("running", modal, true, true)
			pages.ShowPage("running")

			if configFormLock != nil {
				configFormLock()
			}

			go func() {
				// app.Bootstrap now ensures the checkpoint directory exists on startup,
				// even before any data is processed. This provides early feedback on
				// permissions issues (fail-fast). See internal/app/app.go for details.
				newApp, bootstrapErr := app.Bootstrap(ctx, cfg)
				if bootstrapErr != nil {
					tuiApp.QueueUpdateDraw(func() {
						pages.HidePage("running")
						bootstrapErrorModal.SetText(fmt.Sprintf("Khởi tạo CDC thất bại:\n%v", bootstrapErr))
						pages.ShowPage("bootstrap_error")
					})
					return
				}
				cdcAppRef.Store(newApp)

				newApp.MultiSink.Start()
				go utils.StartAdaptiveMonitor(newApp.Config, newApp.EventsCount, time.Duration(newApp.Config.Monitor.MonitorIntervalSec)*time.Second)

				// Chế độ AutoTuner được chọn ở trang config (nút Manual / Automatic,
				// xem cmd/cli/config_form.go) quyết định AutoTuner có được phép chạy
				// hay không:
				//   - "manual": KHÔNG gọi AutoTuner.Start() -> không goroutine nào
				//     ghi đè lên các giá trị người dùng vừa cấu hình, chúng giữ
				//     nguyên (bị "khoá") trong suốt vòng đời của lần chạy này.
				//   - "automatic": AutoTuner.Start() được gọi, các biến
				//     real-time-tunable có thể bị AutoTuner điều chỉnh khi đang chạy.
				if newApp.Config.Tuning.Mode == "automatic" {
					newApp.AutoTuner.Start()
					slog.Info("AUTO-TUNER: Đang chạy ở chế độ Automatic")
				} else {
					slog.Info("AUTO-TUNER: Bị khoá (chế độ Manual) — dùng nguyên config người dùng đã thiết lập")
				}

				done := make(chan struct{})
				listenerDone.Store(&done)
				go func() {
					defer close(done)
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

		configForm, lockConfigForm := NewConfigForm(tuiApp, cfg, userConfigPath, runCdcCallback)
		configFormLock = lockConfigForm

		// Remove the previous "config" page (if any) before adding the new one
		// to avoid having two pages with the same name in the tview.Pages manager.
		pages.RemovePage("config")
		pages.AddPage("config", configForm, true, false)
		pages.SwitchToPage("config")
	}

	loginAttemptCallback := func(username, apiKey string) {
		if apiKey == "" || username == "" {
			pages.ShowPage("error")
			return
		}

		// Hash the input API key and check if it exists and matches the given username.
		hashedInput := api.HashAPIKey(apiKey)
		storedUsername, exists := accounts.HashedAPIKeys[hashedInput]

		if exists && storedUsername == username {
			slog.Info("TUI: API Key validated successfully.", "username", username)
			enterWorkspace(username)
		} else {
			slog.Warn("TUI: Failed login attempt.", "username", username)
			pages.ShowPage("error")
		}
	}

	// Form for creating a new user.
	createUserForm := tview.NewForm().
		AddInputField("Username", "", 40, nil, nil).
		SetFieldBackgroundColor(tview.Styles.PrimitiveBackgroundColor).
		SetFieldTextColor(tview.Styles.PrimaryTextColor)

	// This callback handles the "Create" button action on the new user form.
	// It only updates the shared accounts.yaml file. The new user's specific
	// operational config will be auto-generated on their first login (see loadUserConfig).
	createUserAndKey := func() {
		username := createUserForm.GetFormItem(0).(*tview.InputField).GetText()
		if username == "" {
			return
		}

		for _, existingUsername := range accounts.HashedAPIKeys {
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

		accounts.HashedAPIKeys[hashedKey] = username

		absPath, _ := filepath.Abs(accountsPath)
		if err := config.SaveAccounts(accountsPath, accounts); err != nil {
			slog.Error("Failed to save new account to accounts.yaml", "path", absPath, "error", err)
			return
		}

		slog.Info("Successfully generated and saved new account", "path", absPath, "username", username)

		// Display the new key to the user.
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

	loginForm := NewLoginForm(tuiApp, loginAttemptCallback, createKeyCallback)

	pages.AddPage("login", loginForm, true, true)
	pages.AddPage("createUser", centeredCreateUserForm, true, false)
	pages.AddPage("dashboard", dashboard.Layout, true, false)
	pages.AddPage("error", errorModal, false, false)
	pages.AddPage("usernameExists", usernameExistsModal, false, false)
	pages.AddPage("bootstrap_error", bootstrapErrorModal, false, false)

	// Handle graceful shutdown on interrupt signals.
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		slog.Info("Received stop signal, starting shutdown process")
		cancel()

		if runningApp := cdcAppRef.Load(); runningApp != nil {
			// 1. Đợi Listener dừng hẳn — không còn event mới nào được đẩy
			//    vào pipeline. Nếu CDC chưa từng được "Run" (listenerDone
			//    vẫn nil), bỏ qua bước này.
			if donePtr := listenerDone.Load(); donePtr != nil {
				<-*donePtr
			}

			// 2. Flush hết batch còn tồn trong buffer của từng sink. Trước
			//    bản sửa này, bước này BỊ THIẾU HOÀN TOÀN trong CLI mode,
			//    khiến mọi batch chưa đủ BatchMaxSize / chưa tới
			//    BatchTimeout bị bỏ luôn khi thoát app, không kịp ghi
			//    xuống sink.
			runningApp.MultiSink.Stop()

			// 3. Lưu checkpoint cuối cùng — BẮT BUỘC chạy sau bước 2, nếu
			//    không checkpoint ghi ra đĩa sẽ cũ hơn batch vừa flush
			//    thành công ở trên.
			runningApp.Shutdown()
		}
		tuiApp.Stop()
	}()

	if err := tuiApp.SetRoot(pages, true).EnableMouse(true).Run(); err != nil {
		panic(err)
	}
}