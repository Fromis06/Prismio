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

	// Đăng ký các Driver (Provider và Consumer) — không phụ thuộc vào
	// việc cmd/server có được import cùng lúc hay không.
	_ "my-cdc/internal/drivers"
)

const (
	accountsPath = "accounts.yaml" // Bảng tài khoản DÙNG CHUNG — cần đọc trước khi biết ai đăng nhập.
	configsDir   = "configs"       // Mỗi tài khoản có 1 file cấu hình vận hành riêng: configs/<username>.yaml
)

func Run() {
	logger.Initialize()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tuiApp := tview.NewApplication()
	pages := tview.NewPages()

	// Ghi log ra cả stdout lẫn panel log của dashboard
	dashboard := NewDashboard(tuiApp)

	// --- Nạp bảng tài khoản (dùng chung) ---
	// Đây là dữ liệu DUY NHẤT cần đọc trước khi đăng nhập, vì phải xác thực xong mới
	// biết nạp file cấu hình vận hành của tài khoản nào (configs/<username>.yaml).
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
	var configFormLock func()

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

	// enterWorkspace được gọi ngay sau khi đăng nhập thành công: nạp cấu hình riêng
	// của tài khoản đó, dựng lại trang Config gắn với đúng cfg này, rồi chuyển màn hình.
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

		configForm, lockConfigForm := NewConfigForm(tuiApp, cfg, userConfigPath, runCdcCallback)
		configFormLock = lockConfigForm

		// Gỡ trang "config" của lần đăng nhập trước (nếu có) trước khi gắn trang mới,
		// tránh 2 trang trùng tên cùng tồn tại trong Pages.
		pages.RemovePage("config")
		pages.AddPage("config", configForm, true, false)
		pages.SwitchToPage("config")
	}

	loginAttemptCallback := func(username, apiKey string) {
		if apiKey == "" || username == "" {
			pages.ShowPage("error")
			return
		}

		// Băm API Key và kiểm tra xem nó có tồn tại và khớp với username không
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

	// Form để tạo user mới
	createUserForm := tview.NewForm().
		AddInputField("Username", "", 40, nil, nil).
		SetFieldBackgroundColor(tview.Styles.PrimitiveBackgroundColor).
		SetFieldTextColor(tview.Styles.PrimaryTextColor)

	// Callback khi người dùng bấm nút "Create" trên form tạo user.
	// Chỉ cập nhật bảng tài khoản dùng chung (accounts.yaml) — cấu hình vận hành
	// riêng của tài khoản mới sẽ tự được tạo (giá trị mặc định) ở lần đăng nhập đầu
	// tiên, xem loadUserConfig().
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

	loginForm := NewLoginForm(tuiApp, loginAttemptCallback, createKeyCallback)

	pages.AddPage("login", loginForm, true, true)
	pages.AddPage("createUser", centeredCreateUserForm, true, false)
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