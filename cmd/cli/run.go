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

	// Register the built-in drivers.
	_ "my-cdc/internal/drivers"
)

const (
	accountsPath = "accounts.yaml" // Shared account list, needed before login.
	configsDir   = "configs"       // Per-account config lives in configs/<username>.yaml.
)

func Run() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tuiApp := tview.NewApplication()
	pages := tview.NewPages()

	// Logger writes into this panel, so build it first.
	dashboard := NewDashboard(tuiApp)
	logger.Initialize(dashboard.LogWriter())

	// Accounts are enough for login; the rest loads after it.
	accounts, err := config.LoadAccounts(accountsPath)
	if err != nil {
		absPath, _ := filepath.Abs(accountsPath)
		slog.Info("accounts.yaml does not exist yet, creating a new (empty) accounts table.", "path", absPath, "error", err)
		accounts = &config.AccountsFile{HashedAPIKeys: make(map[string]string)}
		if saveErr := config.SaveAccounts(accountsPath, accounts); saveErr != nil {
			slog.Error("Failed to save initial accounts.yaml", "error", saveErr)
		}
	}

	var cdcAppRef atomic.Pointer[app.Application]

	// Shutdown waits for the listener before flushing and saving the checkpoint.
	var listenerDone atomic.Pointer[chan struct{}]

	var configFormLock func()

	errorModal := tview.NewModal().
		SetText("Invalid API Key. Please try again.").
		AddButtons([]string{"OK"}).
		SetDoneFunc(func(buttonIndex int, buttonLabel string) {
			pages.HidePage("error")
		})

	// Bootstrap errors go back to config.
	bootstrapErrorModal := tview.NewModal().AddButtons([]string{"OK"})
	bootstrapErrorModal.SetDoneFunc(func(buttonIndex int, buttonLabel string) {
		pages.SwitchToPage("config") // Return to the config page to fix the issue
	})

	usernameExistsModal := tview.NewModal().
		SetText("Error: Username already exists. Please choose a different username.").
		AddButtons([]string{"OK"}).
		SetDoneFunc(func(buttonIndex int, buttonLabel string) { pages.HidePage("usernameExists") })
	usernameExistsModal.SetTitle("Username Conflict")

	// Each account gets its own config file.
	loadUserConfig := func(username string) (*config.AppConfig, string) {
		userConfigPath := filepath.Join(configsDir, username+".yaml")
		cfg := config.NewDefaultConfig()

		// Keep checkpoints apart when switching accounts.
		cfg.SaveDestination.Path = filepath.Join("local_checkpoints", username)

		// API keys stay in the shared account file.
		cfg.Monitor.HashedAPIKeys = accounts.HashedAPIKeys

		if overrides, loadErr := config.LoadOverrides(userConfigPath); loadErr == nil {
			absPath, _ := filepath.Abs(userConfigPath)
			slog.Info("Loaded per-account configuration", "username", username, "path", absPath)
			overrides.ApplyTo(cfg)
			// ApplyTo leaves the shared API keys alone.
		} else {
			absPath, _ := filepath.Abs(userConfigPath)
			slog.Info("No dedicated configuration for this account yet, creating a new one with default values.", "username", username, "path", absPath, "error", loadErr)
			if mkErr := os.MkdirAll(configsDir, 0755); mkErr != nil {
				slog.Error("Failed to create configs directory", "error", mkErr)
			}
			if saveErr := config.SaveFullConfig(userConfigPath, cfg); saveErr != nil {
				slog.Error("Failed to save initial per-account config", "username", username, "error", saveErr)
			}
		}

		return cfg, userConfigPath
	}

	// Open the account workspace after login.
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
				// Checkpoint path is checked during bootstrap.
				newApp, bootstrapErr := app.Bootstrap(ctx, cfg)
				if bootstrapErr != nil {
					tuiApp.QueueUpdateDraw(func() {
						pages.HidePage("running")
						bootstrapErrorModal.SetText(fmt.Sprintf("Failed to initialize CDC:\n%v", bootstrapErr))
						pages.ShowPage("bootstrap_error")
					})
					return
				}
				cdcAppRef.Store(newApp)

				newApp.MultiSink.Start()
				go utils.StartAdaptiveMonitor(newApp.Config, newApp.EventsCount, time.Duration(newApp.Config.Monitor.MonitorIntervalSec)*time.Second)

				// Manual keeps the chosen values. Automatic lets the tuner change them.
				if newApp.Config.Tuning.IsAutomatic() {
					newApp.AutoTuner.Start()
					slog.Info("AUTO-TUNER: Running in Automatic mode")
				} else {
					slog.Info("AUTO-TUNER: Locked (Manual mode) — using the config exactly as set by the user")
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

		// Replace the old config page for this account.
		pages.RemovePage("config")
		pages.AddPage("config", configForm, true, false)
		pages.SwitchToPage("config")
	}

	loginAttemptCallback := func(username, apiKey string) {
		if apiKey == "" || username == "" {
			pages.ShowPage("error")
			return
		}

		// Match the key hash with its account.
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

	// New account form.
	createUserForm := tview.NewForm().
		AddInputField("Username", "", 40, nil, nil).
		SetFieldBackgroundColor(tview.Styles.PrimitiveBackgroundColor).
		SetFieldTextColor(tview.Styles.PrimaryTextColor)

	// The account config gets created on first login.
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

		// Show the key once so they can save it.
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

	// Stop in order: listener, sinks, then checkpoint.
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		slog.Info("Received stop signal, starting shutdown process")
		cancel()

		if runningApp := cdcAppRef.Load(); runningApp != nil {
			// Let listener finish first; it may still be sending events.
			if donePtr := listenerDone.Load(); donePtr != nil {
				<-*donePtr
			}

			// Flush what is still buffered.
			runningApp.MultiSink.Stop()

			// Save the checkpoint after the last flush.
			runningApp.Shutdown()
		}
		tuiApp.Stop()
	}()

	if err := tuiApp.SetRoot(pages, true).EnableMouse(true).Run(); err != nil {
		panic(err)
	}
}
