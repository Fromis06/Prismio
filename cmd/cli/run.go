package cli

import (
	"context"
	"crypto/subtle"
	"log/slog"

	"my-cdc/internal/api"
	"my-cdc/internal/app"
	"my-cdc/internal/logger"

	"github.com/rivo/tview"
)

func Run() {
	logger.Initialize()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize the core application to get access to configuration.
	cdcApp := app.Initialize(ctx)

	tuiApp := tview.NewApplication()

	// Pages will manage switching between the login screen and the main dashboard.
	pages := tview.NewPages()

	// Create the main dashboard view
	dashboard := NewDashboard(tuiApp, cdcApp)

	// This modal will be shown on login failure.
	errorModal := tview.NewModal().
		SetText("Invalid API Key. Please try again.").
		AddButtons([]string{"OK"}).
		SetDoneFunc(func(buttonIndex int, buttonLabel string) {
			// When the user clicks "OK", we hide the modal.
			pages.HidePage("error")
		})

	// This callback function handles the login attempt.
	loginAttemptCallback := func(apiKey string) {
		if apiKey == "" {
			return // Do nothing if the key is empty.
		}

		// Hash the input key and compare it securely.
		hashedInput := api.HashAPIKey(apiKey)
		expectedHash := cdcApp.Config.Monitor.HashedAPIKey

		// Use constant-time comparison to prevent timing attacks.
		if subtle.ConstantTimeCompare([]byte(hashedInput), []byte(expectedHash)) == 1 {
			slog.Info("TUI: API Key validated successfully.")
			pages.SwitchToPage("dashboard")
		} else {
			slog.Warn("TUI: Failed login attempt.")
			pages.ShowPage("error")
		}
	}

	loginForm := NewLoginForm(tuiApp, loginAttemptCallback)

	pages.AddPage("login", loginForm, true, true)
	pages.AddPage("dashboard", dashboard, true, false)
	pages.AddPage("error", errorModal, false, false) // Add the modal page but keep it hidden initially.

	if err := tuiApp.SetRoot(pages, true).EnableMouse(true).Run(); err != nil {
		panic(err)
	}
}
