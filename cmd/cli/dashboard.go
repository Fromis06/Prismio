package cli

import (
	"my-cdc/internal/app"

	"github.com/rivo/tview"
)

// NewDashboard creates the main dashboard layout for the TUI.
func NewDashboard(tuiApp *tview.Application, cdcApp *app.Application) *tview.Flex {
	// Panel 1: System Status
	statusPanel := tview.NewTextView().
		SetDynamicColors(true).
		SetBorder(true).
		SetTitle(" System Status ")

	// Panel 2: Connectivity Info
	connectionPanel := tview.NewTextView().
		SetDynamicColors(true).
		SetBorder(true).
		SetTitle(" Connectivity ")

	// Panel 3: Live Tuning
	tuningPanel := tview.NewTable().
		SetBorders(true).
		SetBorder(true).
		SetTitle(" Live Tuning ")

	// Panel 4: Logs
	logPanel := tview.NewTextView().
		SetDynamicColors(true).
		SetScrollable(true).
		SetBorder(true).
		SetTitle(" Logs ")

	// Create the layout
	leftColumn := tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(statusPanel, 0, 1, false).
		AddItem(connectionPanel, 0, 1, false)

	mainContent := tview.NewFlex().
		AddItem(leftColumn, 0, 1, false).
		AddItem(tuningPanel, 0, 1, false)

	layout := tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(mainContent, 0, 3, false). // Main content takes 3/4 of the height
		AddItem(logPanel, 0, 1, true)      // Logs take 1/4 and have focus

	return layout
}