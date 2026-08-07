package cli

import (
	"github.com/rivo/tview"
)

// NewLoginForm creates a login form interface.
// It takes callbacks to handle user actions like "Login" and "Create Key".
func NewLoginForm(tuiApp *tview.Application, onLoginAttempt func(username, apiKey string), onCreateKey func()) *tview.Flex {
	// Create the basic form.
	form := tview.NewForm().
		SetFieldBackgroundColor(tview.Styles.PrimitiveBackgroundColor).
		SetFieldTextColor(tview.Styles.PrimaryTextColor)

	// Add a username input field.
	form.AddInputField("Username", "", 0, nil, nil)
	// Add a password field for the API Key.
	form.AddPasswordField("API Key", "", 0, '*', nil)

	// Add action buttons.
	form.AddButton("Login", func() {
		// Get values from the input fields.
		username := form.GetFormItem(0).(*tview.InputField).GetText()
		apiKey := form.GetFormItem(1).(*tview.InputField).GetText()
		onLoginAttempt(username, apiKey)
	})
	form.AddButton("Create API Key", onCreateKey)
	form.AddButton("Quit", func() { tuiApp.Stop() })

	form.SetBorder(true).SetTitle("Login").SetTitleAlign(tview.AlignLeft)

	// Use a Flex layout to center the form on the screen.
	flex := tview.NewFlex().AddItem(nil, 0, 1, false).AddItem(tview.NewFlex().SetDirection(tview.FlexRow).AddItem(nil, 0, 1, false).AddItem(form, 0, 2, true).AddItem(nil, 0, 1, false), 0, 3, true).AddItem(nil, 0, 1, false)

	return flex
}
