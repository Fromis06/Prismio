package cli

import (
	"github.com/rivo/tview"
)

// NewLoginForm tạo một giao diện form đăng nhập.
// Nó nhận vào một hàm callback để xử lý khi người dùng bấm nút "Login".
func NewLoginForm(tuiApp *tview.Application, onLoginAttempt func(username, apiKey string), onCreateKey func()) *tview.Flex {
	// Tạo một form cơ bản
	form := tview.NewForm().
		SetFieldBackgroundColor(tview.Styles.PrimitiveBackgroundColor).
		SetFieldTextColor(tview.Styles.PrimaryTextColor)

	// Thêm trường nhập username
	form.AddInputField("Username", "", 0, nil, nil)
	// Thêm trường nhập mật khẩu cho API Key
	form.AddPasswordField("API Key", "", 0, '*', nil)

	// Thêm các nút chức năng
	form.AddButton("Login", func() {
		// Lấy giá trị từ các trường nhập liệu
		username := form.GetFormItem(0).(*tview.InputField).GetText()
		apiKey := form.GetFormItem(1).(*tview.InputField).GetText()
		onLoginAttempt(username, apiKey)
	})
	form.AddButton("Create API Key", func() {
		onCreateKey()
	})
	form.AddButton("Quit", func() {
		tuiApp.Stop()
	})

	form.SetBorder(true).SetTitle("Login").SetTitleAlign(tview.AlignLeft)

	// Sử dụng Flex layout để căn giữa form trên màn hình
	flex := tview.NewFlex().
		AddItem(nil, 0, 1, false). // Khoảng trống ở trên
		AddItem(tview.NewFlex().SetDirection(tview.FlexRow).
						AddItem(nil, 0, 1, false).              // Khoảng trống bên trái
						AddItem(form, 0, 2, true).              // Form chiếm 2/4 chiều rộng và nhận focus
						AddItem(nil, 0, 1, false), 0, 3, true). // Khoảng trống bên phải
		AddItem(nil, 0, 1, false) // Khoảng trống ở dưới

	return flex
}
