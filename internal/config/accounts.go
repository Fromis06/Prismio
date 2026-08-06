package config

import (
	"os"

	"gopkg.in/yaml.v3"
)

// AccountsFile là bảng đăng ký tài khoản dùng CHUNG cho toàn hệ thống — khác với
// OverrideConfig (giờ chỉ chứa cấu hình vận hành: Provider/Consumers/Performance...)
// vốn là riêng của từng tài khoản. Bảng này phải tách riêng vì cần đọc được TRƯỚC
// khi biết ai đang đăng nhập, để còn xác thực; không thể nằm chung trong file cấu
// hình theo-từng-user vì lúc đó chưa biết nạp file nào.
type AccountsFile struct {
	// Key: API key đã băm SHA-256. Value: username tương ứng.
	HashedAPIKeys map[string]string `yaml:"hashed_api_keys"`
}

// LoadAccounts đọc bảng tài khoản từ đĩa.
func LoadAccounts(path string) (*AccountsFile, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var af AccountsFile
	if err := yaml.Unmarshal(data, &af); err != nil {
		return nil, err
	}
	if af.HashedAPIKeys == nil {
		af.HashedAPIKeys = make(map[string]string)
	}
	return &af, nil
}

// SaveAccounts lưu bảng tài khoản xuống đĩa theo cơ chế ghi file tạm rồi rename,
// tránh file bị hỏng nếu tiến trình bị kill giữa lúc đang ghi.
func SaveAccounts(path string, af *AccountsFile) error {
	if af.HashedAPIKeys == nil {
		af.HashedAPIKeys = make(map[string]string)
	}
	data, err := yaml.Marshal(af)
	if err != nil {
		return err
	}
	tempPath := path + ".tmp"
	if err := os.WriteFile(tempPath, data, 0644); err != nil {
		return err
	}
	return os.Rename(tempPath, path)
}