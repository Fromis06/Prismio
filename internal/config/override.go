package config

import (
	"os"

	"gopkg.in/yaml.v3"
)

// OverrideConfig chứa các giá trị cấu hình có thể ghi đè lên giá trị mặc định.
// Chúng ta chỉ định nghĩa các trường mà chúng ta muốn có thể ghi đè từ file.
type OverrideConfig struct {
	Monitor struct {
		HashedAPIKeys map[string]string `yaml:"hashed_api_keys"`
	} `yaml:"monitor"`
}

// LoadOverrides đọc file cấu hình ghi đè.
func LoadOverrides(path string) (*OverrideConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var overrides OverrideConfig
	if err := yaml.Unmarshal(data, &overrides); err != nil {
		return nil, err
	}
	return &overrides, nil
}

// SaveOverrides lưu file cấu hình ghi đè.
func SaveOverrides(path string, overrides *OverrideConfig) error {
	data, err := yaml.Marshal(overrides)
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0644)
}
