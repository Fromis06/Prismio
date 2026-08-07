package capture

import (
	"context"
	"fmt"
	"sort"

	"my-cdc/internal/config"
	"my-cdc/internal/models"
	"my-cdc/internal/sinks"
)

// Listener định nghĩa hợp đồng chung cho các nguồn Capture.
type Listener interface {
	Start(ctx context.Context, url string, state *models.GlobalState) error
}

// Factory là hàm khởi tạo sinh ra một Listener cụ thể.
type Factory func(cfg *config.AppConfig, multiSink *sinks.MultiSink, eventsCount *models.EventsCount) Listener

// Metadata contains display information for a capture (source) driver type,
// used for building UIs (like the TUI's source-selection dropdown) without a
// separate config file. Each driver declares its own metadata when it calls
// Register(), so the UI list always matches the drivers compiled into the
// binary — mirroring sinks.Metadata.
type Metadata struct {
	DisplayName string // User-friendly display name, e.g., "PostgreSQL".
	URLTemplate string // A connection string template to guide the user.
}

// RegisteredCapture pairs a registered source type name with its display metadata.
type RegisteredCapture struct {
	Type     string
	Metadata Metadata
}

type registeredDriver struct {
	Metadata Metadata
	Factory  Factory
}

var registry = make(map[string]registeredDriver)

// Register đăng ký một Provider mới vào hệ thống, cùng metadata hiển thị của nó.
func Register(name string, meta Metadata, factory Factory) {
	registry[name] = registeredDriver{Metadata: meta, Factory: factory}
}

// CreateListener khởi tạo Listener dựa vào tên Provider cấu hình.
func CreateListener(name string, cfg *config.AppConfig, multiSink *sinks.MultiSink, eventsCount *models.EventsCount) (Listener, error) {
	if driver, ok := registry[name]; ok {
		return driver.Factory(cfg, multiSink, eventsCount), nil
	}
	return nil, fmt.Errorf("không tìm thấy provider được hỗ trợ: %s", name)
}

// ListRegistered trả về danh sách các driver Source đã đăng ký, sắp xếp theo
// tên để hiển thị ổn định trên UI (ví dụ dropdown chọn nguồn dữ liệu).
func ListRegistered() []RegisteredCapture {
	result := make([]RegisteredCapture, 0, len(registry))
	for name, driver := range registry {
		result = append(result, RegisteredCapture{Type: name, Metadata: driver.Metadata})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Type < result[j].Type })
	return result
}