package sinks

import (
	"context"
	"fmt"
	"sort"

	"my-cdc/internal/config"
	"my-cdc/internal/models"
)

// [Pattern: Driver Factory] AppenderFactory sinh ra đích đến và gắn trực tiếp vào MultiSink.
type AppenderFactory func(ctx context.Context, consumerName string, cfg *config.AppConfig, consumerURL string, state *models.GlobalState, multiSink *MultiSink) error

// Metadata chứa thông tin hiển thị cho một loại Sink, dùng để build UI (dropdown TUI,
// danh sách lựa chọn...) mà không cần một file cấu hình riêng biệt tách khỏi code.
// Mỗi driver tự khai Metadata của mình ngay lúc gọi Register() trong init.go của nó —
// nhờ vậy danh sách hiển thị luôn khớp chính xác với driver đã thực sự được compile
// vào binary, không có rủi ro lệch đồng bộ giữa 2 nguồn dữ liệu.
type Metadata struct {
	DisplayName string // Tên hiển thị cho người dùng, VD: "PostgreSQL".
	URLTemplate string // Mẫu connection string để người dùng điền thay thông số thật vào.
}

// RegisteredSink là kết quả trả về từ ListRegistered — cặp (tên đăng ký, metadata hiển thị).
type RegisteredSink struct {
	Type     string
	Metadata Metadata
}

type registeredDriver struct {
	Metadata Metadata
	Factory  AppenderFactory
}

var registry = make(map[string]registeredDriver)

// Register đăng ký một loại Consumer mới (Postgres, MySQL, Kafka,...) kèm theo
// metadata hiển thị của nó.
func Register(name string, meta Metadata, factory AppenderFactory) {
	registry[name] = registeredDriver{Metadata: meta, Factory: factory}
}

// BuildAndAddPipeline khởi tạo pipeline dựa trên type và thêm thẳng vào MultiSink.
func BuildAndAddPipeline(ctx context.Context, sinkType string, consumerName string, cfg *config.AppConfig, consumerURL string, state *models.GlobalState, multiSink *MultiSink) error {
	if driver, exists := registry[sinkType]; exists {
		return driver.Factory(ctx, consumerName, cfg, consumerURL, state, multiSink)
	}
	return fmt.Errorf("không hỗ trợ consumer type: %s", sinkType)
}

// ListRegistered trả về danh sách toàn bộ Sink đã đăng ký, sắp xếp theo tên để UI
// hiển thị ổn định (không đổi thứ tự ngẫu nhiên giữa các lần chạy do map không có
// thứ tự cố định). Dùng hàm này để build dropdown/lựa chọn trên TUI thay vì đọc
// từ 1 file cấu hình riêng — danh sách trả về luôn khớp 100% với driver đã compile.
func ListRegistered() []RegisteredSink {
	result := make([]RegisteredSink, 0, len(registry))
	for name, driver := range registry {
		result = append(result, RegisteredSink{Type: name, Metadata: driver.Metadata})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Type < result[j].Type })
	return result
}