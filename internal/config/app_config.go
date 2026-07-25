package config

import (
	"sync/atomic"
)

// ==============================================================================
// 1. CẤU HÌNH KẾT NỐI (CONNECTIVITY)
// ==============================================================================

// DBConnection định nghĩa thông tin cơ bản cho một kết nối cơ sở dữ liệu.
type DBConnection struct {
	Name     string // Tên định danh cho kết nối, dùng trong logging (VD: "postgres_master_db").
	Type     string // Loại cơ sở dữ liệu (VD: "postgres").
	URL      string // Chuỗi kết nối đầy đủ (Connection String).
	IsActive bool   // Cờ để bật/tắt kết nối này.
}

// RetryConfig chứa các tham số cho logic thử lại khi kết nối hoặc thao tác thất bại.
type RetryConfig struct {
	MaxRetries     int `json:"max_retries"`       // Số lần thử lại tối đa trước khi bỏ cuộc.
	BaseDelayMs    int `json:"base_delay_ms"`     // Thời gian chờ ban đầu (mili giây) trước khi thử lại lần đầu.
	MaxDelayTimeMs int `json:"max_delay_time_ms"` // Thời gian chờ tối đa (mili giây) giữa các lần thử lại (tránh chờ quá lâu).
}

// SourceProviderConfig định nghĩa cấu hình cho nguồn dữ liệu (nơi CDC đọc thay đổi).
type SourceProviderConfig struct {
	Source DBConnection // Chỉ hỗ trợ một nguồn duy nhất tại một thời điểm.
	// Có thể mở rộng thêm: SlotName, PublicationName...
}

// DataConsumerConfig quản lý danh sách các đích dữ liệu (nơi CDC ghi dữ liệu vào).
type DataConsumerConfig struct {
	List []DBConnection // Cho phép ghi ra nhiều đích cùng lúc.
}

// ==============================================================================
// 2. CẤU HÌNH HIỆU NĂNG (PERFORMANCE TUNING)
// ==============================================================================
// Các cấu hình trong phần này sử dụng kiểu `atomic` để có thể được điều chỉnh
// "nóng" (live-tuning) trong lúc ứng dụng đang chạy mà không cần khởi động lại.

// CaptureConfig cấu hình cho giai đoạn "bắt" dữ liệu từ nguồn.
type CaptureConfig struct {
	CaptureMaxSize   atomic.Int64 // Kích thước tối đa của một lần đọc từ WAL. (Chưa dùng)
	FeedbackInterval atomic.Int32 // Tần suất (giây) gửi phản hồi StandbyStatus về cho Postgres.
}

// PipelineConfig cấu hình cho kênh (channel) trung chuyển.
type PipelineConfig struct {
	PipelineMaxSize atomic.Int32 // Kích thước bộ đệm của kênh chính. (Chưa dùng)
}

// BagConfig cấu hình cho "túi" chứa sự kiện trước khi gửi đi.
type BagConfig struct {
	BagMaxSize     atomic.Int64 // Số lượng sự kiện tiêu chuẩn trong một túi.
	BagMaxMultiple atomic.Int32 // Hệ số nhân, kích thước túi tối đa = BagMaxSize * BagMaxMultiple.
}

// DataProcessingWorkerConfig cấu hình số lượng worker xử lý song song.
type DataProcessingWorkerConfig struct {
	DataProcessingWorkerCount atomic.Int32 // Số goroutine xử lý và xây dựng câu lệnh SQL.
}

// BatchConfig cấu hình cho việc gom lô (batching) trước khi ghi vào đích.
type BatchConfig struct {
	BatchMaxSize atomic.Int64 // Số lượng câu lệnh SQL tối đa trong một lô.
	BatchTimeout atomic.Int64 // Thời gian (mili giây) chờ tối đa trước khi xả lô, dù chưa đầy.
}

// ==============================================================================
// 3. CẤU HÌNH ĐỘ TIN CẬY & KIỂM SOÁT (STABILITY & CONTROL)
// ==============================================================================

// StateStorageConfig cấu hình nơi lưu trữ trạng thái (checkpoint).
type StateStorageConfig struct {
	StorageType string `json:"storage_type"` // Loại lưu trữ: "file" hoặc "postgres" (chưa hỗ trợ).
}

// FilterConfig cho phép lọc các bảng muốn hoặc không muốn theo dõi.
type FilterConfig struct {
	IncludeTables []string `json:"include_tables"` // Chỉ theo dõi các bảng trong danh sách này.
	ExcludeTables []string `json:"exclude_tables"` // Bỏ qua các bảng trong danh sách này.
}

// MonitorConfig cấu hình cho bộ giám sát và auto-tuning.
type MonitorConfig struct {
	EnableMetrics bool `json:"enable_metrics"` // Bật/tắt endpoint Prometheus. (Chưa dùng)
	HttpPort      int  `json:"http_port"`      // Cổng HTTP cho endpoint giám sát.
}

// CheckpointSaveDestination định nghĩa nơi lưu file checkpoint.
type CheckpointSaveDestination struct {
	Path string `json:"path"` // Đường dẫn đến thư mục chứa các file checkpoint.
}

// ==============================================================================
// 4. CẤU HÌNH TRUNG TÂM (CENTRAL CONFIG)
// ==============================================================================

// AppConfig là struct gốc, tổng hợp tất cả các cấu hình của ứng dụng.
type AppConfig struct {
	Provider        SourceProviderConfig
	Consumers       DataConsumerConfig
	Capture         CaptureConfig
	Pipeline        PipelineConfig
	Bag             BagConfig
	DataProcessing  DataProcessingWorkerConfig
	Batch           BatchConfig
	State           StateStorageConfig
	Retry           RetryConfig
	Filter          FilterConfig
	Monitor         MonitorConfig
	SaveDestination CheckpointSaveDestination
}

// NewDefaultConfig khởi tạo một bộ cấu hình mặc định, an toàn để chạy.
func NewDefaultConfig() *AppConfig {
	cfg := &AppConfig{}

	// --- Cấu hình kết nối mặc định ---
	// Thêm tham số slot_name vào cuối URL
	cfg.Provider.Source.URL = "postgres://postgres:password@192.168.137.89:5420/postgres?sslmode=disable&replication=database&slot_name=cdc_test_slot&publication_names=cdc_pub"
	cfg.Provider.Source.Name = "postgres_source_native"
	cfg.Provider.Source.Type = "postgres"
	cfg.Consumers.List = []DBConnection{
		{
			Name:     "postgres_dest_native",
			Type:     "postgres",
			URL:      "postgres://postgres:password@192.168.137.194:5419/postgres?sslmode=disable",
			IsActive: true,
		},
		// {
		// 	Name:     "cdc-db_destination_mysql",
		// 	Type:     "mysql",
		// 	URL:      "root:password@tcp(127.0.0.1:3306)/dest_db",
		// 	IsActive: true,
		// },
	}
	// --- Cấu hình hiệu năng mặc định ---
	cfg.Capture.CaptureMaxSize.Store(100000)
	cfg.Capture.FeedbackInterval.Store(10)
	cfg.Pipeline.PipelineMaxSize.Store(1000)
	cfg.Bag.BagMaxSize.Store(10000)
	cfg.Bag.BagMaxMultiple.Store(5)
	cfg.DataProcessing.DataProcessingWorkerCount.Store(10)
	cfg.Batch.BatchMaxSize.Store(10000)
	cfg.Batch.BatchTimeout.Store(1000)

	// --- Cấu hình độ tin cậy & giám sát mặc định ---
	cfg.Retry.MaxRetries = 3
	cfg.Retry.BaseDelayMs = 2000
	cfg.Retry.MaxDelayTimeMs = 30000
	cfg.State.StorageType = "file"
	cfg.Monitor.HttpPort = 8080

	// --- Cấu hình lưu trữ checkpoint ---
	cfg.SaveDestination.Path = "./local_checkpoints" // Thư mục lưu file checkpoint

	return cfg
}
