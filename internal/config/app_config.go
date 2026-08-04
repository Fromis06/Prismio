package config

import (
	"crypto/sha256"
	"encoding/hex"
	"sync/atomic"
)

// Connectivity

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

// Performance Tuning
// Các cấu hình trong phần này sử dụng kiểu `atomic` để có thể được điều chỉnh "nóng"
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
	BatchMaxSize   atomic.Int64 // Số lượng câu lệnh SQL tối đa trong một lô.
	BatchTimeout   atomic.Int64 // Thời gian (mili giây) chờ tối đa trước khi xả lô, dù chưa đầy.
	FlushTimeoutMs atomic.Int64 // Thời gian (mili giây) timeout cho một thao tác ghi (flush) xuống DB.
}

// Stability & Control

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
	EnableMetrics      bool   `json:"enable_metrics"`       // Bật/tắt endpoint Prometheus. (Chưa dùng)
	HttpPort           int    `json:"http_port"`            // Cổng HTTP cho endpoint giám sát.
	ListenAddress      string `json:"listen_address"`       // Địa chỉ lắng nghe cho HTTP server (VD: "localhost", "0.0.0.0").
	MonitorIntervalSec int    `json:"monitor_interval_sec"` // Tần suất (giây) giám sát và in log.
	HashedAPIKeys      map[string]string `yaml:"hashed_api_keys"` // Map các khóa API đã băm. Key là hash, value là mô tả.
}

// CheckpointSaveDestination định nghĩa nơi lưu file checkpoint.
type CheckpointSaveDestination struct {
	Path string `json:"path"` // Đường dẫn đến thư mục chứa các file checkpoint.
}

// Central Config

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

func NewDefaultConfig() *AppConfig {
	cfg := &AppConfig{}

	// Cấu hình kết nối mặc định
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
	}
	// Cấu hình hiệu năng mặc định
	cfg.Capture.CaptureMaxSize.Store(100000)
	cfg.Capture.FeedbackInterval.Store(10)
	cfg.Pipeline.PipelineMaxSize.Store(1000)
	cfg.Bag.BagMaxSize.Store(10000)
	cfg.Bag.BagMaxMultiple.Store(5)
	cfg.DataProcessing.DataProcessingWorkerCount.Store(10) // Số worker xử lý
	cfg.Batch.BatchMaxSize.Store(5000)                     // Kích thước lô
	cfg.Batch.BatchTimeout.Store(200)                      // Thời gian chờ lô (ms)
	cfg.Batch.FlushTimeoutMs.Store(120000)

	// Cấu hình độ tin cậy & giám sát mặc định
	cfg.Retry.MaxRetries = 3
	cfg.Retry.BaseDelayMs = 2000
	cfg.Retry.MaxDelayTimeMs = 30000
	cfg.State.StorageType = "file"
	cfg.Monitor.HttpPort = 8080
	cfg.Monitor.ListenAddress = "localhost"
	cfg.Monitor.MonitorIntervalSec = 5
	// Băm API Key mặc định. RẤT QUAN TRỌNG: Thay đổi khóa này trong môi trường sản phẩm!
	cfg.Monitor.HashedAPIKeys = make(map[string]string) // Khởi tạo map
	defaultAPIKey := "your-super-secret-api-key"
	hashedDefaultAPIKey := sha256.Sum256([]byte(defaultAPIKey))
	hexHashedKey := hex.EncodeToString(hashedDefaultAPIKey[:])
	cfg.Monitor.HashedAPIKeys[hexHashedKey] = "Default key" // Sửa lỗi đánh máy

	// Cấu hình lưu trữ checkpoint
	cfg.SaveDestination.Path = "./local_checkpoints"

	return cfg
}
