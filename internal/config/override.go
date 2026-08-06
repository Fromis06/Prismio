package config

import (
	"os"

	"gopkg.in/yaml.v3"
)

// DBConnectionOverride là dạng phẳng của DBConnection dùng để (de)serialize YAML.
// IsActive dùng con trỏ để phân biệt "không khai báo trong file" (nil -> mặc định true)
// với "khai báo rõ ràng là false" (tắt đích này).
type DBConnectionOverride struct {
	Name     string            `yaml:"name"`
	Type     string            `yaml:"type"`
	URL      string            `yaml:"url"`
	IsActive *bool             `yaml:"is_active,omitempty"`
	Options  map[string]string `yaml:"options,omitempty"`
}

type ProviderOverride struct {
	Source DBConnectionOverride `yaml:"source"`
}

// PerformanceOverride dùng con trỏ cho mọi field số để có thể phân biệt
// "không có trong file -> giữ giá trị mặc định" với "có trong file, giá trị = 0".
type PerformanceOverride struct {
	DataProcessingWorkerCount *int32 `yaml:"data_processing_worker_count,omitempty"`
	BatchMaxSize              *int64 `yaml:"batch_max_size,omitempty"`
	BatchTimeoutMs            *int64 `yaml:"batch_timeout_ms,omitempty"`
	FlushTimeoutMs            *int64 `yaml:"flush_timeout_ms,omitempty"`
	BagMaxSize                *int64 `yaml:"bag_max_size,omitempty"`
	BagMaxMultiple            *int32 `yaml:"bag_max_multiple,omitempty"`
	FeedbackIntervalSec       *int32 `yaml:"feedback_interval_sec,omitempty"`
}

type RetryOverride struct {
	MaxRetries     *int `yaml:"max_retries,omitempty"`
	BaseDelayMs    *int `yaml:"base_delay_ms,omitempty"`
	MaxDelayTimeMs *int `yaml:"max_delay_time_ms,omitempty"`
}

type StateOverride struct {
	StorageType string `yaml:"storage_type,omitempty"`
	SavePath    string `yaml:"save_path,omitempty"`
}

// MonitorOverride KHÔNG còn chứa HashedAPIKeys — bảng tài khoản giờ nằm ở
// accounts.yaml (dùng chung, xem accounts.go), tách khỏi cấu hình vận hành
// riêng từng user để tránh 2 khái niệm khác nhau bị trộn vào 1 file.
type MonitorOverride struct {
	HttpPort           int    `yaml:"http_port,omitempty"`
	ListenAddress      string `yaml:"listen_address,omitempty"`
	MonitorIntervalSec int    `yaml:"monitor_interval_sec,omitempty"`
}

// OverrideConfig là toàn bộ nội dung có thể lưu/đọc từ file cấu hình RIÊNG của
// 1 tài khoản (VD: configs/<username>.yaml). Khác với AppConfig (chứa
// atomic.Int64/Int32 để live-tuning trong lúc chạy), OverrideConfig chỉ dùng
// kiểu dữ liệu thường để (de)serialize YAML thuận tiện.
type OverrideConfig struct {
	Provider    ProviderOverride       `yaml:"provider"`
	Consumers   []DBConnectionOverride `yaml:"consumers"`
	Performance PerformanceOverride    `yaml:"performance"`
	Retry       RetryOverride          `yaml:"retry"`
	State       StateOverride          `yaml:"state"`
	Monitor     MonitorOverride        `yaml:"monitor"`
}

// LoadOverrides đọc file cấu hình từ đĩa.
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

// SaveOverrides lưu cấu hình xuống đĩa. Ghi vào file tạm rồi rename, cùng nguyên lý
// với SaveProviderCheckpoint (utils/checkpoint.go), để tránh file config bị hỏng
// nếu tiến trình bị kill giữa lúc đang ghi.
func SaveOverrides(path string, overrides *OverrideConfig) error {
	data, err := yaml.Marshal(overrides)
	if err != nil {
		return err
	}
	tempPath := path + ".tmp"
	if err := os.WriteFile(tempPath, data, 0644); err != nil {
		return err
	}
	return os.Rename(tempPath, path)
}

// SaveFullConfig chụp toàn bộ trạng thái hiện tại của AppConfig (bao gồm mọi consumer
// đã thêm qua CLI lúc runtime) và lưu xuống file cấu hình của tài khoản đang dùng.
// Đây là hàm nên dùng thay vì tự dựng OverrideConfig tay, để đảm bảo file trên đĩa
// luôn khớp với cfg đang chạy.
func SaveFullConfig(path string, cfg *AppConfig) error {
	return SaveOverrides(path, FromAppConfig(cfg))
}

// FromAppConfig chuyển trạng thái sống (AppConfig, gồm cả atomic) thành OverrideConfig
// phẳng để có thể marshal ra YAML.
func FromAppConfig(cfg *AppConfig) *OverrideConfig {
	o := &OverrideConfig{}

	o.Provider.Source = toOverride(cfg.Provider.Source)

	for _, c := range cfg.Consumers.List {
		o.Consumers = append(o.Consumers, toOverride(c))
	}

	workerCount := cfg.DataProcessing.DataProcessingWorkerCount.Load()
	batchMaxSize := cfg.Batch.BatchMaxSize.Load()
	batchTimeout := cfg.Batch.BatchTimeout.Load()
	flushTimeout := cfg.Batch.FlushTimeoutMs.Load()
	bagMaxSize := cfg.Bag.BagMaxSize.Load()
	bagMaxMultiple := cfg.Bag.BagMaxMultiple.Load()
	feedbackInterval := cfg.Capture.FeedbackInterval.Load()

	o.Performance = PerformanceOverride{
		DataProcessingWorkerCount: &workerCount,
		BatchMaxSize:              &batchMaxSize,
		BatchTimeoutMs:            &batchTimeout,
		FlushTimeoutMs:            &flushTimeout,
		BagMaxSize:                &bagMaxSize,
		BagMaxMultiple:            &bagMaxMultiple,
		FeedbackIntervalSec:       &feedbackInterval,
	}

	o.Retry = RetryOverride{
		MaxRetries:     &cfg.Retry.MaxRetries,
		BaseDelayMs:    &cfg.Retry.BaseDelayMs,
		MaxDelayTimeMs: &cfg.Retry.MaxDelayTimeMs,
	}

	o.State = StateOverride{
		StorageType: cfg.State.StorageType,
		SavePath:    cfg.SaveDestination.Path,
	}

	o.Monitor = MonitorOverride{
		HttpPort:           cfg.Monitor.HttpPort,
		ListenAddress:      cfg.Monitor.ListenAddress,
		MonitorIntervalSec: cfg.Monitor.MonitorIntervalSec,
	}

	return o
}

// ApplyTo ghi đè các giá trị có trong OverrideConfig lên AppConfig. Field nào không
// được khai báo trong YAML (chuỗi rỗng, con trỏ nil) thì AppConfig giữ nguyên giá trị
// mặc định đã có sẵn từ NewDefaultConfig — cho phép file cấu hình chỉ cần liệt kê phần
// người dùng muốn thay đổi.
func (o *OverrideConfig) ApplyTo(cfg *AppConfig) {
	if o == nil {
		return
	}

	applyConn(&cfg.Provider.Source, o.Provider.Source)

	// Consumers là danh sách nên override = thay thế toàn bộ, không merge từng phần tử.
	if len(o.Consumers) > 0 {
		list := make([]DBConnection, 0, len(o.Consumers))
		for _, c := range o.Consumers {
			var conn DBConnection
			applyConn(&conn, c)
			list = append(list, conn)
		}
		cfg.Consumers.List = list
	}

	p := o.Performance
	if p.DataProcessingWorkerCount != nil {
		cfg.DataProcessing.DataProcessingWorkerCount.Store(*p.DataProcessingWorkerCount)
	}
	if p.BatchMaxSize != nil {
		cfg.Batch.BatchMaxSize.Store(*p.BatchMaxSize)
	}
	if p.BatchTimeoutMs != nil {
		cfg.Batch.BatchTimeout.Store(*p.BatchTimeoutMs)
	}
	if p.FlushTimeoutMs != nil {
		cfg.Batch.FlushTimeoutMs.Store(*p.FlushTimeoutMs)
	}
	if p.BagMaxSize != nil {
		cfg.Bag.BagMaxSize.Store(*p.BagMaxSize)
	}
	if p.BagMaxMultiple != nil {
		cfg.Bag.BagMaxMultiple.Store(*p.BagMaxMultiple)
	}
	if p.FeedbackIntervalSec != nil {
		cfg.Capture.FeedbackInterval.Store(*p.FeedbackIntervalSec)
	}

	r := o.Retry
	if r.MaxRetries != nil {
		cfg.Retry.MaxRetries = *r.MaxRetries
	}
	if r.BaseDelayMs != nil {
		cfg.Retry.BaseDelayMs = *r.BaseDelayMs
	}
	if r.MaxDelayTimeMs != nil {
		cfg.Retry.MaxDelayTimeMs = *r.MaxDelayTimeMs
	}

	if o.State.StorageType != "" {
		cfg.State.StorageType = o.State.StorageType
	}
	if o.State.SavePath != "" {
		cfg.SaveDestination.Path = o.State.SavePath
	}

	if o.Monitor.HttpPort != 0 {
		cfg.Monitor.HttpPort = o.Monitor.HttpPort
	}
	if o.Monitor.ListenAddress != "" {
		cfg.Monitor.ListenAddress = o.Monitor.ListenAddress
	}
	if o.Monitor.MonitorIntervalSec != 0 {
		cfg.Monitor.MonitorIntervalSec = o.Monitor.MonitorIntervalSec
	}
}

func toOverride(c DBConnection) DBConnectionOverride {
	active := c.IsActive
	return DBConnectionOverride{
		Name:     c.Name,
		Type:     c.Type,
		URL:      c.URL,
		IsActive: &active,
		Options:  c.Options,
	}
}

func applyConn(dst *DBConnection, src DBConnectionOverride) {
	if src.Name != "" {
		dst.Name = src.Name
	}
	if src.Type != "" {
		dst.Type = src.Type
	}
	if src.URL != "" {
		dst.URL = src.URL
	}
	if src.IsActive != nil {
		dst.IsActive = *src.IsActive
	} else {
		dst.IsActive = true // Không khai báo -> mặc định bật, tránh im lặng tắt một đích.
	}
	if src.Options != nil {
		dst.Options = src.Options
	}
}