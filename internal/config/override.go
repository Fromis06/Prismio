package config

import (
	"os"

	"gopkg.in/yaml.v3"
)

// DBConnectionOverride is a flattened version of DBConnection for YAML serialization.
// IsActive uses a pointer to distinguish between "not declared in file" (nil -> defaults to true)
// and "explicitly declared as false".
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

// PerformanceOverride uses pointers for all numeric fields to distinguish between
// "not in file" (keep default value) and "in file with value 0".
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

// MonitorOverride no longer contains HashedAPIKeys. The account registry is now in
// the shared accounts.yaml (see accounts.go), separating it from per-user
// operational configuration.
type MonitorOverride struct {
	HttpPort           int    `yaml:"http_port,omitempty"`
	ListenAddress      string `yaml:"listen_address,omitempty"`
	MonitorIntervalSec int    `yaml:"monitor_interval_sec,omitempty"`
}

// OverrideConfig represents the entire content that can be loaded from or saved to
// a per-user configuration file (e.g., configs/<username>.yaml). Unlike AppConfig,
// which uses atomic types for live-tuning, this struct uses plain types for easy
// YAML serialization.
type OverrideConfig struct {
	Provider    ProviderOverride       `yaml:"provider"`
	Consumers   []DBConnectionOverride `yaml:"consumers"`
	Performance PerformanceOverride    `yaml:"performance"`
	Retry       RetryOverride          `yaml:"retry"`
	State       StateOverride          `yaml:"state"`
	Monitor     MonitorOverride        `yaml:"monitor"`
}

// LoadOverrides reads a configuration override file from disk.
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

// SaveOverrides saves the configuration to disk using a temporary file and rename
// mechanism (similar to SaveProviderCheckpoint) to prevent corruption.
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

// SaveFullConfig captures the entire current state of an AppConfig (including consumers
// added at runtime via the CLI) and saves it to the user's configuration file.
// This should be used instead of manually building an OverrideConfig to ensure the
// file on disk always matches the running config.
func SaveFullConfig(path string, cfg *AppConfig) error {
	return SaveOverrides(path, FromAppConfig(cfg))
}

// FromAppConfig converts a live AppConfig (with atomic types) into a flat
// OverrideConfig suitable for YAML marshalling.
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

// ApplyTo overwrites an AppConfig with values from an OverrideConfig. Fields not
// present in the YAML (empty strings, nil pointers) are ignored, so the AppConfig
// retains its default values from NewDefaultConfig. This allows config files to be sparse.
func (o *OverrideConfig) ApplyTo(cfg *AppConfig) {
	if o == nil {
		return
	}

	applyConn(&cfg.Provider.Source, o.Provider.Source)

	// The Consumers list is overridden entirely; it is not merged.
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
		dst.IsActive = true // If not specified, defaults to true to avoid silently disabling a destination.
	}
	if src.Options != nil {
		dst.Options = src.Options
	}
}
