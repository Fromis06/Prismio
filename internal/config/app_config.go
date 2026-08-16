package config

import (
	"crypto/sha256"
	"encoding/hex"
	"sync/atomic"
)

// Connectivity

// DBConnection defines the basic information for a database connection.
type DBConnection struct {
	Name     string // A unique identifier for the connection, used in logging (e.g., "postgres_master_db").
	Type     string // The type of database (e.g., "postgres", "kafka").
	URL      string // The full connection string.
	IsActive bool   // A flag to enable or disable this connection.
	// Options contains database-specific parameters (e.g., MongoDB: "auth_source",
	// Kafka: "topic", "sasl_mechanism"). The specific sink/capture implementation
	// reads the keys it needs, allowing new DB types to be added without
	// modifying this core config struct.
	Options map[string]string
}

// RetryConfig contains parameters for the retry logic on failed connections or operations.
type RetryConfig struct {
	MaxRetries     int `json:"max_retries"`       // Maximum number of retries before giving up.
	BaseDelayMs    int `json:"base_delay_ms"`     // Initial delay (in milliseconds) before the first retry.
	MaxDelayTimeMs int `json:"max_delay_time_ms"` // Maximum delay (in milliseconds) between retries to prevent excessively long waits.
}

// SourceProviderConfig defines the configuration for the data source (where CDC reads changes from).
type SourceProviderConfig struct {
	Source DBConnection // Only a single source is supported at a time.
	// Could be extended with fields like SlotName, PublicationName, etc.
}

// DataConsumerConfig manages the list of data destinations (where CDC writes data to).
type DataConsumerConfig struct {
	List []DBConnection // Allows writing to multiple destinations simultaneously.
}

// Performance Tuning
// The configurations in this section use atomic types to allow for live-tuning
// while the application is running, without requiring a restart.
//
// Note: not every field here can be changed while the app is running —
// some are only ever read once, at startup (see field-level comments below).
// Whether live-tuning takes effect at all is also gated by TuningConfig.Mode
// (see below): in "manual" mode the AutoTuner goroutine is never started, so
// nothing writes to these atomics after Bootstrap; in "automatic" mode it may.

// CaptureConfig configures the data capture phase from the source.
type CaptureConfig struct {
	FeedbackInterval atomic.Int32 // Frequency (in seconds) for sending StandbyStatus feedback to Postgres.
	// Read only once, when Listener.Start() creates its ticker — changing this
	// value while the CDC is already running has no effect until next restart.
}

// PipelineConfig configures the central processing channel.
type PipelineConfig struct {
	// PipelineMaxSize sets the buffer capacity of the main event channel
	// (DataProcessor.EventChan). Read only once, when the channel is created
	// in NewDataProcessor() during Bootstrap — Go channels cannot be resized
	// after creation, so changing this value while the CDC is already running
	// has no effect until next restart.
	PipelineMaxSize atomic.Int32
}

// BagConfig configures the event "bag" before it is dispatched.
type BagConfig struct {
	BagMaxSize     atomic.Int64 // The standard number of events in a bag.
	BagMaxMultiple atomic.Int32 // Multiplier for the maximum bag size (max size = BagMaxSize * BagMaxMultiple).
}

// DataProcessingWorkerConfig configures the number of parallel processing workers.
type DataProcessingWorkerConfig struct {
	DataProcessingWorkerCount atomic.Int32 // Number of goroutines for processing events and building SQL statements.
}

// BatchConfig configures batching before writing to the destination.
type BatchConfig struct {
	BatchMaxSize   atomic.Int64 // Maximum number of SQL statements in a single batch.
	BatchTimeout   atomic.Int64 // Maximum time (in milliseconds) to wait before flushing a batch, even if not full.
	FlushTimeoutMs atomic.Int64 // Timeout (in milliseconds) for a single flush operation to the database.
}

// Stability & Control

// StateStorageConfig configures where to store the application's state (checkpoint).
type StateStorageConfig struct {
	StorageType string `json:"storage_type"` // Storage type: "file" or "postgres" (not yet supported).
}

// FilterConfig allows filtering which tables to include or exclude from tracking.
type FilterConfig struct {
	IncludeTables []string `json:"include_tables"` // Only track tables in this list.
	ExcludeTables []string `json:"exclude_tables"` // Ignore tables in this list.
}

// MonitorConfig configures the monitoring and auto-tuning components.
type MonitorConfig struct {
	EnableMetrics      bool              `json:"enable_metrics"`       // Enable/disable the Prometheus endpoint. (Not yet used)
	HttpPort           int               `json:"http_port"`            // HTTP port for the monitoring endpoint.
	ListenAddress      string            `json:"listen_address"`       // Listen address for the HTTP server (e.g., "localhost", "0.0.0.0").
	MonitorIntervalSec int               `json:"monitor_interval_sec"` // Frequency (in seconds) for monitoring and logging stats.
	HashedAPIKeys      map[string]string `yaml:"hashed_api_keys"`      // Map of hashed API keys. Key is the hash, value is a description.
}

// TuningConfig controls whether the AutoTuner is allowed to adjust the
// performance-tuning fields above (Capture/Pipeline/Bag/DataProcessing/Batch)
// while the CDC is running.
type TuningConfig struct {
	// Mode is either "manual" or "automatic".
	//
	//   - "manual": AutoTuner.Start() is never called (see cmd/cli/run.go).
	//     The values the user set in the config UI stay fixed for the entire
	//     run — nothing else writes to the atomics above.
	//   - "automatic": AutoTuner.Start() is called and may adjust the
	//     real-time-tunable atomics while the CDC is running (see
	//     internal/tuning/auto_tuner.go for which ones currently apply).
	Mode string `json:"mode" yaml:"mode"`
}

const (
	TuningModeManual    = "manual"
	TuningModeAutomatic = "automatic"
)

// IsAutomatic is the single gate for every runtime tuning mutation. Unknown
// or missing modes fail closed to Manual so a malformed configuration cannot
// silently rewrite values selected by the user.
func (c TuningConfig) IsAutomatic() bool {
	return c.Mode == TuningModeAutomatic
}

// CheckpointSaveDestination defines where checkpoint files are stored.
type CheckpointSaveDestination struct {
	Path string `json:"path"` // Path to the directory containing checkpoint files.
}

// Central Config

// AppConfig is the root struct that aggregates all application configurations.
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
	Tuning          TuningConfig
	SaveDestination CheckpointSaveDestination
}

// NewDefaultConfig creates a new AppConfig instance with sensible default values.
func NewDefaultConfig() *AppConfig {
	cfg := &AppConfig{}

	cfg.Provider.Source.URL = ""
	cfg.Provider.Source.Name = ""
	cfg.Provider.Source.Type = ""

	// Consumers.List intentionally starts empty. Previously it was seeded with
	// a single Type-less entry, which the UI had no way to fix or remove and
	// caused Bootstrap to fail with an unhelpful "unsupported consumer type"
	// error. Now every destination — including the first one — is created
	// explicitly through the config UI's "choose a type" dropdown (see
	// cmd/cli/config_form.go), the same way the source is now chosen too.

	cfg.Capture.FeedbackInterval.Store(10)
	cfg.Pipeline.PipelineMaxSize.Store(1000)
	cfg.Bag.BagMaxSize.Store(10000)
	cfg.Bag.BagMaxMultiple.Store(5)
	cfg.DataProcessing.DataProcessingWorkerCount.Store(10)
	cfg.Batch.BatchMaxSize.Store(5000)
	cfg.Batch.BatchTimeout.Store(200)
	cfg.Batch.FlushTimeoutMs.Store(120000)

	cfg.Retry.MaxRetries = 3
	cfg.Retry.BaseDelayMs = 2000
	cfg.Retry.MaxDelayTimeMs = 30000
	cfg.State.StorageType = "file"
	cfg.Monitor.HttpPort = 8080
	cfg.Monitor.ListenAddress = "localhost"
	cfg.Monitor.MonitorIntervalSec = 5

	// Default to "manual": AutoTuner's tuning logic (applyTuningLogic) is
	// still a placeholder today, so starting the CDC without ever touching
	// the mode should not risk having something silently rewrite the user's
	// config. Switch to "automatic" explicitly in the config UI once wanted.
	cfg.Tuning.Mode = TuningModeManual

	// Hash a default API key. This key must be changed in production environments.
	cfg.Monitor.HashedAPIKeys = make(map[string]string)
	defaultAPIKey := "your-super-secret-api-key"
	hashedDefaultAPIKey := sha256.Sum256([]byte(defaultAPIKey))
	hexHashedKey := hex.EncodeToString(hashedDefaultAPIKey[:])
	cfg.Monitor.HashedAPIKeys[hexHashedKey] = "Default key"

	cfg.SaveDestination.Path = "./local_checkpoints"

	return cfg
}