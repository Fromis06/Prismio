package sinks

import (
	"context"
	"fmt"
	"sort"

	"my-cdc/internal/config"
	"my-cdc/internal/models"
)

// AppenderFactory, following the Driver Factory pattern, creates a sink instance
// and attaches it directly to the provided MultiSink.
type AppenderFactory func(ctx context.Context, consumerName string, cfg *config.AppConfig, consumerURL string, state *models.GlobalState, multiSink *MultiSink) error

// Metadata contains display information for a sink type, used for building UIs
// (like TUI dropdowns) without needing a separate configuration file.
// Each driver declares its own metadata when it calls Register(), ensuring the UI
// list always matches the drivers that were actually compiled into the binary,
// preventing any synchronization drift.
type Metadata struct {
	DisplayName string // User-friendly display name, e.g., "PostgreSQL".
	URLTemplate string // A connection string template to guide the user.
}

// RegisteredSink is the result returned by ListRegistered, pairing a
// registered name with its display metadata.
type RegisteredSink struct {
	Type     string
	Metadata Metadata
}

// registeredDriver holds the factory and metadata for a registered sink driver.
type registeredDriver struct {
	Metadata Metadata
	Factory  AppenderFactory
}

// registry stores all registered sink drivers.
var registry = make(map[string]registeredDriver)

// Register adds a new sink driver (e.g., PostgreSQL, Kafka) and its
// associated display metadata to the global registry.
func Register(name string, meta Metadata, factory AppenderFactory) {
	registry[name] = registeredDriver{Metadata: meta, Factory: factory}
}

// BuildAndAddPipeline finds a registered driver by type, uses its factory to
// create a new pipeline, and adds it to the provided MultiSink.
func BuildAndAddPipeline(ctx context.Context, sinkType string, consumerName string, cfg *config.AppConfig, consumerURL string, state *models.GlobalState, multiSink *MultiSink) error {
	if driver, exists := registry[sinkType]; exists {
		return driver.Factory(ctx, consumerName, cfg, consumerURL, state, multiSink)
	}
	return fmt.Errorf("không hỗ trợ consumer type: %s", sinkType)
}

// ListRegistered returns a slice of all registered sinks, sorted by name for
// stable UI presentation. This function should be used to build UI selection
// elements (like dropdowns) instead of reading from a separate config file,
// as it guarantees the list is 100% consistent with the compiled drivers.
func ListRegistered() []RegisteredSink {
	result := make([]RegisteredSink, 0, len(registry))
	for name, driver := range registry {
		result = append(result, RegisteredSink{Type: name, Metadata: driver.Metadata})
	}
	// Sort by type name to ensure a consistent order in the UI.
	sort.Slice(result, func(i, j int) bool { return result[i].Type < result[j].Type })
	return result
}
