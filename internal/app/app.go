package app

import (
	"context"
	"fmt"
	"os"
	"time"

	"log/slog"
	"my-cdc/internal/capture"
	"my-cdc/internal/config"
	"my-cdc/internal/models"
	"my-cdc/internal/pb"
	"my-cdc/internal/sinks"
	"my-cdc/internal/tuning"
	"my-cdc/internal/utils"
)

// Application encapsulates all core components of the CDC system.
// This struct helps manage the global state of the application.
type Application struct {
	Config      *config.AppConfig
	GlobalState *models.GlobalState
	EventsCount *models.EventsCount
	MultiSink   *sinks.MultiSink
	Listener    capture.Listener
	AutoTuner   *tuning.AutoTuner
}

// Bootstrap initializes all major components of the application.
// This function should only be called after the configuration has been loaded and validated.
func Bootstrap(ctx context.Context, cfg *config.AppConfig) (*Application, error) {
	// Fail fast with a clear message instead of letting CreateListener /
	// BuildAndAddPipeline fail later with a generic "unsupported type: ''"
	// error when the source or every destination was left unconfigured.
	if cfg.Provider.Source.Type == "" {
		return nil, fmt.Errorf("no data source selected: please configure it before running")
	}

	hasActiveConsumer := false
	for _, c := range cfg.Consumers.List {
		if c.IsActive && c.Type != "" {
			hasActiveConsumer = true
			break
		}
	}
	if !hasActiveConsumer {
		return nil, fmt.Errorf("at least 1 active destination is required before running")
	}

	poolCapacity := int(cfg.Bag.BagMaxSize.Load() * int64(cfg.Bag.BagMaxMultiple.Load()))
	models.InitBagPool(poolCapacity)

	eventsCount := &models.EventsCount{}
	globalState := models.NewGlobalState()

	// Ensure the checkpoint directory exists on startup, independent of whether there's
	// data to save yet. SaveProviderCheckpoint is only called after a transaction
	// with LSN > 0 is processed. Creating the directory late would mean a user might
	// not know about a disk permission error until data flows through, which could be
	// confused with a connection issue. Creating it here allows the application to
	// fail-fast and informs the user where checkpoints will be stored.
	if err := os.MkdirAll(cfg.SaveDestination.Path, 0755); err != nil {
		return nil, fmt.Errorf("unable to create checkpoint storage directory [%s]: %w", cfg.SaveDestination.Path, err)
	}
	slog.Info("CHECKPOINT: Storage directory is ready", "path", cfg.SaveDestination.Path)

	// The source type is a string taken directly from the config (e.g., "postgres"),
	// which must match a driver name registered in the sinks registry.
	sourceType := cfg.Provider.Source.Type
	instanceName := cfg.Provider.Source.Name

	slog.Info("CHECKPOINT: Checking history...")
	ckptData, err := utils.LoadProviderCheckpoint(cfg.SaveDestination, sourceType, instanceName)
	if err != nil {
		return nil, fmt.Errorf("critical error reading checkpoint file: %w", err)
	}

	recoveredLSN := uint64(0)
	if ckptData != nil && ckptData.CheckpointData != nil {
		if lsn, ok := ckptData.CheckpointData.Offset.(*pb.Checkpoint_Lsn); ok && lsn.Lsn > 0 {
			recoveredLSN = lsn.Lsn
			lastSaved := time.Unix(ckptData.UpdatedAt, 0).Format("2006-01-02 15:04:05")
			slog.Info("CHECKPOINT: Successfully recovered", "lsn", recoveredLSN, "last_saved", lastSaved)
		}
	}
	if recoveredLSN == 0 {
		slog.Info("CHECKPOINT: No previous checkpoint found, will start from the latest LSN.")
	}

	// Initialize the starting checkpoint for all active consumers.
	for _, consumer := range cfg.Consumers.List {
		if consumer.IsActive {
			globalState.InitSink(consumer.Name, recoveredLSN)
		}
	}

	multiSink := sinks.NewMultiSink()

	for _, consumer := range cfg.Consumers.List {
		if !consumer.IsActive {
			slog.Info("SINK: Skipping inactive destination", "sink_name", consumer.Name)
			continue
		}

		// Build and add the sink pipeline based on its type.
		if err := sinks.BuildAndAddPipeline(ctx, consumer.Type, consumer.Name, cfg, consumer.URL, globalState, multiSink); err != nil {
			return nil, fmt.Errorf("failed to initialize destination [%s]: %w", consumer.Name, err)
		}
		slog.Info("SINK: Pipeline initialized for destination", "sink_name", consumer.Name)
	}

	// Initialize the data source (capture listener).
	listener, err := capture.CreateListener(cfg.Provider.Source.Type, cfg, multiSink, eventsCount)
	if err != nil {
		return nil, fmt.Errorf("error initializing source: %w", err)
	}

	autoTuner := tuning.NewAutoTuner(cfg, eventsCount)

	return &Application{
		Config:      cfg,
		GlobalState: globalState,
		EventsCount: eventsCount,
		MultiSink:   multiSink,
		Listener:    listener,
		AutoTuner:   autoTuner,
	}, nil
}

// Shutdown performs a graceful shutdown, primarily by saving the final checkpoint state
// before the application exits.
func (a *Application) Shutdown() {
	sourceType := a.Config.Provider.Source.Type
	finalLSN := a.GlobalState.GetMinCheckpoint()
	if finalLSN > 0 {
		finalData := models.CheckpointFileData{
			InstanceName:   a.Config.Provider.Source.Name,
			SourceType:     sourceType,
			CheckpointData: &pb.Checkpoint{Offset: &pb.Checkpoint_Lsn{Lsn: finalLSN}},
		}
		if errSave := utils.SaveProviderCheckpoint(a.Config.SaveDestination, finalData); errSave != nil {
			slog.Error("CHECKPOINT: Error saving final checkpoint", "error", errSave)
		} else {
			slog.Info("CHECKPOINT: Successfully saved final LSN.", "lsn", finalLSN)
		}
	} else {
		// This occurs if the app is shut down before processing any transactions.
		slog.Info("CHECKPOINT: No new data to save (no transactions processed), skipping final checkpoint.")
	}
}