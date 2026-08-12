package tuning

import (
	"log/slog"
	"time"

	"my-cdc/internal/config"
	"my-cdc/internal/models"
)

// AutoTuner is responsible for automatically adjusting system parameters based on performance metrics.
type AutoTuner struct {
	Config *config.AppConfig
	Counts *models.EventsCount
}

// NewAutoTuner creates a new instance of AutoTuner.
func NewAutoTuner(cfg *config.AppConfig, counts *models.EventsCount) *AutoTuner {
	return &AutoTuner{
		Config: cfg,
		Counts: counts,
	}
}

// Start launches the auto-tuning process in a separate goroutine.
func (at *AutoTuner) Start() {
	slog.Info("AUTO-TUNER: Starting...")
	go at.runLoop()
}

// runLoop is the main loop that periodically checks metrics and adjusts the configuration.
func (at *AutoTuner) runLoop() {
	// The tuning interval should be longer than the monitoring interval
	// to avoid overly frequent adjustments.
	interval := time.Duration(at.Config.Monitor.MonitorIntervalSec*2) * time.Second
	if interval <= 0 {
		interval = 10 * time.Second // Fallback value
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	var lastInsert, lastUpdate, lastDelete int64

	for range ticker.C {
		// Calculate the current Events Per Second (EPS).
		currentInsert := at.Counts.InsertCount.Load()
		currentUpdate := at.Counts.UpdateCount.Load()
		currentDelete := at.Counts.DeleteCount.Load()

		totalDelta := (currentInsert - lastInsert) + (currentUpdate - lastUpdate) + (currentDelete - lastDelete)
		eps := float64(totalDelta) / interval.Seconds()

		lastInsert = currentInsert
		lastUpdate = currentUpdate
		lastDelete = currentDelete

		// Apply the tuning logic based on the calculated EPS.
		at.applyTuningLogic(eps)
	}
}

// applyTuningLogic contains the logic for adjusting system parameters.
// It takes the current EPS and decides what actions to take.
// NOTE: This is currently a placeholder for more sophisticated logic.
func (at *AutoTuner) applyTuningLogic(eps float64) {
	// The idea is that this function will calculate new optimal values
	// and then apply them using atomic Store methods on the config.
	highTrafficThreshold := 10000.0 // This threshold should also be configurable.

	if eps > highTrafficThreshold {
		slog.Info("AUTO-TUNER: High traffic detected", "eps", eps, "action", "using high-traffic configuration (placeholder)")
	} else {
		slog.Info("AUTO-TUNER: Normal traffic detected", "eps", eps, "action", "using standard configuration (placeholder)")
	}
}