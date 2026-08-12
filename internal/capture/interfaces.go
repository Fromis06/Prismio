package capture

import (
	"context"

	"my-cdc/internal/models"
	"my-cdc/internal/sinks"
)

// SourceCapture is the common interface for all types of source databases
type SourceCapture interface {
	Start(ctx context.Context, sourceURL string, targetSink sinks.Pipeline, eventsCount *models.EventsCount) error
	Stop() error
}