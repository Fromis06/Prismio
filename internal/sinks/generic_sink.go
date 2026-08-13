package sinks

import (
	"log/slog"
	"my-cdc/internal/models"
	"my-cdc/internal/pb"
)

// [Pattern: Broadcast / Pub-Sub] MultiSink copies and broadcasts event bags to multiple destinations.

type MultiSink struct {
	pipelines []Pipeline
}

func NewMultiSink() *MultiSink {
	return &MultiSink{pipelines: make([]Pipeline, 0)}
}

// AddPipeline adds a child pipeline to the list to receive data.
func (m *MultiSink) AddPipeline(p Pipeline) {
	m.pipelines = append(m.pipelines, p)
}

// PendingEvents returns the HIGHEST backpressure level among active child pipelines.
// The system is only as fast as its slowest sink.
func (m *MultiSink) PendingEvents() int64 {
	var maxPending int64
	for _, p := range m.pipelines {
		if !p.IsActive() {
			continue
		}
		if pe := p.PendingEvents(); pe > maxPending {
			maxPending = pe
		}
	}
	return maxPending
}

// Start starts all child pipelines.
func (m *MultiSink) Start() error {
	for _, p := range m.pipelines {
		if err := p.Start(); err != nil {
			slog.Warn("Failed to start child pipeline", "error", err)
		}
	}
	return nil
}

// Stop safely stops all child pipelines.
func (m *MultiSink) Stop() error {
	for _, p := range m.pipelines {
		_ = p.Stop() // Ignore errors from one pipeline to ensure others are still stopped.
	}
	return nil
}

// IsActive checks if at least one of its child pipelines is active.
// A MultiSink is considered active if it has at least one active destination.
func (m *MultiSink) IsActive() bool {
	for _, p := range m.pipelines {
		if p.IsActive() {
			return true
		}
	}
	return false
}

// WriteShared implements the Pipeline interface. This method is not expected to be
// called on a MultiSink, as the primary entry point is WriteBatch. Calling it
// indicates a potential logic error in the pipeline's construction.
func (m *MultiSink) WriteShared(bag *models.SharedEventBag) {
	// We must call Done() to prevent the bag from leaking.
	bag.Done()
	slog.Error("MultiSink.WriteShared was called, which is not an expected workflow. The event bag was dropped to prevent data races.")
}

// WriteBatch sends an event bag to all registered child pipelines.
// It uses a reference counting mechanism to ensure the event bag
// is returned to the pool only after ALL pipelines have finished processing, preventing data races.
func (m *MultiSink) WriteBatch(events []*pb.ChangeEvent) error {
	if len(events) == 0 {
		return nil
	}

	// 1. Filter out active pipelines to determine the number of references.
	activePipelines := make([]Pipeline, 0, len(m.pipelines))
	for _, p := range m.pipelines {
		if p.IsActive() {
			activePipelines = append(activePipelines, p)
		}
	}

	// 2. If no pipelines are active, the MultiSink is responsible for returning the bag to the pool.
	if len(activePipelines) == 0 {
		models.ChangeEventBagPool.Put(events[:0])
		return nil
	}

	// 3. Create a shared event bag with a reference counter equal to the number of active pipelines.
	sharedBag := models.NewSharedEventBag(events, int32(len(activePipelines)))

	// 4. Send the shared bag to each active pipeline.
	for _, p := range activePipelines {
		p.WriteShared(sharedBag)
	}

	return nil
}
