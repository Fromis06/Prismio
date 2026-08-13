package models

import (
	"sync"
	"sync/atomic"
	"time"
)

type FlushSample struct {
	BatchSize int64
	Duration  time.Duration
	Reason    string
	At        time.Time
}

// [Pattern: Thread-Safe State] GlobalState safely manages independent checkpoints for each Sink.
type GlobalState struct {
	checkpoints sync.Map
	Probe       *FlushProbe // Replaces old flushMu/flushBuf/... fields
}

// NewGlobalState initializes a new GlobalState.
const defaultFlushHistoryCap = 500

func NewGlobalState() *GlobalState {
	return &GlobalState{ // Starts from the lowest level, then climbs up automatically
		Probe: NewFlushProbe(SafeMinBatch, SafeMaxBatch, SafeMinBatch),
	}
}

// InitSink initializes the initial checkpoint for a specific Sink.
func (g *GlobalState) InitSink(sinkName string, initialVal uint64) {
	val := &atomic.Uint64{}
	val.Store(initialVal)
	g.checkpoints.Store(sinkName, val)
}

// RemoveSink removes a Sink's checkpoint from the system.
// Used when a Sink fails and needs to be disconnected without affecting the overall process.
func (g *GlobalState) RemoveSink(sinkName string) {
	g.checkpoints.Delete(sinkName)
}

// UpdateCheckpoint updates the checkpoint for a specific Sink.
func (g *GlobalState) UpdateCheckpoint(sinkName string, val uint64) {
	actual, ok := g.checkpoints.Load(sinkName)
	if !ok {
		return
	}
	atomicVal := actual.(*atomic.Uint64)
	for {
		current := atomicVal.Load()
		if val <= current {
			return
		}
		if atomicVal.CompareAndSwap(current, val) {
			return
		}
	}
}

// GetMinCheckpoint returns the smallest checkpoint among all Sinks.
func (g *GlobalState) GetMinCheckpoint() uint64 {
	var min uint64 = 0
	first := true

	g.checkpoints.Range(func(key, value any) bool {
		val := value.(*atomic.Uint64).Load()
		if first {
			min = val
			first = false
		} else if val < min {
			min = val
		}
		return true
	})

	return min
}

// ActiveSinks returns the list of Sink names that currently have a checkpoint
// (i.e., are still active — RemoveSink() removes them from the map when a sink disconnects).
func (g *GlobalState) ActiveSinks() []string {
	var sinks []string
	g.checkpoints.Range(func(key, value any) bool {
		sinks = append(sinks, key.(string))
		return true
	})
	return sinks
}
