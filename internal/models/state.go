package models

import (
	"sync"
	"sync/atomic"
)

// [Pattern: Thread-Safe State] GlobalState safely manages independent checkpoints for each Sink.
type GlobalState struct {
	checkpoints sync.Map // Key: Sink Name (string), Value: LSN (*atomic.Uint64).
}

// NewGlobalState initializes a new GlobalState.
func NewGlobalState() *GlobalState {
	return &GlobalState{}
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
