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

	// ramThrottled is set by AutoTuner's RAM guard and read by the Listener
	// to pause pulling new WAL data from the source when RAM pressure is
	// high. See SetRAMThrottled/IsRAMThrottled below.
	//
	// This REPLACES the old approach of cutting BatchMaxSize / freezing
	// FlushProbe when RAM hit its ceiling. Cutting batch size only tunes
	// flush throughput at the sink; it does not address the underlying cause.
	// of RAM growth (WAL ingest rate outpacing flush rate), and over
	// high-RTT links to the sink it actively makes things worse: fewer
	// events per round-trip means lower throughput, which widens the
	// ingest/flush gap and grows the backlog (and RAM) faster, not slower.
	//
	// Backpressure now happens at the INPUT side instead: the Listener
	// stops reading further WAL data while ramThrottled is true (see
	// waitForRAMRecovery in internal/capture/postgres/listener.go). This
	// leaves flush batches (and therefore flush throughput) untouched, and
	// relies on Postgres's own replication flow control / TCP backpressure
	// to hold data back at the source until this app's backlog drains and
	// RAM recovers.
	ramThrottled atomic.Bool
}

// NewGlobalState initializes a new GlobalState. The probe starts from the
// batch size selected by the user, rather than a fixed minimum, so enabling
// automatic tuning never begins from an unrelated value.
const defaultFlushHistoryCap = 500

func NewGlobalState(initialBatchSize int64) *GlobalState {
	return &GlobalState{ // Starts from the lowest level, then climbs up automatically
		Probe: NewFlushProbe(SafeMinBatch, SafeMaxBatch, initialBatchSize),
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

// SetRAMThrottled is called by AutoTuner's RAM guard (internal/tuning/auto_tuner.go)
// to signal system-wide backpressure. See the doc-comment on the
// ramThrottled field for why this throttles the INPUT side (Listener)
// instead of cutting BatchMaxSize on the OUTPUT side.
func (g *GlobalState) SetRAMThrottled(v bool) {
	g.ramThrottled.Store(v)
}

// IsRAMThrottled reports whether the system is currently under RAM pressure.
// Polled by the Listener's read loop (see waitForRAMRecovery in
// internal/capture/postgres/listener.go) to decide whether to pause pulling
// more WAL data from the source.
func (g *GlobalState) IsRAMThrottled() bool {
	return g.ramThrottled.Load()
}