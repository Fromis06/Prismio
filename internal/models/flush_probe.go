package models

import (
	"math"
	"sync"
	"sync/atomic"
	"time"
)

// FlushProbe searches for an efficient batch size using Perturb and Observe
// (P&O). It handles three independent signals:
//
//  1. A full batch provides a normal P&O signal.
//  2. A significantly underfilled timeout can reduce the batch size after
//     enough consecutive observations. Near-full timeouts are treated as
//     timing noise while traffic remains high.
//  3. ForceSet applies an emergency RAM limit. While RAM throttling is active,
//     full-batch and timeout signals cannot change the batch size.
//
// Smoothed EPS and batch estimates reduce sensitivity to network jitter. A
// timeout underfill must be observed repeatedly before the probe reduces the
// batch size, preventing isolated timing fluctuations from causing a sharp
// downward adjustment.
type FlushProbe struct {
	mu sync.Mutex

	history    []probeRecord
	cap        int
	head       int
	filled     bool
	cumulative int64

	current   int64
	step      int64
	trend     int64
	lastEPS   float64
	haveEPS   bool
	bestEPS   float64
	bestBatch int64
	sinceMove int

	smoothedEPS float64
	lastRawEPS  float64 // Raw EPS from the latest flush, used for diagnostics only.

	// smoothedBatch is an EWMA of p.current, updated whenever the current
	// batch size changes.
	smoothedBatch     float64
	haveSmoothedBatch bool

	// consecutiveUnderfill counts consecutive significantly underfilled
	// timeouts. It resets after a near-full timeout or a full-batch flush.
	consecutiveUnderfill int

	lastFlushAt time.Time // Time of the latest full or timeout flush.

	ramThrottled       atomic.Bool
	minBatch, maxBatch int64
}

type probeRecord struct {
	at         time.Time
	batchSize  int64
	cumulative int64
}

const (
	probeHistoryCap   = 16
	probeInitialStep  = 500
	probeMinStep      = 100
	probeGrowFactor   = 1.5
	probeShrinkFactor = 0.5
	probeNoiseband    = 0.02
	probeReprobeEvery = 10
	probeEWMAAlpha    = 0.2

	// probeBatchEWMAAlpha smooths p.current into smoothedBatch. Batch size is
	// less noisy than EPS, so it can use a slightly more responsive alpha.
	probeBatchEWMAAlpha = 0.3

	// timeoutNearFullRatio treats a nearly full timeout as timing noise while
	// traffic remains high; it does not trigger decay.
	timeoutNearFullRatio = 0.2

	// timeoutDecayRate is the proportional reduction applied after enough
	// consecutive significantly underfilled timeouts.
	timeoutDecayRate = 0.10

	// minConsecutiveUnderfill prevents a single noisy timeout from shrinking
	// the batch size.
	minConsecutiveUnderfill = 5

	SafeMinBatch int64 = 200
	SafeMaxBatch int64 = 200000

	// RAMEmergencyMinBatch is the minimum batch size used during RAM
	// throttling. Keeping it above the normal floor avoids excessive per-flush
	// overhead while the backlog is being drained.
	RAMEmergencyMinBatch int64 = 2000
)

func NewFlushProbe(minBatch, maxBatch, initialBatch int64) *FlushProbe {
	initialBatch = clamp64(initialBatch, minBatch, maxBatch)
	return &FlushProbe{
		history:   make([]probeRecord, probeHistoryCap),
		cap:       probeHistoryCap,
		current:   initialBatch,
		step:      probeInitialStep,
		trend:     1,
		minBatch:  minBatch,
		maxBatch:  maxBatch,
		bestBatch: initialBatch,
	}
}

func (p *FlushProbe) SetRAMThrottled(v bool) {
	p.ramThrottled.Store(v)
}

// ForceSet applies an externally selected batch size, such as a RAM guard
// limit, and restarts probing from that value.
func (p *FlushProbe) ForceSet(v int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.current = clamp64(v, p.minBatch, p.maxBatch)
	p.trackBatchLocked()
	p.step = probeMinStep
	p.consecutiveUnderfill = 0 // Start a new underfill sequence.
}

// RecordFullFlush records a full batch as a normal P&O signal. It has no
// effect while RAM throttling is active.
func (p *FlushProbe) RecordFullFlush(n int64) int64 {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.updateEPSLocked(n)
	p.lastFlushAt = time.Now()

	// A full batch proves that traffic remains sufficient and breaks any
	// underfill sequence.
	p.consecutiveUnderfill = 0

	if p.ramThrottled.Load() {
		return p.current
	}
	// Use smoothed EPS to reduce the effect of RTT and network jitter.
	if p.smoothedEPS > 0 {
		p.observe(p.smoothedEPS, n)
	}
	return p.current
}

// RecordTimeoutFlush records a batch that did not fill before the timeout.
// It reduces the batch size only after repeated significant underfills.
//
// Near-full timeouts are treated as timing noise, and RAM throttling disables
// automatic changes entirely.
func (p *FlushProbe) RecordTimeoutFlush(n int64) int64 {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.updateEPSLocked(n)
	p.lastFlushAt = time.Now()

	if p.ramThrottled.Load() {
		return p.current
	}
	if p.current <= 0 {
		return p.current
	}

	fillRatio := float64(n) / float64(p.current)
	if fillRatio >= timeoutNearFullRatio {
		// A near-full timeout is likely timing noise while traffic remains high.
		p.consecutiveUnderfill = 0
		return p.current
	}

	// Accumulate evidence before reducing the batch size.
	p.consecutiveUnderfill++
	if p.consecutiveUnderfill < minConsecutiveUnderfill {
		return p.current
	}

	// Repeated underfills indicate that traffic is insufficient for the current
	// batch size. Apply the proportional reduction and restart the counter.
	next := int64(float64(p.current) * (1 - timeoutDecayRate))
	p.current = clamp64(next, p.minBatch, p.maxBatch)
	p.trackBatchLocked()
	p.step = probeMinStep
	p.consecutiveUnderfill = 0
	return p.current
}

func (p *FlushProbe) updateEPSLocked(n int64) float64 {
	now := time.Now()
	prev, hasPrev := p.latestLocked()

	p.cumulative += n
	p.push(probeRecord{at: now, batchSize: n, cumulative: p.cumulative})

	if !hasPrev {
		return 0
	}
	dt := now.Sub(prev.at).Seconds()
	if dt <= 0 {
		return 0
	}
	eps := float64(p.cumulative-prev.cumulative) / dt
	p.lastRawEPS = eps

	if p.smoothedEPS == 0 {
		p.smoothedEPS = eps
	} else {
		p.smoothedEPS = p.smoothedEPS*(1-probeEWMAAlpha) + eps*probeEWMAAlpha
	}
	// Track the smoothed signal so an isolated spike cannot become the best
	// observed result.
	if p.smoothedEPS > p.bestEPS {
		p.bestEPS = p.smoothedEPS
		p.bestBatch = n
	}
	return eps
}

// observe implements the core P&O step. RAM throttling is handled by the
// public recording methods before this function is called.
func (p *FlushProbe) observe(eps float64, batchUsed int64) {
	if !p.haveEPS {
		p.haveEPS = true
		p.lastEPS = eps
		p.move(p.trend)
		p.sinceMove = 0
		return
	}

	delta := (eps - p.lastEPS) / math.Max(p.lastEPS, 1)
	p.lastEPS = eps

	switch {
	case delta > probeNoiseband:
		p.step = min64(int64(float64(p.step)*probeGrowFactor), (p.maxBatch-p.minBatch)/4)
		p.move(p.trend)
		p.sinceMove = 0

	case delta < -probeNoiseband:
		if b, ok := p.fitLocalPeakLocked(); ok {
			p.jumpTo(b)
			p.step = probeMinStep
		} else {
			p.trend = -p.trend
			p.step = max64(int64(float64(p.step)*probeShrinkFactor), probeMinStep)
			p.move(p.trend)
		}
		p.sinceMove = 0

	default:
		p.sinceMove++
		if p.sinceMove >= probeReprobeEvery {
			p.step = probeMinStep
			p.move(p.trend)
			p.sinceMove = 0
		}
	}
}

func (p *FlushProbe) move(direction int64) {
	next := p.current + direction*p.step
	p.current = clamp64(next, p.minBatch, p.maxBatch)
	p.trackBatchLocked()
}

func (p *FlushProbe) jumpTo(b int64) {
	p.current = clamp64(b, p.minBatch, p.maxBatch)
	p.trackBatchLocked()
	p.trend = 1
}

// trackBatchLocked updates smoothedBatch after each current batch-size change.
func (p *FlushProbe) trackBatchLocked() {
	if !p.haveSmoothedBatch {
		p.smoothedBatch = float64(p.current)
		p.haveSmoothedBatch = true
		return
	}
	p.smoothedBatch = p.smoothedBatch*(1-probeBatchEWMAAlpha) + float64(p.current)*probeBatchEWMAAlpha
}

func (p *FlushProbe) fitLocalPeakLocked() (int64, bool) {
	recs := p.snapshotLocked()
	if len(recs) < 5 {
		return 0, false
	}

	type point struct{ b, eps float64 }
	pts := make([]point, 0, len(recs)-1)
	minB, maxB := math.MaxFloat64, -math.MaxFloat64
	for i := 1; i < len(recs); i++ {
		dt := recs[i].at.Sub(recs[i-1].at).Seconds()
		if dt <= 0 {
			continue
		}
		e := float64(recs[i].cumulative-recs[i-1].cumulative) / dt
		b := float64(recs[i].batchSize)
		pts = append(pts, point{b, e})
		if b < minB {
			minB = b
		}
		if b > maxB {
			maxB = b
		}
	}
	if len(pts) < 5 || maxB-minB < float64(probeMinStep) {
		return 0, false
	}

	var n, sx, sx2, sx3, sx4, sy, sxy, sx2y float64
	for _, pt := range pts {
		x, y := pt.b, pt.eps
		x2 := x * x
		n++
		sx += x
		sx2 += x2
		sx3 += x2 * x
		sx4 += x2 * x2
		sy += y
		sxy += x * y
		sx2y += x2 * y
	}

	det := det3(n, sx, sx2, sx, sx2, sx3, sx2, sx3, sx4)
	if math.Abs(det) < 1e-9 {
		return 0, false
	}
	b1 := det3(n, sy, sx2, sx, sxy, sx3, sx2, sx2y, sx4) / det
	a := det3(n, sx, sy, sx, sx2, sxy, sx2, sx3, sx2y) / det

	if a >= 0 {
		return 0, false
	}
	vertex := -b1 / (2 * a)
	if vertex < minB || vertex > maxB {
		return 0, false
	}
	return int64(vertex), true
}

func (p *FlushProbe) SmoothedEPS() float64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.smoothedEPS
}

// StableBatch returns the smoothed batch-size estimate used by the auto tuner.
func (p *FlushProbe) StableBatch() (int64, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.haveSmoothedBatch {
		return int64(p.smoothedBatch), true
	}
	return p.bestBatch, p.bestEPS > 0
}

// LastFlushAt returns the time of the latest full or timeout flush.
func (p *FlushProbe) LastFlushAt() time.Time {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.lastFlushAt
}

// FlushStats contains diagnostic values captured for a flush. It is not used
// to make tuning decisions.
type FlushStats struct {
	RawEPS      float64
	SmoothedEPS float64
	CurrentSize int64
	StableSize  int64
}

func (p *FlushProbe) LastFlushStats() FlushStats {
	p.mu.Lock()
	defer p.mu.Unlock()
	stable := p.bestBatch
	if p.haveSmoothedBatch {
		stable = int64(p.smoothedBatch)
	}
	return FlushStats{
		RawEPS:      p.lastRawEPS,
		SmoothedEPS: p.smoothedEPS,
		CurrentSize: p.current,
		StableSize:  stable,
	}
}

func (p *FlushProbe) latestLocked() (probeRecord, bool) {
	if !p.filled && p.head == 0 {
		return probeRecord{}, false
	}
	idx := (p.head - 1 + p.cap) % p.cap
	return p.history[idx], true
}

func (p *FlushProbe) push(r probeRecord) {
	p.history[p.head] = r
	p.head = (p.head + 1) % p.cap
	if p.head == 0 {
		p.filled = true
	}
}

func (p *FlushProbe) snapshotLocked() []probeRecord {
	if !p.filled {
		out := make([]probeRecord, p.head)
		copy(out, p.history[:p.head])
		return out
	}
	out := make([]probeRecord, p.cap)
	copy(out, p.history[p.head:])
	copy(out[p.cap-p.head:], p.history[:p.head])
	return out
}

func det3(a, b, c, d, e, f, g, h, i float64) float64 {
	return a*(e*i-f*h) - b*(d*i-f*g) + c*(d*h-e*g)
}

func clamp64(v, lo, hi int64) int64 {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
