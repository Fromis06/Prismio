package models

import (
	"math"
	"sync"
	"sync/atomic"
	"time"
)

// FlushProbe automatically probes for the optimal Batch Size using the Perturb & Observe
// (P&O) algorithm — similar to the idea behind MPPT (Maximum Power Point Tracking) in
// solar panels: continuously nudge the batch size in one direction, observe the EPS
// response ("balancing stick" — tilt one way, if it improves, tilt more; if it worsens,
// reverse direction and nudge less, never staying completely still).
//
// Unlike the old approach (least-squares on 200 samples, requiring 6 different batch sizes
// to be trusted, hard ceiling based on EPS*maxTimeout could jump straight to hundreds of thousands):
// FlushProbe keeps a SHORT history (16 most recent records), reacts IMMEDIATELY after each flush
// instead of waiting for a 10s tick, and only changes by AT MOST one step at a time —
// it cannot suddenly jump to an extreme value.
type FlushProbe struct {
	mu sync.Mutex

	history    []probeRecord
	cap        int
	head       int
	filled     bool
	cumulative int64

	// Hill-climbing (P&O) state
	current   int64
	step      int64
	trend     int64
	lastEPS   float64
	haveEPS   bool
	bestEPS   float64
	bestBatch int64
	sinceMove int // Number of stable flushes since the last batch size adjustment.

	// The CONVERGED estimate, updated only when a peak is successfully fitted — used
	// specifically for Timeout. It deliberately DOES NOT use `current` (which
	// continuously fluctuates during probing) to avoid the "alternating large/small batch"
	// issue encountered previously.
	stableBatch int64
	haveStable  bool
	smoothedEPS float64

	ramThrottled       atomic.Bool
	minBatch, maxBatch int64 // Absolute SAFETY RAIL, not a target.
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
	probeNoiseband    = 0.02 // ±2%: considered "unchanged" to resist measurement noise
	probeReprobeEvery = 10   // after this many "stable" flushes, proactively nudge again to catch a peak that has shifted over time
	probeEWMAAlpha    = 0.2
	// SafeMinBatch/SafeMaxBatch are the ultimate SAFETY RAILS, not target
	// values — the truly optimal batch will converge to something much smaller
	// than SafeMaxBatch once the actual EPS peak is found.
	SafeMinBatch int64 = 200
	SafeMaxBatch int64 = 100_000
)

// NewFlushProbe creates a new probe, starting from initialBatch (should be set to minBatch,
// meaning it starts from the lowest level and climbs up, in the spirit of "initial batch
// tuning starting near 0").
func NewFlushProbe(minBatch, maxBatch, initialBatch int64) *FlushProbe {
	return &FlushProbe{
		// Initialize history as a ring buffer
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

// SetRAMThrottled is called by AutoTuner whenever the RAM guard changes state. When
// throttled, the probe can only decrease, not increase, regardless of EPS.
func (p *FlushProbe) SetRAMThrottled(v bool) {
	p.ramThrottled.Store(v)
}

// RecordFlush is called by DataProcessor immediately after EACH successful flush —
// it does not wait for AutoTuner's periodic tick, which is why it reacts much faster
// than the previous design. Returns the batch size to be used for the next flush.
func (p *FlushProbe) RecordFlush(batchSize int64) int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := time.Now()
	prev, hasPrev := p.latestLocked()

	p.cumulative += batchSize
	p.push(probeRecord{at: now, batchSize: batchSize, cumulative: p.cumulative})

	if !hasPrev {
		return p.current // Need at least 2 points to calculate EPS
	}

	dt := now.Sub(prev.at).Seconds()
	if dt <= 0 {
		return p.current
	}
	// EPS = difference between 2 cumulative points / time between 2 points — as discussed
	// ("this record minus that record"). Mathematically, it equals batchSize/dt because
	// cumulative always increases by exactly batchSize on each call, but written as cumulative
	// to be more general if sampling at other points is desired later.
	eps := float64(p.cumulative-prev.cumulative) / dt

	if p.smoothedEPS == 0 {
		p.smoothedEPS = eps
	} else {
		p.smoothedEPS = p.smoothedEPS*(1-probeEWMAAlpha) + eps*probeEWMAAlpha
	}

	p.observe(eps, batchSize)
	return p.current
}

// observe is the core of the Perturb & Observe algorithm.
func (p *FlushProbe) observe(eps float64, batchUsed int64) {
	if eps > p.bestEPS {
		p.bestEPS = eps
		p.bestBatch = batchUsed
	}

	if p.ramThrottled.Load() { // RAM is constrained: always decrease, ignore EPS
		p.move(-1)
		p.trend = -1
		p.sinceMove = 0
		return
	}

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
		// Improving -> continue in the same direction, accelerate the step size (like momentum).
		p.step = min64(int64(float64(p.step)*probeGrowFactor), (p.maxBatch-p.minBatch)/4)
		p.move(p.trend)
		p.sinceMove = 0

	case delta < -probeNoiseband:
		// Worsening -> just overshot the peak. Try to fit a local quadratic from the short history
		// to jump closer to the peak, instead of slowly binary searching.
		if b, ok := p.fitLocalPeakLocked(); ok {
			p.jumpTo(b)
			p.step = probeMinStep // Converged, reduce step size, just "balance" gently around the peak.
		} else {
			p.trend = -p.trend
			p.step = max64(int64(float64(p.step)*probeShrinkFactor), probeMinStep)
			p.move(p.trend)
		}
		p.sinceMove = 0
	default:
		// Nearly unchanged -> considered at the peak. DO NOT stay still forever —
		// after every `probeReprobeEvery` stable flushes, actively nudge a small step
		// to detect if the peak has shifted.
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
}

func (p *FlushProbe) jumpTo(b int64) {
	p.current = clamp64(b, p.minBatch, p.maxBatch)
	p.stableBatch = p.current
	p.haveStable = true
	p.trend = 1
}

// fitLocalPeakLocked performs a quadratic least-squares fit (E = a*B² + b*B + c) on the
// short available history. It returns the vertex (peak) if the parabola opens downwards (a<0)
// AND the peak lies WITHIN the range of batch sizes actually probed — it does not extrapolate
// outside the observed region (extrapolating with limited data is very risky, similar to the
// previous analysis about solving a 3-point system amplifying errors).
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
		return 0, false // Not enough diverse data to fit meaningfully
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
		return 0, false // Not a downward-opening parabola in the current data -> don't trust
	}
	vertex := -b1 / (2 * a)
	if vertex < minB || vertex > maxB {
		return 0, false // Peak extrapolates outside the probed region -> don't trust, let P&O continue probing instead of guessing
	}
	return int64(vertex), true
}

// SmoothedEPS returns the EWMA-smoothed EPS — more stable than instantaneous EPS
// used to decide the probing direction, suitable as input for Timeout.
func (p *FlushProbe) SmoothedEPS() float64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.smoothedEPS
}

// StableBatch returns the CONVERGED estimate (from the most recent successful peak fit),
// false if it has never converged. It deliberately DOES NOT return `current` — that
// value continuously fluctuates during probing, using it for Timeout would cause
// Timeout to be erratic, exactly the problem encountered in the previous design.
func (p *FlushProbe) StableBatch() (int64, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.haveStable {
		return p.stableBatch, true
	}
	return p.bestBatch, p.bestEPS > 0 // fallback: best point ever seen, even if not officially peak-fitted
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
