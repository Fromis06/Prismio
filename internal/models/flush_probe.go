package models

import (
	"math"
	"sync"
	"time"
)

// FlushProbe dò Batch Size tối ưu bằng Perturb & Observe (P&O), với 2 loại
// tín hiệu được xử lý HOÀN TOÀN TÁCH BIỆT, không cộng dồn lên nhau:
//
//  1. "Batch full" (RecordFullFlush) -> P&O bình thường: đây là tín hiệu
//     THẬT về khả năng đáp ứng của traffic hiện tại.
//  2. "Timeout" underfill đáng kể (RecordTimeoutFlush) -> ép GIẢM TỈ LỆ
//     (không qua cơ chế step chậm của P&O), NHƯNG chỉ khi batch lúc đó lấp
//     chưa đủ phần lớn target — nếu lấp gần đầy (>timeoutNearFullRatio) thì
//     coi là nhiễu định thời trong lúc traffic vẫn cao, bỏ qua để tránh
//     giật cục giữa spike.
//
// LƯU Ý: FlushProbe KHÔNG còn xử lý áp lực RAM. Trước đây RAM guard
// (AutoTuner) ép cắt BatchMaxSize qua ForceSet + đóng băng P&O qua
// SetRAMThrottled — nhưng cắt batch chỉ tối ưu THROUGHPUT của sink, không
// giải quyết nguyên nhân RAM tăng (tốc độ nạp từ WAL > tốc độ xả), và ở môi
// trường có RTT cao tới sink, cắt batch còn LÀM TỆ HƠN (throughput giảm ->
// backlog phình to nhanh hơn, đúng vòng xoáy đã quan sát trong thực tế).
// RAM guard giờ tác động ở phía INPUT (Listener tạm dừng đọc thêm WAL) thay
// vì OUTPUT (batch size) — xem GlobalState.SetRAMThrottled trong
// internal/models/state.go và waitForRAMRecovery trong
// internal/capture/postgres/listener.go.
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

	stableBatch int64
	haveStable  bool
	smoothedEPS float64

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

	// timeoutNearFullRatio: nếu batch lúc "Timeout" đã lấp được từ tỉ lệ
	// này trở lên so với target, coi là nhiễu định thời (ticker lệch pha
	// với tốc độ dồn dữ liệu trong lúc traffic vẫn cao), KHÔNG decay — đây
	timeoutNearFullRatio = 0.2

	// timeoutDecayRate: mức ép giảm khi Timeout với underfill THẬT SỰ đáng
	// kể (< timeoutNearFullRatio) — bỏ qua hoàn toàn cơ chế step chậm của
	// P&O, giải quyết đúng vấn đề "decay quá chậm, học vẹt trên đỉnh ảo".
	timeoutDecayRate = 0.10

	SafeMinBatch int64 = 200
	SafeMaxBatch int64 = 100_000
)

func NewFlushProbe(minBatch, maxBatch, initialBatch int64) *FlushProbe {
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

// RecordFullFlush: batch đã lấp đầy đúng target -> tín hiệu THẬT, đưa vào
// P&O bình thường.
func (p *FlushProbe) RecordFullFlush(n int64) int64 {
	p.mu.Lock()
	defer p.mu.Unlock()

	eps := p.updateEPSLocked(n)
	if eps > 0 {
		p.observe(eps, n)
	}
	return p.current
}

// RecordTimeoutFlush: batch KHÔNG lấp đầy trong thời gian chờ cho phép.
// Phân biệt "traffic giảm thật" (decay tỉ lệ) với "nhiễu định thời giữa
// spike" (bỏ qua) bằng fillRatio so với target.
func (p *FlushProbe) RecordTimeoutFlush(n int64) int64 {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.updateEPSLocked(n)
	if p.current <= 0 {
		return p.current
	}

	fillRatio := float64(n) / float64(p.current)
	if fillRatio >= timeoutNearFullRatio {
		// Suýt đầy khi Timeout bắn -> nhiều khả năng chỉ là lệch pha giữa
		// ticker và tốc độ dồn dữ liệu trong lúc traffic vẫn cao, KHÔNG
		// PHẢI traffic giảm thật. Giữ nguyên current, không decay.
		return p.current
	}

	// Underfill đáng kể -> tín hiệu THẬT rằng traffic đã tụt dưới mức cần
	// để lấp đầy batch hiện tại trong thời gian chờ. Ép giảm tỉ lệ ngay,
	// bỏ qua cơ chế step chậm của P&O.
	next := int64(float64(p.current) * (1 - timeoutDecayRate))
	p.current = clamp64(next, p.minBatch, p.maxBatch)
	p.stableBatch = p.current
	p.haveStable = true
	p.step = probeMinStep
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

	if p.smoothedEPS == 0 {
		p.smoothedEPS = eps
	} else {
		p.smoothedEPS = p.smoothedEPS*(1-probeEWMAAlpha) + eps*probeEWMAAlpha
	}
	if eps > p.bestEPS {
		p.bestEPS = eps
		p.bestBatch = n
	}
	return eps
}

// observe là lõi P&O gốc.
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
}

func (p *FlushProbe) jumpTo(b int64) {
	p.current = clamp64(b, p.minBatch, p.maxBatch)
	p.stableBatch = p.current
	p.haveStable = true
	p.trend = 1
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

func (p *FlushProbe) StableBatch() (int64, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.haveStable {
		return p.stableBatch, true
	}
	return p.bestBatch, p.bestEPS > 0
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