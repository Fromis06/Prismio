package models

import (
	"math"
	"sync"
	"sync/atomic"
	"time"
)

// FlushProbe dò Batch Size tối ưu bằng Perturb & Observe (P&O), với 3 loại
// tín hiệu được xử lý HOÀN TOÀN TÁCH BIỆT, không cộng dồn lên nhau:
//
//  1. "Batch full" (RecordFullFlush) -> P&O bình thường: đây là tín hiệu
//     THẬT về khả năng đáp ứng của traffic hiện tại.
//  2. "Timeout" underfill đáng kể (RecordTimeoutFlush) -> ép GIẢM TỈ LỆ
//     (không qua cơ chế step chậm của P&O), NHƯNG chỉ khi batch lúc đó lấp
//     chưa đủ phần lớn target — nếu lấp gần đầy (>timeoutNearFullRatio) thì
//     coi là nhiễu định thời trong lúc traffic vẫn cao, bỏ qua để tránh
//     giật cục giữa spike.
//  3. RAM khẩn cấp (ForceSet, gọi từ AutoTuner) -> ghi đè trực tiếp, và
//     trong lúc `ramThrottled==true`, CẢ (1) VÀ (2) ĐỀU BỊ ĐÓNG BĂNG hoàn
//     toàn — không tự ý giảm thêm ở mỗi lần flush — để RAM guard (tick theo
//     chu kỳ AutoTuner, chậm hơn nhiều so với tần suất flush) là nơi DUY
//     NHẤT quyết định trong tình huống khẩn cấp, tránh cộng dồn cắt giảm.
//
// GHI CHÚ (sau khi vá lỗi "timeout ngược" + "batch không leo lên đủ cao khi
// RTT cao"):
//   - observe() giờ ra quyết định dựa trên smoothedEPS (EWMA), KHÔNG dùng
//     eps thô tức thời nữa. eps thô giữa 2 lần flush liên tiếp bị nhiễu bởi
//     jitter của chính thời gian flush (RTT mạng), nên trước đây dễ khiến
//     P&O nhầm nhiễu mạng thành hiệu ứng thật của batch size, gây xu hướng
//     giảm nhiều hơn tăng khi RTT cao.
//   - "stableBatch" (giá trị dùng cho AutoTuner.tuneTimeout) trước đây chỉ
//     được cập nhật ở các mốc rời rạc (jumpTo/ForceSet/decay), nên trong
//     một pha đang leo dần qua move() nó bị "đứng yên" ở giá trị cũ trong
//     khi p.current (batch thật đang chạy) đã tăng lên nhiều — đây chính là
//     nguyên nhân hiện tượng "batch cao nhưng timeout tính ra thấp, batch
//     thấp nhưng timeout tính ra cao". Đã thay bằng smoothedBatch, một EWMA
//     luôn bám theo p.current mỗi khi nó thay đổi, ở bất kỳ nhánh nào.
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
	lastRawEPS  float64 // eps thô của lần flush gần nhất — chỉ dùng để log/chẩn đoán, KHÔNG dùng để ra quyết định.

	// smoothedBatch là EWMA của p.current, cập nhật ở MỌI nơi p.current thay
	// đổi (move/jumpTo/ForceSet/decay) — thay thế hoàn toàn "stableBatch" cũ
	// vốn chỉ cập nhật ở vài mốc rời rạc và dễ bị lag so với batch thật.
	smoothedBatch     float64
	haveSmoothedBatch bool

	lastFlushAt time.Time // thời điểm ghi nhận lần flush gần nhất (Full hoặc Timeout) — dùng để AutoTuner phát hiện tình trạng idle.

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

	// probeBatchEWMAAlpha làm mượt p.current thành smoothedBatch. Alpha lớn
	// hơn probeEWMAAlpha một chút vì batch size là giá trị AutoTuner mình
	// tự set (ít nhiễu đo lường hơn eps), nên có thể bám sát current nhanh
	// hơn mà vẫn tránh hiện tượng "nhảy số" mỗi micro-thay đổi.
	probeBatchEWMAAlpha = 0.3

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

	// RAMEmergencyMinBatch là sàn RIÊNG cho tình huống RAM khẩn cấp, cao
	// hơn SafeMinBatch — cắt tới 200 khi đang cần xả backlog gấp có thể
	// khiến chi phí cố định mỗi lần flush (network RTT, overhead pgx.Batch)
	// chiếm ưu thế, làm throughput sụp thêm thay vì phục hồi (đúng vòng
	// xoáy đã quan sát). Con số này nên tinh chỉnh lại theo C0 đo thực tế
	// trên hệ thống của bạn nếu có điều kiện.
	RAMEmergencyMinBatch int64 = 2000
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

func (p *FlushProbe) SetRAMThrottled(v bool) {
	p.ramThrottled.Store(v)
}

// ForceSet ép current/smoothedBatch về một giá trị cụ thể do BÊN NGOÀI quyết
// định (RAM guard) — probe không "cãi lại" ở lần flush kế tiếp, mà coi đây
// là điểm xuất phát mới, dò lại nhẹ nhàng (step co về mức nhỏ nhất) từ đó.
func (p *FlushProbe) ForceSet(v int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.current = clamp64(v, p.minBatch, p.maxBatch)
	p.trackBatchLocked()
	p.step = probeMinStep
}

// RecordFullFlush: batch đã lấp đầy đúng target -> tín hiệu THẬT, đưa vào
// P&O bình thường. Đóng băng hoàn toàn nếu đang ramThrottled.
func (p *FlushProbe) RecordFullFlush(n int64) int64 {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.updateEPSLocked(n)
	p.lastFlushAt = time.Now()

	if p.ramThrottled.Load() {
		return p.current
	}
	// Quyết định P&O dùng smoothedEPS (đã lọc nhiễu RTT/jitter qua EWMA),
	// KHÔNG dùng eps thô — xem doc-comment của struct để biết lý do.
	if p.smoothedEPS > 0 {
		p.observe(p.smoothedEPS, n)
	}
	return p.current
}

// RecordTimeoutFlush: batch KHÔNG lấp đầy trong thời gian chờ cho phép.
// Phân biệt "traffic giảm thật" (decay tỉ lệ) với "nhiễu định thời giữa
// spike" (bỏ qua) bằng fillRatio so với target. Đóng băng hoàn toàn nếu
// đang ramThrottled — lý do xem doc-comment của struct.
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
	p.trackBatchLocked()
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
	p.lastRawEPS = eps

	if p.smoothedEPS == 0 {
		p.smoothedEPS = eps
	} else {
		p.smoothedEPS = p.smoothedEPS*(1-probeEWMAAlpha) + eps*probeEWMAAlpha
	}
	// bestEPS/bestBatch giờ theo dõi smoothedEPS thay vì eps thô, cùng lý do
	// lọc nhiễu như observe() — tránh "best ever" bị ghi nhận nhầm từ một
	// đỉnh nhiễu tức thời không lặp lại được.
	if p.smoothedEPS > p.bestEPS {
		p.bestEPS = p.smoothedEPS
		p.bestBatch = n
	}
	return eps
}

// observe là lõi P&O gốc, KHÔNG còn nhánh ramThrottled bên trong — việc
// đóng băng đã được xử lý ở đầu RecordFullFlush/RecordTimeoutFlush, nên khi
// hàm này chạy, chắc chắn không đang trong tình trạng RAM khẩn cấp.
// eps truyền vào đây LUÔN là smoothedEPS (xem RecordFullFlush).
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

// trackBatchLocked cập nhật smoothedBatch (EWMA của p.current) — được gọi ở
// MỌI nơi p.current thay đổi (move/jumpTo/ForceSet/decay), nên
// StableBatch() không bao giờ bị "đứng yên" lag lại phía sau batch thật
// đang chạy như "stableBatch" cũ.
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

// StableBatch trả về ước lượng batch size "ổn định" dùng cho
// AutoTuner.tuneTimeout — nay là smoothedBatch (EWMA bám sát p.current mọi
// lúc), thay cho stableBatch cũ vốn chỉ cập nhật ở vài mốc rời rạc.
func (p *FlushProbe) StableBatch() (int64, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.haveSmoothedBatch {
		return int64(p.smoothedBatch), true
	}
	return p.bestBatch, p.bestEPS > 0
}

// LastFlushAt trả về thời điểm ghi nhận lần flush gần nhất (Full hoặc
// Timeout). AutoTuner dùng giá trị này để phân biệt "traffic đang thấp
// thật" với "hệ thống hoàn toàn idle" — trước đây không phân biệt được,
// khiến tuneTimeout tính lại timeout từ dữ liệu đã đóng băng mỗi khi idle,
// luôn bị đẩy lên trần maxTimeoutMs một cách vô nghĩa (xem auto_tuner.go).
func (p *FlushProbe) LastFlushAt() time.Time {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.lastFlushAt
}

// FlushStats là một snapshot chỉ dùng cho mục đích log/chẩn đoán chi tiết
// mỗi lần flush (xem DataProcessor.flusherLoop) — KHÔNG dùng để ra quyết
// định tuning, chỉ để có dữ liệu thực nghiệm khi cần kiểm chứng hành vi
// P&O (ví dụ so sánh RawEPS jitter cao bao nhiêu so với SmoothedEPS).
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