package sinks

import (
	"log/slog"
	"my-cdc/internal/models"
	"my-cdc/internal/pb"
)

// [Pattern: Broadcast / Pub-Sub] MultiSink sao chép và phát sóng túi sự kiện tới nhiều đích.

type MultiSink struct {
	pipelines []Pipeline
}

func NewMultiSink() *MultiSink {
	return &MultiSink{pipelines: make([]Pipeline, 0)}
}

// AddPipeline thêm một pipeline con vào danh sách để nhận dữ liệu.
func (m *MultiSink) AddPipeline(p Pipeline) {
	m.pipelines = append(m.pipelines, p)
}

// Start khởi động tất cả các pipeline con.
func (m *MultiSink) Start() error {
	for _, p := range m.pipelines {
		if err := p.Start(); err != nil {
			slog.Warn("Failed to start child pipeline", "error", err)
		}
	}
	return nil
}

// Stop dừng tất cả các pipeline con một cách an toàn.
func (m *MultiSink) Stop() error {
	for _, p := range m.pipelines {
		_ = p.Stop() // Bỏ qua lỗi của một pipeline để đảm bảo các pipeline khác vẫn được dừng.
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

// WriteBatch gửi một túi sự kiện đến tất cả các pipeline con đã đăng ký.
// Nó sử dụng cơ chế đếm tham chiếu (reference counting) để đảm bảo túi sự kiện
// chỉ được trả về pool sau khi TẤT CẢ các pipeline xử lý xong, tránh data race.
func (m *MultiSink) WriteBatch(events []*pb.ChangeEvent) error {
	if len(events) == 0 {
		return nil
	}

	// 1. Lọc ra các pipeline đang hoạt động để xác định số lượng tham chiếu.
	activePipelines := make([]Pipeline, 0, len(m.pipelines))
	for _, p := range m.pipelines {
		if p.IsActive() {
			activePipelines = append(activePipelines, p)
		}
	}

	// 2. Nếu không có pipeline nào hoạt động, MultiSink chịu trách nhiệm trả lại túi vào pool.
	if len(activePipelines) == 0 {
		models.ChangeEventBagPool.Put(events[:0])
		return nil
	}

	// 3. Tạo một túi sự kiện được chia sẻ với bộ đếm tham chiếu bằng số pipeline đang hoạt động.
	sharedBag := models.NewSharedEventBag(events, int32(len(activePipelines)))

	// 4. Gửi túi được chia sẻ đến từng pipeline đang hoạt động.
	for _, p := range activePipelines {
		p.WriteShared(sharedBag)
	}

	return nil
}
