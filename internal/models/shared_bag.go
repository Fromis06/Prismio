package models

import (
	"my-cdc/internal/pb"
	"sync/atomic"
)

// SharedEventBag đóng gói một slice sự kiện với một bộ đếm tham chiếu
// để đảm bảo nó chỉ được trả về pool sau khi tất cả consumer xử lý xong.
type SharedEventBag struct {
	Events   []*pb.ChangeEvent
	refCount atomic.Int32
}

// NewSharedEventBag tạo một túi được chia sẻ mới với số tham chiếu ban đầu.
func NewSharedEventBag(events []*pb.ChangeEvent, count int32) *SharedEventBag {
	bag := &SharedEventBag{
		Events: events,
	}
	bag.refCount.Store(count)
	return bag
}

// Done báo hiệu rằng một consumer đã xử lý xong túi.
// Nó giảm bộ đếm tham chiếu và trả túi về pool nếu bộ đếm về 0.
func (b *SharedEventBag) Done() {
	if b.refCount.Add(-1) == 0 {
		// Đây là consumer cuối cùng, trả slice bên dưới về pool.
		ChangeEventBagPool.Put(b.Events[:0])
	}
}
