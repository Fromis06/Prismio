package models

import (
	"my-cdc/internal/pb"
	"sync/atomic"
)

// SharedEventBag wraps an event slice with a reference counter to ensure
// it is returned to the pool only after all consumers have finished processing it.
type SharedEventBag struct {
	Events   []*pb.ChangeEvent
	refCount atomic.Int32
}

// NewSharedEventBag creates a new shared bag with an initial reference count.
func NewSharedEventBag(events []*pb.ChangeEvent, count int32) *SharedEventBag {
	bag := &SharedEventBag{
		Events: events,
	}
	bag.refCount.Store(count)
	return bag
}

// Done signals that a consumer has finished with the bag.
// It decrements the reference counter and returns the bag to the pool if the count reaches zero.
func (b *SharedEventBag) Done() {
	if b.refCount.Add(-1) == 0 {
		// This was the last consumer, so return the underlying slice to the pool.
		ChangeEventBagPool.Put(b.Events[:0])
	}
}
