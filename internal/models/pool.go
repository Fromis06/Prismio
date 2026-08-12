package models

import ( "sync"

		"my-cdc/internal/pb"
)

// [Pattern: Object Pool] ChangeEventBagPool reuses slice memory to reduce the load on the Garbage Collector.
var ChangeEventBagPool sync.Pool

// QueryPool reuses slices containing SQL strings.
var QueryPool = sync.Pool{
	New: func() any {
		return make([]string, 0, 5000) // Pre-allocate capacity
	},
}

// ArgsPool reuses slices containing parameter arrays.
var ArgsPool = sync.Pool{
	New: func() any {
		return make([][]any, 0, 5000)
	},
}

// InitBagPool initializes the pool's New function with a capacity taken from the configuration.
func InitBagPool(capacity int) {
	ChangeEventBagPool.New = func() any {
		return make([]*pb.ChangeEvent, 0, capacity)
	}
}