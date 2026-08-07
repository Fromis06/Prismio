package models

import "sync/atomic"

// EventsCount holds various counters for monitoring purposes.
// It uses atomic types to ensure safe concurrent access from multiple goroutines
// without the need for mutexes, improving performance.
type EventsCount struct {
	InsertCount atomic.Int64 // Total number of INSERT events processed.
	UpdateCount atomic.Int64 // Total number of UPDATE events processed.
	DeleteCount atomic.Int64 // Total number of DELETE events processed.
}
