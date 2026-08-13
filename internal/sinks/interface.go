package sinks

import (
	"context"
	"my-cdc/internal/models"
	"my-cdc/internal/pb"
)

// QueryBuilder defines the behavior for converting a standardized ChangeEvent
// into a database-specific SQL statement.
type QueryBuilder interface {
	// BuildQuery takes an event and returns a query string and a slice of arguments,
	// which helps prevent SQL injection attacks.
	BuildQuery(event *pb.ChangeEvent) (query string, args []any)
}

// DatabaseExecutor is responsible for managing the physical connection
// and executing SQL statements on the target database.
type DatabaseExecutor interface {
	Init(ctx context.Context, url string) error                                 // Initializes the connection (e.g., creates a connection pool).
	ExecuteBatch(ctx context.Context, queries []string, argsList [][]any) error // Executes a batch of statements.
	Close() error                                                               // Closes the connection and releases resources.
}

// Pipeline represents a complete data processing flow for a target database.
// It includes receiving data, processing it, and writing it to the destination.
type Pipeline interface {
	Start() error
	WriteBatch(events []*pb.ChangeEvent) error
	WriteShared(bag *models.SharedEventBag)
	Stop() error
	IsActive() bool
	PendingEvents() int64
}
