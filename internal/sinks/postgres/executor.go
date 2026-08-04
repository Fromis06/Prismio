package postgres

import (
	"context"
	"log/slog"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// [Pattern: Connection Pool] Executor maintains a connection pool to speed up writes.
type Executor struct {
	Pool *pgxpool.Pool
}

// Init opens a connection using a connection pool instead of a single connection.
func (pe *Executor) Init(ctx context.Context, url string) error {
	slog.Info("SINK (Postgres): Initializing connection pool")
	var err error

	// Parse the URL string to allow for further customization of the pool configuration if needed.
	config, err := pgxpool.ParseConfig(url)
	if err != nil {
		return err
	}

	pe.Pool, err = pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return err
	}

	// Check the actual connection
	if err := pe.Pool.Ping(ctx); err != nil {
		return err
	}

	slog.Info("SINK (Postgres): Connection pool is ready")
	return nil
}

// [Pattern: Batch Execution] ExecuteBatch sends multiple SQL commands in a single network round-trip to optimize I/O.
func (pe *Executor) ExecuteBatch(ctx context.Context, queries []string, argsList [][]any) error {
	if len(queries) == 0 {
		return nil
	}

	batch := &pgx.Batch{}
	for i, q := range queries {
		batch.Queue(q, argsList[i]...)
	}

	br := pe.Pool.SendBatch(ctx, batch)
	defer br.Close()

	for i := 0; i < len(queries); i++ {
		if _, err := br.Exec(); err != nil {
			// Log details about the failing command for easy debugging.
			return err // The error has been logged in DataProcessor, just return it here
		}
	}

	return nil
}

// Close cleans up resources when the system shuts down.
func (pe *Executor) Close() error {
	if pe.Pool != nil {
		slog.Info("SINK (Postgres): Closing connection pool")
		pe.Pool.Close()
	}
	return nil
}
