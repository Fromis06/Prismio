package sqlserver

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"

	_ "github.com/microsoft/go-mssqldb"
)

// Executor owns the database/sql connection pool used by the SQL Server sink.
type Executor struct {
	DB *sql.DB
}

// Init opens and verifies a SQL Server connection pool.
func (se *Executor) Init(ctx context.Context, url string) error {
	slog.Info("SINK (SQL Server): Initializing connection pool")

	db, err := sql.Open("sqlserver", url)
	if err != nil {
		return err
	}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return err
	}

	se.DB = db
	slog.Info("SINK (SQL Server): Connection pool is ready")
	return nil
}

// ExecuteBatch applies a complete flush in one transaction. Keeping the batch
// atomic prevents a retry from observing a partially applied flush.
func (se *Executor) ExecuteBatch(ctx context.Context, queries []string, argsList [][]any) error {
	if len(queries) == 0 {
		return nil
	}
	if se.DB == nil {
		return fmt.Errorf("SQL Server connection pool is not initialized")
	}
	if len(queries) != len(argsList) {
		return fmt.Errorf("query and argument batch lengths differ: %d != %d", len(queries), len(argsList))
	}

	tx, err := se.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	for i, query := range queries {
		if _, err := tx.ExecContext(ctx, query, argsList[i]...); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("execute SQL Server statement %d: %w", i+1, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit SQL Server batch: %w", err)
	}
	return nil
}

// Close releases the SQL Server connection pool.
func (se *Executor) Close() error {
	if se.DB == nil {
		return nil
	}
	slog.Info("SINK (SQL Server): Closing connection pool")
	err := se.DB.Close()
	se.DB = nil
	return err
}
