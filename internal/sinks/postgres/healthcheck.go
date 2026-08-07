package postgres

import (
	"context"

	"my-cdc/internal/sinks"

	"github.com/jackc/pgx/v5/pgconn"
)

func init() {
	sinks.RegisterTester("postgres", TestConnection)
}

// TestConnection performs a lightweight round-trip check against a Postgres
// destination: connect, run a trivial query, close immediately. It
// intentionally avoids pgxpool.Pool (used by Executor for real writes) so a
// "Check" click never leaves a lingering pooled connection behind.
func TestConnection(ctx context.Context, url string) error {
	conn, err := pgconn.Connect(ctx, url)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)

	result := conn.Exec(ctx, "SELECT 1;")
	_, err = result.ReadAll()
	return err
}