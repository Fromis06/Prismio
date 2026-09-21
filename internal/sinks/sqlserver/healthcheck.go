package sqlserver

import (
	"context"
	"database/sql"

	"my-cdc/internal/sinks"
)

func init() {
	sinks.RegisterTester("sqlserver", TestConnection)
}

// TestConnection performs a lightweight round-trip against SQL Server and
// closes the temporary database handle immediately afterward.
func TestConnection(ctx context.Context, url string) error {
	db, err := sql.Open("sqlserver", url)
	if err != nil {
		return err
	}
	defer db.Close()
	return db.PingContext(ctx)
}
