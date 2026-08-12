package postgres

import (
	"context"
	"fmt"

	"my-cdc/internal/capture"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
)

func init() {
	capture.RegisterTester("postgres", TestConnection)
}

// TestConnection verifies a Postgres source URL is reachable and has
// replication rights, WITHOUT creating a replication slot or starting
// streaming — unlike Listener.Start, which does both as a real side effect.
// This makes it safe to call repeatedly from the "Check" action row in the
// config UI without leaving slots or streams behind.
func TestConnection(ctx context.Context, sourceURL string) error {
	connConfig, err := pgconn.ParseConfig(sourceURL)
	if err != nil {
		return fmt.Errorf("failed to parse Source URL: %w", err)
	}

	if connConfig.RuntimeParams["slot_name"] == "" {
		return fmt.Errorf("missing slot_name in Source URL")
	}
	if connConfig.RuntimeParams["publication_names"] == "" {
		return fmt.Errorf("missing publication_names in Source URL")
	}
	// slot_name/publication_names are custom runtime params understood by our
	// own listener code, not by Postgres itself — must be stripped before
	// connecting, same as internal/capture/postgres/listener.go does.
	delete(connConfig.RuntimeParams, "slot_name")
	delete(connConfig.RuntimeParams, "publication_names")

	conn, err := pgconn.ConnectConfig(ctx, connConfig)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)

	_, err = pglogrepl.IdentifySystem(ctx, conn)
	return err
}