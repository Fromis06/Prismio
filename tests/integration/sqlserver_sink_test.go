//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"testing"
	"time"

	"my-cdc/internal/models"
	"my-cdc/internal/pb"
	"my-cdc/internal/sinks"
	"my-cdc/internal/sinks/sqlserver"
)

// These credentials belong only to the disposable Compose test service.
const defaultSQLServerURL = "sqlserver://sa:Prismio_Test_2026!@localhost:11433?database=tempdb&encrypt=disable"
const defaultLaggedSQLServerURL = "sqlserver://sa:Prismio_Test_2026!@localhost:11434?database=tempdb&encrypt=disable"

// Exercise the actual pgoutput -> registered pipeline -> SQL Server path,
// including PostgreSQL's text encodings and updates without an old tuple.
func TestSQLServerCDCDataIntegrity(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	sourceURL := envOrDefault("TEST_SOURCE_DB_URL", defaultSourceURL)
	targetURL := envOrDefault("TEST_SQLSERVER_DB_URL", defaultSQLServerURL)
	source := openPool(t, ctx, sourceURL, "source")
	defer source.Close()
	target := openSQLServer(t, ctx, targetURL)
	prepareSourceDatabase(t, ctx, source)
	prepareSQLServerRecords(t, ctx, target)
	if err := sinks.TestConnection(ctx, "sqlserver", targetURL); err != nil {
		t.Fatalf("registered SQL Server health check: %v", err)
	}
	cfg := testConfig(t, sourceURL, targetURL, t.TempDir())
	cfg.Consumers.List[0].Type = "sqlserver"
	cfg.Retry.MaxRetries = 0
	run := startCDC(t, cfg)
	defer run.stop(t, true)
	waitForSlotActive(t, ctx, source, run.listenerResult)

	want := []integrityRecord{
		{ID: 1, Name: "Nguyễn Ánh 🌈", Email: "anh@example.com", EmailValid: true, Quantity: 7, Active: true, Note: "dòng 1\ndòng 2 — Unicode"},
		{ID: 9007199254740993, Name: "Unchanged row", Quantity: 0, Active: false, Note: "exact bigint above float64 precision"},
	}
	insertRecords(t, ctx, source, want)
	waitForSQLServerState(t, ctx, target, run, want, "INSERT")

	want[0] = integrityRecord{ID: 1, Name: "Đã cập nhật ✅", Quantity: -3, Active: false, Note: `quotes: 'single' and "double"`}
	if _, err := source.Exec(ctx, `UPDATE prismio_integrity_records SET name=$1, email=NULL, quantity=$2, active=$3, note=$4 WHERE id=$5`,
		want[0].Name, want[0].Quantity, want[0].Active, want[0].Note, want[0].ID); err != nil {
		t.Fatal(err)
	}
	waitForSQLServerState(t, ctx, target, run, want, "UPDATE without old tuple")

	if _, err := source.Exec(ctx, `UPDATE prismio_integrity_records SET id=3 WHERE id=1`); err != nil {
		t.Fatal(err)
	}
	want[0].ID = 3
	waitForSQLServerState(t, ctx, target, run, want, "PRIMARY KEY UPDATE")
	if _, err := source.Exec(ctx, `DELETE FROM prismio_integrity_records WHERE id=3`); err != nil {
		t.Fatal(err)
	}
	waitForSQLServerState(t, ctx, target, run, want[1:], "DELETE")
}

func TestSQLServerCDCRecoversAfterInFlightCrash(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	sourceURL := envOrDefault("TEST_SOURCE_DB_URL", defaultSourceURL)
	targetURL := envOrDefault("TEST_SQLSERVER_DB_URL", defaultSQLServerURL)
	laggedURL := envOrDefault("TEST_LAGGED_SQLSERVER_DB_URL", defaultLaggedSQLServerURL)
	source := openPool(t, ctx, sourceURL, "source")
	defer source.Close()
	target := openSQLServer(t, ctx, targetURL)
	prepareSourceDatabase(t, ctx, source)
	prepareSQLServerRecords(t, ctx, target)
	configureDatabaseLatency(t, ctx, envOrDefault("TEST_TOXIPROXY_API_URL", defaultToxiproxyAPIURL),
		"prismio_sqlserver", "0.0.0.0:8667", "sqlserver-target:1433", 100*time.Millisecond)

	checkpointDir := t.TempDir()
	cfg := testConfig(t, sourceURL, laggedURL, checkpointDir)
	cfg.Consumers.List[0].Type = "sqlserver"
	cfg.Batch.BatchMaxSize.Store(2)
	cfg.Retry.MaxRetries = 0
	first := startCDC(t, cfg)
	defer first.stop(t, false)
	waitForSlotActive(t, ctx, source, first.listenerResult)
	want := []integrityRecord{{ID: 10, Name: "before crash", Active: true, Note: "must not duplicate"}}
	insertRecords(t, ctx, source, want)
	waitForSQLServerState(t, ctx, target, first, want, "PRE-CRASH INSERT")
	waitForPersistedCheckpoint(t, cfg, 1)

	burst := make([]integrityRecord, crashBurstSize)
	for i := range burst {
		burst[i] = integrityRecord{ID: int64(1000 + i), Name: fmt.Sprintf("in-flight %02d 🚧", i),
			Quantity: int32(i - 20), Active: i%2 == 0, Note: "must survive replay"}
	}
	insertRecords(t, ctx, source, burst)
	want = append(want, burst...)
	deadline := time.Now().Add(testTimeout)
	observedPartial := false
	for time.Now().Before(deadline) {
		failIfListenerStopped(t, first.listenerResult)
		var count int
		if err := target.QueryRowContext(ctx, `SELECT COUNT(*) FROM prismio_integrity_records`).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count > 1 && count < len(want) && first.application.EventsCount.InsertCount.Load() >= int64(len(want)) {
			t.Logf("crashing with only %d/%d rows on SQL Server", count, len(want))
			observedPartial = true
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if !observedPartial {
		t.Fatal("did not observe a partially applied transfer before crash")
	}
	first.stop(t, false)
	restartCfg := testConfig(t, sourceURL, targetURL, checkpointDir)
	restartCfg.Consumers.List[0].Type = "sqlserver"
	second := startCDC(t, restartCfg)
	defer second.stop(t, true)
	waitForSlotActive(t, ctx, source, second.listenerResult)
	waitForSQLServerState(t, ctx, target, second, want, "CRASH RECOVERY")
}

// Direct sink tests cover replay and transaction guarantees against a real
// server, including SQL syntax that cannot be validated by a mock driver.
func TestSQLServerSinkReplayAndRollback(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	targetURL := envOrDefault("TEST_SQLSERVER_DB_URL", defaultSQLServerURL)
	db := openSQLServer(t, ctx, targetURL)
	if _, err := db.ExecContext(ctx, `
		DROP TABLE IF EXISTS [prismio_sink_rows];
		DROP TABLE IF EXISTS [prismio_sink_keys];
		CREATE TABLE [prismio_sink_rows] (
			[tenant] INT NOT NULL, [key] BIGINT NOT NULL,
			[select] NVARCHAR(200) NULL, [close]]bracket] NVARCHAR(200) NULL,
			PRIMARY KEY ([tenant], [key])
		);
		CREATE TABLE [prismio_sink_keys] ([id] INT PRIMARY KEY);
	`); err != nil {
		t.Fatal(err)
	}
	executor := &sqlserver.Executor{}
	if err := executor.Init(ctx, targetURL); err != nil {
		t.Fatal(err)
	}
	defer executor.Close()
	builder := &sqlserver.Builder{}
	build := func(action pb.Action, table string, keys []string, before, after map[string]any) (string, []any) {
		query, args := builder.BuildQuery(models.BuildChangeEvent("postgres", action, "public", table, keys, before, after, nil))
		if query == "" {
			t.Fatal("builder unexpectedly skipped event")
		}
		return query, args
	}
	keys := []string{"tenant", "key"}
	row := map[string]any{"tenant": "1", "key": "9007199254740993", "select": "Unicode 🌈", "close]bracket": nil}
	q, a := build(pb.Action_INSERT, "prismio_sink_rows", keys, nil, row)
	if err := executor.ExecuteBatch(ctx, []string{q, q}, [][]any{a, a}); err != nil {
		t.Fatalf("replay composite-key insert: %v", err)
	}
	row["select"] = "updated by replay"
	q, a = build(pb.Action_INSERT, "prismio_sink_rows", keys, nil, row)
	if err := executor.ExecuteBatch(ctx, []string{q}, [][]any{a}); err != nil {
		t.Fatal(err)
	}
	var count int
	var key int64
	var value string
	var nullable sql.NullString
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM prismio_sink_rows`).Scan(&count); err != nil || count != 1 {
		t.Fatalf("replay produced %d rows, error: %v", count, err)
	}
	if err := db.QueryRowContext(ctx, `SELECT [key], [select], [close]]bracket] FROM prismio_sink_rows`).Scan(&key, &value, &nullable); err != nil {
		t.Fatal(err)
	}
	if key != 9007199254740993 || value != "updated by replay" || nullable.Valid {
		t.Fatalf("unexpected row: key=%d value=%q nullable=%+v", key, value, nullable)
	}

	q, a = build(pb.Action_INSERT, "prismio_sink_keys", []string{"id"}, nil, map[string]any{"id": "1"})
	if err := executor.ExecuteBatch(ctx, []string{q, q}, [][]any{a, a}); err != nil {
		t.Fatalf("replay key-only insert: %v", err)
	}
	q, a = build(pb.Action_INSERT, "prismio_sink_keys", []string{"id"}, nil, map[string]any{"id": "2"})
	bad := "INSERT INTO prismio_sink_keys (id) VALUES (@p1);"
	if err := executor.ExecuteBatch(ctx, []string{q, bad}, [][]any{a, {1}}); err == nil {
		t.Fatal("expected duplicate-key error in second statement")
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM prismio_sink_keys WHERE id=2`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("failed batch leaked its first insert: count=%d error=%v", count, err)
	}
	if err := executor.ExecuteBatch(ctx, []string{q}, [][]any{a}); err != nil {
		t.Fatalf("valid retry after rollback: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM prismio_sink_keys`).Scan(&count); err != nil || count != 2 {
		t.Fatalf("retry did not persist exactly two rows: count=%d error=%v", count, err)
	}
}

func openSQLServer(t *testing.T, ctx context.Context, connectionURL string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlserver", connectionURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("connect to SQL Server test database: %v", err)
	}
	return db
}

func prepareSQLServerRecords(t *testing.T, ctx context.Context, db *sql.DB) {
	t.Helper()
	if _, err := db.ExecContext(ctx, `
		DROP TABLE IF EXISTS prismio_integrity_records;
		CREATE TABLE prismio_integrity_records (
			id BIGINT PRIMARY KEY, name NVARCHAR(200) NOT NULL,
			email NVARCHAR(200) NULL, quantity INT NOT NULL,
			active BIT NOT NULL, note NVARCHAR(MAX) NOT NULL
		);
	`); err != nil {
		t.Fatalf("prepare SQL Server target: %v", err)
	}
}

func waitForSQLServerState(t *testing.T, ctx context.Context, db *sql.DB, run *runningCDC, want []integrityRecord, operation string) {
	t.Helper()
	deadline := time.Now().Add(testTimeout)
	var got []integrityRecord
	var lastErr error
	for time.Now().Before(deadline) {
		failIfListenerStopped(t, run.listenerResult)
		got, lastErr = readSQLServerState(ctx, db)
		if lastErr == nil && slices.Equal(got, want) {
			t.Logf("%s replicated safely (%d exact SQL Server rows)", operation, len(got))
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("%s: SQL Server did not converge\nerror: %v\ngot: %+v\nwant: %+v", operation, lastErr, got, want)
}

func readSQLServerState(ctx context.Context, db *sql.DB) ([]integrityRecord, error) {
	rows, err := db.QueryContext(ctx, `SELECT id, name, email, quantity, active, note FROM prismio_integrity_records ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var records []integrityRecord
	for rows.Next() {
		var r integrityRecord
		var email sql.NullString
		if err := rows.Scan(&r.ID, &r.Name, &email, &r.Quantity, &r.Active, &r.Note); err != nil {
			return nil, err
		}
		r.Email, r.EmailValid = email.String, email.Valid
		records = append(records, r)
	}
	return records, rows.Err()
}
