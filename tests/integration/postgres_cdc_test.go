//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"my-cdc/internal/app"
	"my-cdc/internal/config"
	_ "my-cdc/internal/drivers"
	"my-cdc/internal/utils"
)

const (
	defaultSourceURL       = "postgres://postgres:password@localhost:15432/sourcedb?sslmode=disable"
	defaultTargetURL       = "postgres://postgres:password@localhost:15433/targetdb?sslmode=disable"
	defaultLaggedTargetURL = "postgres://postgres:password@localhost:15434/targetdb?sslmode=disable"
	defaultToxiproxyAPIURL = "http://localhost:8474"
	testTable              = "prismio_integrity_records"
	testPublication        = "prismio_integrity_pub"
	testSlot               = "prismio_integrity_slot"
	testTimeout            = 12 * time.Second
	crashBurstSize         = 40
)

type integrityRecord struct {
	ID         int64
	Name       string
	Email      string
	EmailValid bool
	Quantity   int32
	Active     bool
	Note       string
}

type runningCDC struct {
	application    *app.Application
	cancel         context.CancelFunc
	listenerResult chan error
	stopped        bool
}

// TestPostgresCDCDataIntegrity checks the final replicated state rather than
// merely checking whether Prismio stayed alive. It catches missing, duplicate,
// incorrectly updated, and incorrectly deleted rows.
func TestPostgresCDCDataIntegrity(t *testing.T) {
	ctx := context.Background()

	sourceURL := envOrDefault("TEST_SOURCE_DB_URL", defaultSourceURL)
	targetURL := envOrDefault("TEST_TARGET_DB_URL", defaultTargetURL)

	sourceDB := openPool(t, ctx, sourceURL, "source")
	defer sourceDB.Close()
	targetDB := openPool(t, ctx, targetURL, "target")
	defer targetDB.Close()

	prepareDatabases(t, ctx, sourceDB, targetDB)

	cfg := testConfig(t, sourceURL, targetURL, t.TempDir())
	cdc := startCDC(t, cfg)
	defer cdc.stop(t, true)

	waitForSlotActive(t, ctx, sourceDB, cdc.listenerResult)

	initial := []integrityRecord{
		{
			ID:         1,
			Name:       "Nguyễn Ánh 🌈",
			Email:      "anh@example.com",
			EmailValid: true,
			Quantity:   7,
			Active:     true,
			Note:       "dòng 1\ndòng 2 — dữ liệu Unicode",
		},
		{
			ID:         2,
			Name:       "Unchanged row",
			EmailValid: false,
			Quantity:   0,
			Active:     false,
			Note:       "must remain unchanged",
		},
	}

	insertRecords(t, ctx, sourceDB, initial)
	waitForExactTargetState(t, ctx, targetDB, cdc.listenerResult, initial, "INSERT")

	updated := slices.Clone(initial)
	updated[0] = integrityRecord{
		ID:         1,
		Name:       "Đã cập nhật ✅",
		EmailValid: false,
		Quantity:   -3,
		Active:     false,
		Note:       `quotes: 'single' and "double"`,
	}
	if _, err := sourceDB.Exec(ctx,
		`UPDATE prismio_integrity_records
		 SET name = $1, email = NULL, quantity = $2, active = $3, note = $4
		 WHERE id = $5`,
		updated[0].Name, updated[0].Quantity, updated[0].Active, updated[0].Note, updated[0].ID,
	); err != nil {
		t.Fatalf("update source row: %v", err)
	}
	waitForExactTargetState(t, ctx, targetDB, cdc.listenerResult, updated, "UPDATE")

	if _, err := sourceDB.Exec(ctx, `DELETE FROM prismio_integrity_records WHERE id = $1`, int64(1)); err != nil {
		t.Fatalf("delete source row: %v", err)
	}
	waitForExactTargetState(t, ctx, targetDB, cdc.listenerResult, updated[1:], "DELETE")
}

// TestPostgresCDCRecoversAfterInFlightCrash slows the target connection, waits
// until only part of a burst has arrived, and then stops Prismio without calling
// Application.Shutdown. The next instance must use the periodic checkpoint and
// retained WAL to converge on the exact source state without duplicates.
func TestPostgresCDCRecoversAfterInFlightCrash(t *testing.T) {
	ctx := context.Background()
	sourceURL := envOrDefault("TEST_SOURCE_DB_URL", defaultSourceURL)
	targetURL := envOrDefault("TEST_TARGET_DB_URL", defaultTargetURL)
	laggedTargetURL := envOrDefault("TEST_LAGGED_TARGET_DB_URL", defaultLaggedTargetURL)
	toxiproxyAPIURL := envOrDefault("TEST_TOXIPROXY_API_URL", defaultToxiproxyAPIURL)

	sourceDB := openPool(t, ctx, sourceURL, "source")
	defer sourceDB.Close()
	targetDB := openPool(t, ctx, targetURL, "target")
	defer targetDB.Close()
	prepareDatabases(t, ctx, sourceDB, targetDB)
	configureTargetLatency(t, ctx, toxiproxyAPIURL, 250*time.Millisecond)

	checkpointDir := t.TempDir()
	firstConfig := testConfig(t, sourceURL, laggedTargetURL, checkpointDir)
	firstConfig.Batch.BatchMaxSize.Store(2)
	firstConfig.Batch.BatchTimeout.Store(20)
	firstConfig.Retry.MaxRetries = 0
	firstRun := startCDC(t, firstConfig)
	defer firstRun.stop(t, false)
	waitForSlotActive(t, ctx, sourceDB, firstRun.listenerResult)

	beforeCrash := []integrityRecord{
		{
			ID:         10,
			Name:       "persisted before crash",
			Email:      "before@example.com",
			EmailValid: true,
			Quantity:   10,
			Active:     true,
			Note:       "this row must not be duplicated after replay",
		},
	}
	insertRecords(t, ctx, sourceDB, beforeCrash)
	waitForExactTargetState(t, ctx, targetDB, firstRun.listenerResult, beforeCrash, "PRE-CRASH INSERT")

	persistedLSN := waitForPersistedCheckpoint(t, firstConfig, 1)
	t.Logf("periodic checkpoint persisted at LSN %d", persistedLSN)

	burst := make([]integrityRecord, 0, crashBurstSize)
	for i := 0; i < crashBurstSize; i++ {
		record := integrityRecord{
			ID:         int64(1_000 + i),
			Name:       fmt.Sprintf("in-flight row %02d 🚧", i),
			EmailValid: i%2 == 0,
			Quantity:   int32(i - crashBurstSize/2),
			Active:     i%3 == 0,
			Note:       fmt.Sprintf("must survive crash and replay: %02d", i),
		}
		if record.EmailValid {
			record.Email = fmt.Sprintf("row-%02d@example.com", i)
		}
		burst = append(burst, record)
	}
	insertRecords(t, ctx, sourceDB, burst)

	expectedAfterRestart := append(slices.Clone(beforeCrash), burst...)
	partialCount := waitForPartialTransfer(
		t,
		ctx,
		targetDB,
		firstRun,
		int64(len(beforeCrash)),
		int64(len(expectedAfterRestart)),
	)
	t.Logf(
		"simulating crash while target has only %d/%d rows",
		partialCount,
		len(expectedAfterRestart),
	)

	// Simulate a crash: cancel in-flight work and release resources, but skip
	// Application.Shutdown so there is no final checkpoint save.
	firstRun.stop(t, false)

	secondConfig := testConfig(t, sourceURL, targetURL, checkpointDir)
	secondRun := startCDC(t, secondConfig)
	defer secondRun.stop(t, true)
	waitForSlotActive(t, ctx, sourceDB, secondRun.listenerResult)
	waitForExactTargetState(
		t,
		ctx,
		targetDB,
		secondRun.listenerResult,
		expectedAfterRestart,
		"IN-FLIGHT CRASH RECOVERY",
	)
}

func startCDC(t *testing.T, cfg *config.AppConfig) *runningCDC {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	cdc, err := app.Bootstrap(ctx, cfg)
	if err != nil {
		cancel()
		t.Fatalf("bootstrap Prismio: %v", err)
	}
	if err := cdc.MultiSink.Start(); err != nil {
		cancel()
		_ = cdc.MultiSink.Stop()
		t.Fatalf("start target pipeline: %v", err)
	}

	run := &runningCDC{
		application:    cdc,
		cancel:         cancel,
		listenerResult: make(chan error, 1),
	}
	go func() {
		run.listenerResult <- cdc.Listener.Start(ctx, cfg.Provider.Source.URL, cdc.GlobalState)
	}()
	return run
}

func (run *runningCDC) stop(t *testing.T, saveFinalCheckpoint bool) {
	t.Helper()
	if run == nil || run.stopped {
		return
	}
	run.stopped = true
	run.cancel()

	select {
	case err := <-run.listenerResult:
		if err != nil {
			t.Errorf("stop source listener: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Error("source listener did not stop within 3 seconds")
	}
	if err := run.application.MultiSink.Stop(); err != nil {
		t.Errorf("stop target pipeline: %v", err)
	}
	if saveFinalCheckpoint {
		run.application.Shutdown()
	}
}

func insertRecords(t *testing.T, ctx context.Context, sourceDB *pgxpool.Pool, records []integrityRecord) {
	t.Helper()
	tx, err := sourceDB.Begin(ctx)
	if err != nil {
		t.Fatalf("begin source insert transaction: %v", err)
	}
	for _, record := range records {
		var email any
		if record.EmailValid {
			email = record.Email
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO prismio_integrity_records (id, name, email, quantity, active, note)
			 VALUES ($1, $2, $3, $4, $5, $6)`,
			record.ID, record.Name, email, record.Quantity, record.Active, record.Note,
		); err != nil {
			_ = tx.Rollback(ctx)
			t.Fatalf("insert source row %d: %v", record.ID, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit source inserts: %v", err)
	}
}

func envOrDefault(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func configureTargetLatency(t *testing.T, ctx context.Context, apiURL string, latency time.Duration) {
	t.Helper()
	configureDatabaseLatency(t, ctx, apiURL, "prismio_target", "0.0.0.0:8666", "postgres-target:5432", latency)
}

func configureDatabaseLatency(t *testing.T, ctx context.Context, apiURL, name, listen, upstream string, latency time.Duration) {
	t.Helper()
	client := &http.Client{Timeout: 2 * time.Second}
	baseURL := strings.TrimRight(apiURL, "/")
	deadline := time.Now().Add(testTimeout)

	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/version", nil)
		if err != nil {
			t.Fatalf("create Toxiproxy readiness request: %v", err)
		}
		resp, err := client.Do(req)
		if err == nil && resp.StatusCode >= 200 && resp.StatusCode < 300 {
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			break
		}
		if resp != nil {
			_ = resp.Body.Close()
		}
		if time.Now().After(deadline) {
			t.Fatalf("Toxiproxy API at %s was not ready within %s: %v", baseURL, testTimeout, err)
		}
		time.Sleep(100 * time.Millisecond)
	}

	proxyURL := baseURL + "/proxies/" + name
	toxiproxyRequest(t, ctx, client, http.MethodDelete, proxyURL, nil, true)
	toxiproxyRequest(t, ctx, client, http.MethodPost, baseURL+"/proxies", map[string]any{
		"name":     name,
		"listen":   listen,
		"upstream": upstream,
		"enabled":  true,
	}, false)
	toxiproxyRequest(t, ctx, client, http.MethodPost, proxyURL+"/toxics", map[string]any{
		"name":     "target_response_latency",
		"type":     "latency",
		"stream":   "downstream",
		"toxicity": 1.0,
		"attributes": map[string]any{
			"latency": latency.Milliseconds(),
			"jitter":  0,
		},
	}, false)
	t.Logf("target latency proxy configured with %s downstream delay", latency)
}

func toxiproxyRequest(
	t *testing.T,
	ctx context.Context,
	client *http.Client,
	method string,
	requestURL string,
	body any,
	allowNotFound bool,
) {
	t.Helper()
	var payload []byte
	var err error
	if body != nil {
		payload, err = json.Marshal(body)
		if err != nil {
			t.Fatalf("encode Toxiproxy request: %v", err)
		}
	}

	req, err := http.NewRequestWithContext(ctx, method, requestURL, bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("create Toxiproxy request: %v", err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("call Toxiproxy API %s %s: %v", method, requestURL, err)
	}
	defer resp.Body.Close()
	responseBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return
	}
	if allowNotFound && resp.StatusCode == http.StatusNotFound {
		return
	}
	t.Fatalf(
		"Toxiproxy API %s %s returned %s: %s",
		method, requestURL, resp.Status, strings.TrimSpace(string(responseBody)),
	)
}

func waitForPartialTransfer(
	t *testing.T,
	ctx context.Context,
	targetDB *pgxpool.Pool,
	run *runningCDC,
	baselineCount int64,
	totalExpected int64,
) int64 {
	t.Helper()
	deadline := time.Now().Add(testTimeout)
	var lastCount int64
	var lastCaptured int64
	var lastErr error

	for time.Now().Before(deadline) {
		failIfListenerStopped(t, run.listenerResult)
		lastCaptured = run.application.EventsCount.InsertCount.Load()
		lastErr = targetDB.QueryRow(ctx, `SELECT COUNT(*) FROM prismio_integrity_records`).Scan(&lastCount)
		if lastErr == nil &&
			lastCaptured >= totalExpected &&
			lastCount > baselineCount &&
			lastCount < totalExpected {
			return lastCount
		}
		time.Sleep(25 * time.Millisecond)
	}

	t.Fatalf(
		"could not observe an in-flight partial transfer within %s (captured: %d, target: %d/%d, last error: %v)",
		testTimeout, lastCaptured, lastCount, totalExpected, lastErr,
	)
	return 0
}

func openPool(t *testing.T, ctx context.Context, connectionURL, label string) *pgxpool.Pool {
	t.Helper()
	pool, err := pgxpool.New(ctx, connectionURL)
	if err != nil {
		t.Fatalf("parse %s database URL: %v", label, err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Fatalf("connect to %s database: %v", label, err)
	}
	return pool
}

func prepareDatabases(t *testing.T, ctx context.Context, sourceDB, targetDB *pgxpool.Pool) {
	t.Helper()
	prepareSourceDatabase(t, ctx, sourceDB)
	if _, err := targetDB.Exec(ctx, `
		DROP TABLE IF EXISTS prismio_integrity_records;
		CREATE TABLE prismio_integrity_records (
			id BIGINT PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT NULL,
			quantity INTEGER NOT NULL,
			active BOOLEAN NOT NULL,
			note TEXT NOT NULL
		);
	`); err != nil {
		t.Fatalf("prepare target database: %v", err)
	}
}

func prepareSourceDatabase(t *testing.T, ctx context.Context, sourceDB *pgxpool.Pool) {
	t.Helper()

	if _, err := sourceDB.Exec(ctx, `
		DO $$
		BEGIN
			IF EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = 'prismio_integrity_slot' AND NOT active) THEN
				PERFORM pg_drop_replication_slot('prismio_integrity_slot');
			END IF;
		END
		$$;
		DROP PUBLICATION IF EXISTS prismio_integrity_pub;
		DROP TABLE IF EXISTS prismio_integrity_records;
		CREATE TABLE prismio_integrity_records (
			id BIGINT PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT NULL,
			quantity INTEGER NOT NULL,
			active BOOLEAN NOT NULL,
			note TEXT NOT NULL
		);
		CREATE PUBLICATION prismio_integrity_pub FOR TABLE prismio_integrity_records;
	`); err != nil {
		t.Fatalf("prepare source database: %v", err)
	}
}

func testConfig(t *testing.T, sourceURL, targetURL, checkpointDir string) *config.AppConfig {
	t.Helper()

	parsedSourceURL, err := url.Parse(sourceURL)
	if err != nil {
		t.Fatalf("parse source database URL: %v", err)
	}
	query := parsedSourceURL.Query()
	query.Set("slot_name", testSlot)
	query.Set("publication_names", testPublication)
	parsedSourceURL.RawQuery = query.Encode()

	cfg := config.NewDefaultConfig()
	cfg.Provider.Source = config.DBConnection{
		Name:     "integration-source",
		Type:     "postgres",
		URL:      parsedSourceURL.String(),
		IsActive: true,
	}
	cfg.Consumers.List = []config.DBConnection{
		{
			Name:     "integration-target",
			Type:     "postgres",
			URL:      targetURL,
			IsActive: true,
		},
	}
	cfg.SaveDestination.Path = checkpointDir
	cfg.Capture.FeedbackInterval.Store(1)
	cfg.Pipeline.PipelineMaxSize.Store(100)
	cfg.Bag.BagMaxSize.Store(100)
	cfg.Bag.BagMaxMultiple.Store(2)
	cfg.DataProcessing.DataProcessingWorkerCount.Store(2)
	cfg.Batch.BatchMaxSize.Store(100)
	cfg.Batch.BatchTimeout.Store(50)
	cfg.Batch.FlushTimeoutMs.Store(5_000)
	cfg.Retry.MaxRetries = 5
	cfg.Retry.BaseDelayMs = 100
	cfg.Retry.MaxDelayTimeMs = 500

	return cfg
}

func waitForPersistedCheckpoint(t *testing.T, cfg *config.AppConfig, minimumLSN uint64) uint64 {
	t.Helper()
	if minimumLSN == 0 {
		t.Fatal("target pipeline did not produce a non-zero checkpoint")
	}

	deadline := time.Now().Add(testTimeout)
	var lastLSN uint64
	var lastErr error
	for time.Now().Before(deadline) {
		checkpoint, err := utils.LoadProviderCheckpoint(
			cfg.SaveDestination,
			cfg.Provider.Source.Type,
			cfg.Provider.Source.Name,
		)
		lastErr = err
		if err == nil && checkpoint != nil && checkpoint.CheckpointData != nil {
			lastLSN = checkpoint.CheckpointData.GetLsn()
			if lastLSN >= minimumLSN {
				return lastLSN
			}
		}
		time.Sleep(100 * time.Millisecond)
	}

	t.Fatalf(
		"periodic checkpoint did not reach LSN %d within %s (last LSN: %d, last error: %v)",
		minimumLSN, testTimeout, lastLSN, lastErr,
	)
	return 0
}

func waitForSlotActive(t *testing.T, ctx context.Context, sourceDB *pgxpool.Pool, listenerResult chan error) {
	t.Helper()
	deadline := time.Now().Add(testTimeout)
	var lastErr error

	for time.Now().Before(deadline) {
		failIfListenerStopped(t, listenerResult)

		var active bool
		lastErr = sourceDB.QueryRow(ctx,
			`SELECT COALESCE((SELECT active FROM pg_replication_slots WHERE slot_name = $1), false)`,
			testSlot,
		).Scan(&active)
		if lastErr == nil && active {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}

	t.Fatalf("replication slot %q was not active within %s (last error: %v)", testSlot, testTimeout, lastErr)
}

func waitForExactTargetState(
	t *testing.T,
	ctx context.Context,
	targetDB *pgxpool.Pool,
	listenerResult chan error,
	want []integrityRecord,
	operation string,
) {
	t.Helper()
	deadline := time.Now().Add(testTimeout)
	var got []integrityRecord
	var lastErr error

	for time.Now().Before(deadline) {
		failIfListenerStopped(t, listenerResult)
		got, lastErr = readTargetState(ctx, targetDB)
		if lastErr == nil && slices.Equal(got, want) {
			t.Logf("%s replicated safely (%d exact rows)", operation, len(got))
			return
		}
		time.Sleep(100 * time.Millisecond)
	}

	t.Fatalf(
		"%s did not reach the expected target state within %s\nlast error: %v\ngot:  %+v\nwant: %+v",
		operation, testTimeout, lastErr, got, want,
	)
}

func readTargetState(ctx context.Context, targetDB *pgxpool.Pool) ([]integrityRecord, error) {
	rows, err := targetDB.Query(ctx, `
		SELECT id, name, COALESCE(email, ''), email IS NOT NULL, quantity, active, note
		FROM prismio_integrity_records
		ORDER BY id
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	records := make([]integrityRecord, 0)
	for rows.Next() {
		var record integrityRecord
		if err := rows.Scan(
			&record.ID,
			&record.Name,
			&record.Email,
			&record.EmailValid,
			&record.Quantity,
			&record.Active,
			&record.Note,
		); err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return records, nil
}

func failIfListenerStopped(t *testing.T, listenerResult chan error) {
	t.Helper()
	select {
	case err := <-listenerResult:
		// Put the result back so deferred cleanup can observe that the listener
		// has already stopped instead of waiting for a second result.
		listenerResult <- err
		if err != nil {
			t.Fatalf("source listener stopped unexpectedly: %v", err)
		}
		t.Fatal("source listener stopped unexpectedly without an error")
	default:
	}
}
