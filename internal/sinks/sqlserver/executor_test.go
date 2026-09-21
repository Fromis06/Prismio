package sqlserver

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

var testDriverID atomic.Uint64

type executionState struct {
	mu        sync.Mutex
	queries   []string
	args      [][]driver.NamedValue
	begin     int
	commit    int
	rollback  int
	failQuery string
}

type recordingDriver struct{ state *executionState }

func (d *recordingDriver) Open(string) (driver.Conn, error) {
	return &recordingConn{state: d.state}, nil
}

type recordingConn struct{ state *executionState }

func (c *recordingConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("prepare is not supported by the test driver")
}
func (c *recordingConn) Close() error { return nil }
func (c *recordingConn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}
func (c *recordingConn) BeginTx(context.Context, driver.TxOptions) (driver.Tx, error) {
	c.state.mu.Lock()
	c.state.begin++
	c.state.mu.Unlock()
	return &recordingTx{state: c.state}, nil
}
func (c *recordingConn) ExecContext(_ context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	c.state.mu.Lock()
	defer c.state.mu.Unlock()
	c.state.queries = append(c.state.queries, query)
	c.state.args = append(c.state.args, append([]driver.NamedValue(nil), args...))
	if query == c.state.failQuery {
		return nil, errors.New("injected execution error")
	}
	return driver.RowsAffected(1), nil
}

type recordingTx struct{ state *executionState }

func (tx *recordingTx) Commit() error {
	tx.state.mu.Lock()
	tx.state.commit++
	tx.state.mu.Unlock()
	return nil
}
func (tx *recordingTx) Rollback() error {
	tx.state.mu.Lock()
	tx.state.rollback++
	tx.state.mu.Unlock()
	return nil
}

func TestExecutorExecuteBatchCommitsAllStatements(t *testing.T) {
	state := &executionState{}
	executor := &Executor{DB: openRecordingDB(t, state)}
	t.Cleanup(func() { _ = executor.Close() })

	err := executor.ExecuteBatch(
		context.Background(),
		[]string{"UPDATE first", "DELETE second"},
		[][]any{{"value", 1}, {2}},
	)
	if err != nil {
		t.Fatalf("ExecuteBatch returned an error: %v", err)
	}

	state.mu.Lock()
	defer state.mu.Unlock()
	if state.begin != 1 || state.commit != 1 || state.rollback != 0 {
		t.Fatalf("unexpected transaction counts: begin=%d commit=%d rollback=%d", state.begin, state.commit, state.rollback)
	}
	if got := strings.Join(state.queries, ","); got != "UPDATE first,DELETE second" {
		t.Fatalf("unexpected execution order: %s", got)
	}
	if len(state.args) != 2 || len(state.args[0]) != 2 || state.args[0][0].Value != "value" {
		t.Fatalf("arguments were not forwarded: %#v", state.args)
	}
}

func TestExecutorExecuteBatchRollsBackOnFailure(t *testing.T) {
	state := &executionState{failQuery: "broken"}
	executor := &Executor{DB: openRecordingDB(t, state)}
	t.Cleanup(func() { _ = executor.Close() })

	err := executor.ExecuteBatch(
		context.Background(),
		[]string{"works", "broken", "not reached"},
		[][]any{{}, {}, {}},
	)
	if err == nil || !strings.Contains(err.Error(), "statement 2") {
		t.Fatalf("expected a statement-indexed error, got %v", err)
	}

	state.mu.Lock()
	defer state.mu.Unlock()
	if state.begin != 1 || state.commit != 0 || state.rollback != 1 {
		t.Fatalf("unexpected transaction counts: begin=%d commit=%d rollback=%d", state.begin, state.commit, state.rollback)
	}
	if len(state.queries) != 2 {
		t.Fatalf("executor continued after failure: %#v", state.queries)
	}
}

func TestExecutorExecuteBatchValidatesInput(t *testing.T) {
	tests := []struct {
		name     string
		executor *Executor
		queries  []string
		args     [][]any
		want     string
	}{
		{name: "uninitialized", executor: &Executor{}, queries: []string{"SELECT 1"}, args: [][]any{{}}, want: "not initialized"},
		{name: "different lengths", executor: &Executor{DB: openRecordingDB(t, &executionState{})}, queries: []string{"one"}, args: nil, want: "lengths differ"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.executor.DB != nil {
				t.Cleanup(func() { _ = tt.executor.Close() })
			}
			err := tt.executor.ExecuteBatch(context.Background(), tt.queries, tt.args)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("expected error containing %q, got %v", tt.want, err)
			}
		})
	}
}

func openRecordingDB(t *testing.T, state *executionState) *sql.DB {
	t.Helper()
	name := fmt.Sprintf("sqlserver-test-%d", testDriverID.Add(1))
	sql.Register(name, &recordingDriver{state: state})
	db, err := sql.Open(name, "")
	if err != nil {
		t.Fatalf("open test database: %v", err)
	}
	return db
}

var (
	_ driver.Driver        = (*recordingDriver)(nil)
	_ driver.Conn          = (*recordingConn)(nil)
	_ driver.ConnBeginTx   = (*recordingConn)(nil)
	_ driver.ExecerContext = (*recordingConn)(nil)
	_ driver.Tx            = (*recordingTx)(nil)
)
