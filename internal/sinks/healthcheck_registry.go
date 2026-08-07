package sinks

import (
	"context"
	"fmt"
)

// ConnectionTester is the function signature a sink driver implements to
// verify connectivity to its destination without performing any real writes.
// This lives in its own file, separate from registry.go, so no existing
// files need to change when a driver adds health-check support.
type ConnectionTester func(ctx context.Context, url string) error

// testers maps a sink type name (e.g. "postgres") to its ConnectionTester.
var testers = make(map[string]ConnectionTester)

// RegisterTester registers a connectivity-check function for a sink type.
// Called from each driver's own healthcheck.go (e.g.
// internal/sinks/postgres/healthcheck.go), mirroring how Register() in
// registry.go is called from each driver's init.go — same pattern, separate file.
func RegisterTester(sinkType string, fn ConnectionTester) {
	testers[sinkType] = fn
}

// TestConnection runs the registered connectivity check for a sink type.
// Used by the "Check" action row in cmd/cli/config_form.go before Run CDC
// is allowed to proceed.
func TestConnection(ctx context.Context, sinkType string, url string) error {
	fn, ok := testers[sinkType]
	if !ok {
		return fmt.Errorf("chưa có health-check cho loại sink: %s", sinkType)
	}
	return fn(ctx, url)
}