package capture

import (
	"context"
	"fmt"
)

// ConnectionTester is the function signature a capture (source) driver
// implements to verify connectivity without creating a replication slot or
// starting a real stream. Kept in its own file, separate from registry.go.
type ConnectionTester func(ctx context.Context, url string) error

var testers = make(map[string]ConnectionTester)

// RegisterTester registers a connectivity-check function for a source type.
func RegisterTester(sourceType string, fn ConnectionTester) {
	testers[sourceType] = fn
}

// TestConnection runs the registered connectivity check for a source type.
func TestConnection(ctx context.Context, sourceType string, url string) error {
	fn, ok := testers[sourceType]
	if !ok {
		return fmt.Errorf("no health-check registered for source type: %s", sourceType)
	}
	return fn(ctx, url)
}