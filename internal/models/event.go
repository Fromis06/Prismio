package models

import (
	"encoding/json"
	"my-cdc/internal/pb"
)

// CheckpointFileData holds checkpoint information for JSON serialization to disk.
//
// SourceType is a free-form string (e.g., "postgres", "mysql") that directly matches
// the DBConnection.Type in the configuration. This avoids mapping to a rigid enum,
// allowing new source drivers to be added without modifying this file or event.proto.
type CheckpointFileData struct {
	InstanceName   string         `json:"instance_name"`   // Unique identifier for the source instance.
	SourceType     string         `json:"source_type"`     // The source type name (matches DBConnection.Type).
	CheckpointData *pb.Checkpoint `json:"checkpoint_data"` // Detailed coordinate data.
	UpdatedAt      int64          `json:"updated_at"`      // The last update timestamp.
}

// BuildChangeEvent creates a pb.ChangeEvent object and marshals the raw data maps into byte slices.
//
// The sourceType is a free-form string (e.g., "postgres") passed in by the specific
// Capture driver (see internal/capture/postgres/processor.go), avoiding a rigid enum parsing step.
func BuildChangeEvent(sourceType string, action pb.Action, schema, table string, keyNames []string, before, after map[string]any, offset *pb.Checkpoint) *pb.ChangeEvent {
	var beforeBytes, afterBytes []byte
	if before != nil {
		beforeBytes, _ = json.Marshal(before)
	}
	if after != nil {
		afterBytes, _ = json.Marshal(after)
	}

	return &pb.ChangeEvent{
		SourceType: sourceType,
		Action:     action,
		Schema:     schema,
		Table:      table,
		KeyNames:   keyNames,
		Before:     beforeBytes,
		After:      afterBytes,
		Offset:     offset,
	}
}
