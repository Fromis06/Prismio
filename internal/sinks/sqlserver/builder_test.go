package sqlserver

import (
	"encoding/json"
	"reflect"
	"testing"

	"my-cdc/internal/pb"
	"my-cdc/internal/sinks"
)

func TestBuilderBuildQuery(t *testing.T) {
	tests := []struct {
		name      string
		event     *pb.ChangeEvent
		wantQuery string
		wantArgs  []any
	}{
		{
			name: "insert uses idempotent merge",
			event: event(pb.Action_INSERT, "users", []string{"id"}, nil, map[string]any{
				"id": 7, "name": "Lan", "note": nil,
			}),
			wantQuery: "MERGE INTO [users] WITH (HOLDLOCK) AS target USING (VALUES (@p1, @p2, @p3)) AS source ([id], [name], [note]) ON target.[id] = source.[id] WHEN MATCHED THEN UPDATE SET [name] = source.[name], [note] = source.[note] WHEN NOT MATCHED THEN INSERT ([id], [name], [note]) VALUES (source.[id], source.[name], source.[note]);",
			wantArgs:  []any{float64(7), "Lan", nil},
		},
		{
			name: "insert containing only keys remains replay safe",
			event: event(pb.Action_INSERT, "memberships", []string{"team_id", "user_id"}, nil, map[string]any{
				"user_id": 4, "team_id": 2,
			}),
			wantQuery: "MERGE INTO [memberships] WITH (HOLDLOCK) AS target USING (VALUES (@p1, @p2)) AS source ([team_id], [user_id]) ON target.[team_id] = source.[team_id] AND target.[user_id] = source.[user_id] WHEN NOT MATCHED THEN INSERT ([team_id], [user_id]) VALUES (source.[team_id], source.[user_id]);",
			wantArgs:  []any{float64(2), float64(4)},
		},
		{
			name: "insert without a key is a regular insert",
			event: event(pb.Action_INSERT, "audit", nil, nil, map[string]any{
				"message": "created",
			}),
			wantQuery: "INSERT INTO [audit] ([message]) VALUES (@p1);",
			wantArgs:  []any{"created"},
		},
		{
			name: "update uses the old composite key",
			event: event(
				pb.Action_UPDATE,
				"memberships",
				[]string{"team_id", "user_id"},
				map[string]any{"team_id": 1, "user_id": 3},
				map[string]any{"role": "owner", "team_id": 2, "user_id": 3},
			),
			wantQuery: "UPDATE [memberships] SET [role] = @p1, [team_id] = @p2, [user_id] = @p3 WHERE [team_id] = @p4 AND [user_id] = @p5;",
			wantArgs:  []any{"owner", float64(2), float64(3), float64(1), float64(3)},
		},
		{
			name: "delete quotes identifiers",
			event: event(
				pb.Action_DELETE,
				"order]items",
				[]string{"select"},
				map[string]any{"select": "A-1"},
				nil,
			),
			wantQuery: "DELETE FROM [order]]items] WHERE [select] = @p1;",
			wantArgs:  []any{"A-1"},
		},
		{
			name:      "commit is ignored",
			event:     &pb.ChangeEvent{Action: pb.Action_COMMIT},
			wantQuery: "",
			wantArgs:  nil,
		},
		{
			name: "missing insert key is ignored safely",
			event: event(pb.Action_INSERT, "users", []string{"id"}, nil, map[string]any{
				"name": "Lan",
			}),
			wantQuery: "",
			wantArgs:  nil,
		},
		{
			name: "missing delete key is ignored safely",
			event: event(pb.Action_DELETE, "users", []string{"id"}, map[string]any{
				"name": "Lan",
			}, nil),
			wantQuery: "",
			wantArgs:  nil,
		},
	}

	builder := &Builder{}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query, args := builder.BuildQuery(tt.event)
			if query != tt.wantQuery {
				t.Fatalf("query mismatch\n got: %s\nwant: %s", query, tt.wantQuery)
			}
			if !reflect.DeepEqual(args, tt.wantArgs) {
				t.Fatalf("args mismatch\n got: %#v\nwant: %#v", args, tt.wantArgs)
			}
		})
	}
}

func TestSQLServerSinkIsRegistered(t *testing.T) {
	registered := false
	for _, sink := range sinks.ListRegistered() {
		if sink.Type == "sqlserver" {
			registered = sink.Metadata.DisplayName == "SQL Server" && sink.Metadata.URLTemplate != ""
			break
		}
	}
	if !registered {
		t.Fatal("SQL Server sink metadata was not registered")
	}
}

func event(action pb.Action, table string, keyNames []string, before, after map[string]any) *pb.ChangeEvent {
	return &pb.ChangeEvent{
		Action:   action,
		Table:    table,
		KeyNames: keyNames,
		Before:   marshal(before),
		After:    marshal(after),
	}
}

func marshal(value map[string]any) []byte {
	if value == nil {
		return nil
	}
	data, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return data
}
