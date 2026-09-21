package postgres

import (
	"testing"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgtype"
)

func TestDecodeTuplePreservesBooleanType(t *testing.T) {
	tests := []struct {
		name    string
		oid     uint32
		format  uint8
		data    []byte
		want    any
		present bool
	}{
		{"true", pgtype.BoolOID, 't', []byte("t"), true, true},
		{"false", pgtype.BoolOID, 't', []byte("f"), false, true},
		{"binary true", pgtype.BoolOID, 'b', []byte{1}, true, true},
		{"binary false", pgtype.BoolOID, 'b', []byte{0}, false, true},
		{"null boolean", pgtype.BoolOID, 'n', nil, nil, true},
		{"text t", pgtype.TextOID, 't', []byte("t"), "t", true},
		{"text f", pgtype.TextOID, 't', []byte("f"), "f", true},
		{"unchanged toast", pgtype.TextOID, 'u', nil, nil, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rel := &pglogrepl.RelationMessage{Columns: []*pglogrepl.RelationMessageColumn{{Name: "value", DataType: tt.oid}}}
			tuple := &pglogrepl.TupleData{Columns: []*pglogrepl.TupleDataColumn{{DataType: tt.format, Data: tt.data}}}
			got := (&Processor{}).decodeTupleToMap(rel, tuple)
			value, present := got["value"]
			if present != tt.present || value != tt.want {
				t.Fatalf("got %#v (present=%v), want %#v (present=%v)", value, present, tt.want, tt.present)
			}
		})
	}
}
