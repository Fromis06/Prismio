package postgres

import (
	"log/slog"
	"my-cdc/internal/config"
	"my-cdc/internal/models"
	"my-cdc/internal/pb"
	"my-cdc/internal/sinks"

	"github.com/jackc/pglogrepl"
)

// sourceTypeName matches this driver's config and registry name.
const sourceTypeName = "postgres"

// Processor turns WAL packets into event bags.
type Processor struct {
	Config     *config.AppConfig
	TargetSink sinks.Pipeline
	Counts     *models.EventsCount

	relations map[uint32]*pglogrepl.RelationMessage // Table metadata by relation id.
	bag       []*pb.ChangeEvent                     // Events waiting to be sent.
}

func NewProcessor(cfg *config.AppConfig, targetSink sinks.Pipeline, counts *models.EventsCount) *Processor {
	return &Processor{
		Config:     cfg,
		TargetSink: targetSink,
		Counts:     counts,
		relations:  make(map[uint32]*pglogrepl.RelationMessage),

		// Reuse a pooled event slice.
		bag: models.ChangeEventBagPool.Get().([]*pb.ChangeEvent)[:0],
	}
}

// ProcessRawBytes handles one WAL packet.
func (p *Processor) ProcessRawBytes(walData []byte, currentLSN pglogrepl.LSN) {
	logicalMsg, err := pglogrepl.Parse(walData)
	if err != nil {
		slog.Warn("Could not parse WAL message", "error", err)
		return
	}

	switch event := logicalMsg.(type) {
	case *pglogrepl.RelationMessage:
		// Keep table metadata for later rows.
		p.relations[event.RelationID] = event

	case *pglogrepl.BeginMessage:
		// Begin has no event data.

	case *pglogrepl.InsertMessage, *pglogrepl.UpdateMessage, *pglogrepl.DeleteMessage:
		var action pb.Action
		var relID uint32
		var oldTuple, newTuple *pglogrepl.TupleData

		switch e := event.(type) {
		case *pglogrepl.InsertMessage:
			action, relID, newTuple = pb.Action_INSERT, e.RelationID, e.Tuple
			p.Counts.InsertCount.Add(1)
		case *pglogrepl.UpdateMessage:
			action, relID = pb.Action_UPDATE, e.RelationID
			oldTuple, newTuple = e.OldTuple, e.NewTuple
			p.Counts.UpdateCount.Add(1)
		case *pglogrepl.DeleteMessage:
			action, relID, oldTuple = pb.Action_DELETE, e.RelationID, e.OldTuple
			p.Counts.DeleteCount.Add(1)
		}

		rel, ok := p.relations[relID]
		if !ok {
			// Can't map columns without metadata.
			slog.Warn("Metadata not found for relation, skipping event", "relation_id", relID)
			return
		}

		var keyNames []string
		for _, col := range rel.Columns {
			if col.Flags == 1 { // 1 is the flag indicating a Primary Key in pglogrepl
				keyNames = append(keyNames, col.Name)
			}
		}

		var beforeMap, afterMap map[string]any
		if oldTuple != nil {
			beforeMap = p.decodeTupleToMap(rel, oldTuple)
		}
		if newTuple != nil {
			afterMap = p.decodeTupleToMap(rel, newTuple)
		}

		// Build the shared event shape.
		changeEvent := models.BuildChangeEvent(
			sourceTypeName,
			action,
			rel.Namespace,
			rel.RelationName,
			keyNames,
			beforeMap,
			afterMap,
			&pb.Checkpoint{Offset: &pb.Checkpoint_Lsn{Lsn: uint64(currentLSN)}},
		)

		p.bag = append(p.bag, changeEvent)

		standardSize := p.Config.Bag.BagMaxSize.Load()
		multiplier := p.Config.Bag.BagMaxMultiple.Load()
		maxAllowedLimit := standardSize * int64(multiplier)

		// Send a full bag and reuse another slice.
		if int64(len(p.bag)) >= maxAllowedLimit {
			p.TargetSink.WriteBatch(p.bag)
			p.bag = models.ChangeEventBagPool.Get().([]*pb.ChangeEvent)[:0]
		}

	case *pglogrepl.CommitMessage:
		// Use TransactionEndLSN or Postgres won't move the checkpoint.
		commitLSN := uint64(event.TransactionEndLSN)

		if len(p.bag) > 0 {
			// Commit belongs on the last event.
			p.bag[len(p.bag)-1].Offset = &pb.Checkpoint{Offset: &pb.Checkpoint_Lsn{Lsn: commitLSN}}
		} else {
			// Keep a commit-only checkpoint too.
			p.bag = append(p.bag, models.BuildChangeEvent(
				sourceTypeName,
				pb.Action_COMMIT,
				"", "", nil, nil, nil,
				&pb.Checkpoint{Offset: &pb.Checkpoint_Lsn{Lsn: commitLSN}},
			))
		}
		p.TargetSink.WriteBatch(p.bag)
		p.bag = models.ChangeEventBagPool.Get().([]*pb.ChangeEvent)[:0]
	}
}

// decodeTupleToMap maps tuple values to column names.
func (p *Processor) decodeTupleToMap(rel *pglogrepl.RelationMessage, tuple *pglogrepl.TupleData) map[string]any {
	if tuple == nil {
		return nil
	}

	result := make(map[string]any, len(tuple.Columns))

	// Be safe if tuple and relation are out of sync.
	numCols := len(tuple.Columns)
	if len(rel.Columns) < numCols {
		numCols = len(rel.Columns)
	}

	for i := 0; i < numCols; i++ {
		colMeta := rel.Columns[i]
		colData := tuple.Columns[i]
		colName := colMeta.Name

		switch colData.DataType {
		case 'n': // 'n' = Null
			result[colName] = nil

		case 'u': // Unchanged TOAST value.
			continue

		case 't', 'b': // Text or binary payload.
			// pgoutput is text by default; type casting can come later.
			result[colName] = string(colData.Data)
		}
	}

	return result
}