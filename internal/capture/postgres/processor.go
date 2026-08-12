package postgres

import (
	"log/slog"
	"my-cdc/internal/config"
	"my-cdc/internal/models"
	"my-cdc/internal/pb"
	"my-cdc/internal/sinks"

	"github.com/jackc/pglogrepl"
)

// sourceTypeName is this driver's identifier name, matching DBConnection.Type
// in the config ("postgres") and the name registered via capture.Register() in init.go.
// A new source driver (e.g., internal/capture/mysql) will declare its own similar
// constant in its own file, without needing to modify anything here.
const sourceTypeName = "postgres"

// Processor is responsible for parsing raw WAL packets,
// converting them into a standardized ChangeEvent structure, and grouping them into a "bag".
type Processor struct {
	Config     *config.AppConfig
	TargetSink sinks.Pipeline
	Counts     *models.EventsCount

	relations map[uint32]*pglogrepl.RelationMessage // Cache containing table metadata (column names, data types).
	bag       []*pb.ChangeEvent                     // Temporary "bag" to collect events before sending for processing.
}

func NewProcessor(cfg *config.AppConfig, targetSink sinks.Pipeline, counts *models.EventsCount) *Processor {
	return &Processor{
		Config:     cfg,
		TargetSink: targetSink,
		Counts:     counts,
		relations:  make(map[uint32]*pglogrepl.RelationMessage),

		// Initialize bag with an empty slice from the pool.
		bag: models.ChangeEventBagPool.Get().([]*pb.ChangeEvent)[:0],
	}
}

// ProcessRawBytes is the main function that receives raw WAL data and the current LSN.
func (p *Processor) ProcessRawBytes(walData []byte, currentLSN pglogrepl.LSN) {
	logicalMsg, err := pglogrepl.Parse(walData)
	if err != nil {
		slog.Warn("Could not parse WAL message", "error", err)
		return
	}

	switch event := logicalMsg.(type) {
	case *pglogrepl.RelationMessage:
		// This packet contains information about the structure of a table.
		// We need to save it to later map data with the corresponding column names.
		p.relations[event.RelationID] = event

	case *pglogrepl.BeginMessage:
		// Ignore BeginMessage, no processing needed.

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
			// If there is no information about this table in the cache, skip the event.
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

		// Create a standardized event (ChangeEvent).
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

		// If the bag is full, send it for processing and get a new bag from the pool.
		if int64(len(p.bag)) >= maxAllowedLimit {
			p.TargetSink.WriteBatch(p.bag)
			p.bag = models.ChangeEventBagPool.Get().([]*pb.ChangeEvent)[:0]
		}

	case *pglogrepl.CommitMessage:
		// Must use TransactionEndLSN. Postgres will not advance the Checkpoint if only CommitLSN is confirmed.
		commitLSN := uint64(event.TransactionEndLSN)

		if len(p.bag) > 0 {
			// Update the checkpoint of the last event in the bag with the commit's LSN.
			p.bag[len(p.bag)-1].Offset = &pb.Checkpoint{Offset: &pb.Checkpoint_Lsn{Lsn: commitLSN}}
		} else {
			// If the bag is empty, create a dummy event just to carry the checkpoint information.
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

// decodeTupleToMap converts raw data from pglogrepl.TupleData into a map[string]any.
func (p *Processor) decodeTupleToMap(rel *pglogrepl.RelationMessage, tuple *pglogrepl.TupleData) map[string]any {
	if tuple == nil {
		return nil
	}

	result := make(map[string]any, len(tuple.Columns))

	// Ensure no out-of-bounds error if the table structure is skewed.
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

		case 'u': // 'u' = Unchanged (Unchanged TOASTed data, e.g., oversized strings)
			// Skip unchanged columns (usually TOASTed data).
			continue

		case 't', 'b': // 't' = Text format, 'b' = Binary format
			// pgoutput sends data in text format by default.
			// TODO: Based on colMeta.DataType (OID), perform accurate type casting (int, float, bool, etc.).
			result[colName] = string(colData.Data)
		}
	}

	return result
}