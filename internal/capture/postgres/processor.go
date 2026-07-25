package postgres

import (
	"log/slog"
	"my-cdc/internal/config"
	"my-cdc/internal/models"
	"my-cdc/internal/sinks"
	"my-cdc/internal/pb"

	"github.com/jackc/pglogrepl"
)

// Processor chịu trách nhiệm phân tích (parse) các gói tin thô từ WAL,
// chuyển đổi chúng thành cấu trúc ChangeEvent chuẩn hóa và gom chúng vào "túi" (bag).
type Processor struct {
	Config     *config.AppConfig
	TargetSink sinks.Pipeline
	Counts     *models.EventsCount

	relations map[uint32]*pglogrepl.RelationMessage // Cache chứa metadata của các bảng (tên cột, kiểu dữ liệu).
	bag       []*pb.ChangeEvent                     // "Túi" tạm thời để gom các sự kiện trước khi gửi đi xử lý.
}

func NewProcessor(cfg *config.AppConfig, targetSink sinks.Pipeline, counts *models.EventsCount) *Processor {
	return &Processor{
		Config:     cfg,
		TargetSink: targetSink,
		Counts:     counts,
		relations:  make(map[uint32]*pglogrepl.RelationMessage),

		// Lấy một "túi" rỗng từ pool để bắt đầu sử dụng.
		bag: models.ChangeEventBagPool.Get().([]*pb.ChangeEvent)[:0],
	}
}

// ProcessRawBytes là hàm chính, nhận dữ liệu WAL thô và LSN hiện tại.
func (p *Processor) ProcessRawBytes(walData []byte, currentLSN pglogrepl.LSN) {
	logicalMsg, err := pglogrepl.Parse(walData)
	if err != nil {
		slog.Warn("Không thể phân tích thông điệp WAL", "error", err)
		return
	}

	switch event := logicalMsg.(type) {
	case *pglogrepl.RelationMessage:
		// Gói tin này chứa thông tin về cấu trúc của một bảng.
		// Ta cần lưu lại để sau này có thể ánh xạ dữ liệu với tên cột tương ứng.
		p.relations[event.RelationID] = event

	case *pglogrepl.BeginMessage:
		// Gói tin báo hiệu bắt đầu một transaction. Ta không cần xử lý gì ở đây.

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
			// Nếu chưa có thông tin về bảng này trong cache, ta không thể xử lý. Bỏ qua.
			slog.Warn("Không tìm thấy thông tin metadata cho bảng, bỏ qua sự kiện", "relation_id", relID)
			return
		}

		var keyNames []string
		for _, col := range rel.Columns {
			if col.Flags == 1 { // 1 là cờ biểu thị Primary Key trong pglogrepl
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

		// Tạo một sự kiện chuẩn hóa (ChangeEvent) từ thông tin đã phân tích.
		changeEvent := models.BuildChangeEvent(
			pb.SourceType_SOURCE_POSTGRES,
			action,
			rel.Namespace,
			rel.RelationName,
			keyNames,
			beforeMap,
			afterMap,
			&pb.Checkpoint{Offset: &pb.Checkpoint_Lsn{Lsn: uint64(currentLSN)}},
		)

		// Cho vào túi
		p.bag = append(p.bag, changeEvent)

		standardSize := p.Config.Bag.BagMaxSize.Load()
		multiplier := p.Config.Bag.BagMaxMultiple.Load()
		maxAllowedLimit := standardSize * int64(multiplier)

		// Đầy túi thì đưa Shipper, rồi đi lấy túi khác
		if int64(len(p.bag)) >= maxAllowedLimit {
			p.TargetSink.WriteBatch(p.bag)
			p.bag = models.ChangeEventBagPool.Get().([]*pb.ChangeEvent)[:0]
		}

	case *pglogrepl.CommitMessage:
		// BẮT BUỘC phải dùng TransactionEndLSN. Postgres sẽ không dời Checkpoint nếu chỉ xác nhận CommitLSN
		commitLSN := uint64(event.TransactionEndLSN)

		if len(p.bag) > 0 {
			// Cập nhật tọa độ của sự kiện cuối cùng thành LSN của lúc Commit
			p.bag[len(p.bag)-1].Offset = &pb.Checkpoint{Offset: &pb.Checkpoint_Lsn{Lsn: commitLSN}}
		} else {
			// Nếu túi rỗng, tạo một sự kiện giả (Dummy Event) chỉ để mang tọa độ Commit LSN đi lưu
			p.bag = append(p.bag, models.BuildChangeEvent(
				pb.SourceType_SOURCE_POSTGRES,
				pb.Action_COMMIT,
				"", "", nil, nil, nil,
				&pb.Checkpoint{Offset: &pb.Checkpoint_Lsn{Lsn: commitLSN}},
			))
		}
		p.TargetSink.WriteBatch(p.bag)
		p.bag = models.ChangeEventBagPool.Get().([]*pb.ChangeEvent)[:0]
	}
}

// ✅ MAPPER FUNCTION: Trái tim của quá trình chuẩn hóa dữ liệu
func (p *Processor) decodeTupleToMap(rel *pglogrepl.RelationMessage, tuple *pglogrepl.TupleData) map[string]any {
	if tuple == nil {
		return nil
	}

	result := make(map[string]any, len(tuple.Columns))

	// Đảm bảo không bị out of bounds nếu cấu trúc bảng bị lệch
	numCols := len(tuple.Columns)
	if len(rel.Columns) < numCols {
		numCols = len(rel.Columns)
	}

	for i := 0; i < numCols; i++ {
		colMeta := rel.Columns[i]
		colData := tuple.Columns[i]
		colName := colMeta.Name

		// Dựa vào cờ (flag) của Postgres để biết dữ liệu là gì
		switch colData.DataType {
		case 'n': // 'n' = Null
			result[colName] = nil

		case 'u': // 'u' = Unchanged (Dữ liệu TOAST không đổi, ví dụ chuỗi quá dài)
			// Thường ở hệ thống chuẩn, ta bỏ qua cột này hoặc gán placeholder
			continue

		case 't', 'b': // 't' = Text format, 'b' = Binary format
			// Mặc định pgoutput plugin gửi dữ liệu dạng Text (byte array chứa text).
			// Ép []byte thành chuỗi (string).
			// NÂNG CẤP SAU: Dựa vào colMeta.DataType (OID) để ép về int, float, bool.
			result[colName] = string(colData.Data)
		}
	}

	return result
}
