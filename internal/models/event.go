package models

import (
	"encoding/json"
	"my-cdc/internal/pb"
)

// CheckpointFileData chứa thông tin mốc checkpoint để mã hóa JSON lưu xuống đĩa.
//
// SourceType là chuỗi tự do (VD: "postgres", "mysql"...), khớp trực tiếp với
// DBConnection.Type trong config — không còn map qua enum pb.SourceType nữa.
// Nhờ vậy khi thêm driver nguồn mới, không cần sửa file này hay event.proto.
type CheckpointFileData struct {
	InstanceName   string         `json:"instance_name"`   // Tên định danh của instance nguồn.
	SourceType     string         `json:"source_type"`     // Tên loại nguồn (khớp DBConnection.Type).
	CheckpointData *pb.Checkpoint `json:"checkpoint_data"` // Dữ liệu tọa độ chi tiết.
	UpdatedAt      int64          `json:"updated_at"`      // Dấu thời gian cập nhật cuối cùng.
}

// BuildChangeEvent tạo ra đối tượng pb.ChangeEvent và marshal map dữ liệu thô sang mảng byte.
//
// sourceType là chuỗi tự do (VD: "postgres"), do từng driver Capture tự truyền vào
// (xem internal/capture/postgres/processor.go) — không còn đi qua bước parse enum.
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