package utils

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"my-cdc/internal/config"
	"my-cdc/internal/models"
	"my-cdc/internal/pb"

	"google.golang.org/protobuf/encoding/protojson"
)

// checkpointFileDTO là cấu trúc trung gian để xử lý việc lưu trữ JSON an toàn.
// Sử dụng json.RawMessage để tránh lỗi "oneof" của thư viện encoding/json chuẩn.
type checkpointFileDTO struct {
	InstanceName   string          `json:"instance_name"`
	SourceType     pb.SourceType   `json:"source_type"`
	CheckpointData json.RawMessage `json:"checkpoint_data"`
	UpdatedAt      int64           `json:"updated_at"`
}

// SaveProviderCheckpoint lưu trữ thông tin checkpoint của một provider xuống file.
func SaveProviderCheckpoint(dest config.CheckpointSaveDestination, data models.CheckpointFileData) error {
	folderPath := dest.Path

	// 1. Đảm bảo thư mục lưu trữ tồn tại.
	if err := os.MkdirAll(folderPath, 0755); err != nil {
		return err
	}

	// 2. Tạo tên file dựa trên loại nguồn và tên instance để tránh trùng lặp.
	fileName := fmt.Sprintf("%d_%s_ckpt.json", data.SourceType, data.InstanceName)
	fullPath := filepath.Join(folderPath, fileName)
	tempPath := fullPath + ".tmp"

	// 3. Gán thời gian cập nhật ngay trước khi ghi file.
	data.UpdatedAt = time.Now().Unix()

	// Dùng protojson để mã hóa an toàn cấu trúc Protobuf
	var cpDataRaw json.RawMessage
	if data.CheckpointData != nil {
		b, err := protojson.Marshal(data.CheckpointData)
		if err != nil {
			return fmt.Errorf("lỗi marshal protobuf checkpoint: %w", err)
		}
		cpDataRaw = b
	}

	// Chuyển dữ liệu sang DTO trung gian
	dto := checkpointFileDTO{
		InstanceName:   data.InstanceName,
		SourceType:     data.SourceType,
		CheckpointData: cpDataRaw,
		UpdatedAt:      data.UpdatedAt,
	}

	// 4. Mã hóa dữ liệu thành JSON với định dạng đẹp mắt.
	bytes, err := json.MarshalIndent(dto, "", "  ")
	if err != nil {
		return err
	}

	// [Pattern: Atomic Write] Ghi file tạm rồi đổi tên file để đảm bảo checkpoint không hỏng khi crash.
	if err := os.WriteFile(tempPath, bytes, 0644); err != nil {
		return err
	}
	return os.Rename(tempPath, fullPath)
}

// LoadProviderCheckpoint đọc file checkpoint từ đĩa và giải mã nó thành struct.
func LoadProviderCheckpoint(dest config.CheckpointSaveDestination, sourceType pb.SourceType, instanceName string) (*models.CheckpointFileData, error) {
	folderPath := dest.Path
	fileName := fmt.Sprintf("%d_%s_ckpt.json", sourceType, instanceName)
	fullPath := filepath.Join(folderPath, fileName)

	// 1. Đọc nội dung file.
	bytes, err := os.ReadFile(fullPath)
	if err != nil {
		// Xử lý quan trọng: Nếu file không tồn tại (thường là lần chạy đầu tiên),
		// ta trả về nil một cách bình thường, không coi đây là lỗi.
		if os.IsNotExist(err) {
			return nil, nil // Trả về nil một cách hòa bình, không báo lỗi
		}
		// Nếu là lỗi khác (như không có quyền đọc file) thì mới văng lỗi
		return nil, fmt.Errorf("lỗi đọc file checkpoint: %w", err)
	}

	// 2. Giải mã nội dung JSON vào DTO trung gian.
	var dto checkpointFileDTO
	if err := json.Unmarshal(bytes, &dto); err != nil {
		return nil, fmt.Errorf("file checkpoint bị hỏng định dạng JSON: %w", err)
	}

	// Khôi phục phần dữ liệu Protobuf bằng protojson
	var cpData *pb.Checkpoint
	if len(dto.CheckpointData) > 0 && string(dto.CheckpointData) != "null" {
		cpData = &pb.Checkpoint{}
		// Thêm tùy chọn DiscardUnknown để bỏ qua các trường không xác định trong file JSON cũ.
		// Điều này giúp tăng khả năng tương thích ngược.
		unmarshalOpts := protojson.UnmarshalOptions{
			DiscardUnknown: true,
		}
		if err := unmarshalOpts.Unmarshal(dto.CheckpointData, cpData); err != nil {
			// Nếu vẫn lỗi, file có thể đã bị hỏng. Ghi log và coi như không có checkpoint.
			// Việc trả về (nil, nil) sẽ khiến app hoạt động như lần chạy đầu tiên.
			slog.Warn("Failed to unmarshal checkpoint data, skipping", "file", fullPath, "error", err)
			return nil, nil
		}
	}

	data := models.CheckpointFileData{
		InstanceName:   dto.InstanceName,
		SourceType:     dto.SourceType,
		CheckpointData: cpData,
		UpdatedAt:      dto.UpdatedAt,
	}

	return &data, nil
}
