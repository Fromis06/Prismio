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

// checkpointFileDTO is an intermediate structure for safe JSON storage.
// It uses json.RawMessage to avoid the "oneof" error of the standard encoding/json library.
type checkpointFileDTO struct {
	InstanceName   string          `json:"instance_name"`
	SourceType     pb.SourceType   `json:"source_type"`
	CheckpointData json.RawMessage `json:"checkpoint_data"`
	UpdatedAt      int64           `json:"updated_at"`
}

// SaveProviderCheckpoint saves a provider's checkpoint information to a file.
func SaveProviderCheckpoint(dest config.CheckpointSaveDestination, data models.CheckpointFileData) error {
	folderPath := dest.Path

	// 1. Ensure the storage directory exists.
	if err := os.MkdirAll(folderPath, 0755); err != nil {
		return err
	}

	// 2. Create a filename based on the source type and instance name to avoid duplicates.
	fileName := fmt.Sprintf("%d_%s_ckpt.json", data.SourceType, data.InstanceName)
	fullPath := filepath.Join(folderPath, fileName)
	tempPath := fullPath + ".tmp"

	// 3. Set the update time just before writing the file.
	data.UpdatedAt = time.Now().Unix()

	// Use protojson for safe encoding of the Protobuf structure
	var cpDataRaw json.RawMessage
	if data.CheckpointData != nil {
		b, err := protojson.Marshal(data.CheckpointData)
		if err != nil {
			return fmt.Errorf("error marshalling protobuf checkpoint: %w", err)
		}
		cpDataRaw = b
	}

	// Convert data to the intermediate DTO
	dto := checkpointFileDTO{
		InstanceName:   data.InstanceName,
		SourceType:     data.SourceType,
		CheckpointData: cpDataRaw,
		UpdatedAt:      data.UpdatedAt,
	}

	// 4. Marshal the data to JSON with pretty formatting.
	bytes, err := json.MarshalIndent(dto, "", "  ")
	if err != nil {
		return err
	}

	// Write to a temporary file first, then rename.
	// This ensures the checkpoint file is never in a corrupted state if the process crashes midway.
	if err := os.WriteFile(tempPath, bytes, 0644); err != nil {
		return err
	}
	return os.Rename(tempPath, fullPath)
}

// LoadProviderCheckpoint reads a checkpoint file from disk and decodes it into a struct.
func LoadProviderCheckpoint(dest config.CheckpointSaveDestination, sourceType pb.SourceType, instanceName string) (*models.CheckpointFileData, error) {
	folderPath := dest.Path
	fileName := fmt.Sprintf("%d_%s_ckpt.json", sourceType, instanceName)
	fullPath := filepath.Join(folderPath, fileName)

	// 1. Read the file content.
	bytes, err := os.ReadFile(fullPath)
	if err != nil {
		if os.IsNotExist(err) {
			// File does not exist (usually on the first run), return nil, not considered an error.
			return nil, nil
		}
		// Other errors (e.g., no read permission) are still considered errors.
		return nil, fmt.Errorf("error reading checkpoint file: %w", err)
	}

	// 2. Unmarshal the JSON content into the intermediate DTO.
	var dto checkpointFileDTO
	if err := json.Unmarshal(bytes, &dto); err != nil {
		return nil, fmt.Errorf("checkpoint file is corrupted (invalid JSON format): %w", err)
	}

	// Restore the Protobuf data part using protojson
	var cpData *pb.Checkpoint
	if len(dto.CheckpointData) > 0 && string(dto.CheckpointData) != "null" {
		cpData = &pb.Checkpoint{}
		// Ignore unknown fields in old JSON files to improve backward compatibility.
		unmarshalOpts := protojson.UnmarshalOptions{
			DiscardUnknown: true,
		}
		if err := unmarshalOpts.Unmarshal(dto.CheckpointData, cpData); err != nil {
			// If decoding fails, the file might be corrupted. Skip this checkpoint and operate as if it's the first run.
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
