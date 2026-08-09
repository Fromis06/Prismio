package api

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"

	"my-cdc/internal/config"
)

// ConfigHandler handles HTTP requests for viewing and live-updating application
// configuration.
//
// TRẠNG THÁI: KHÔNG được gọi/khởi động ở đâu trong app hiện tại — xem
// internal/utils/monitor.go, khối "DISABLED: remote HTTP server". Được giữ
// nguyên (không xoá) vì đây là hạ tầng dự phòng cho use-case CDC chạy trên
// máy khác với máy admin, sẽ cần một cách bắn thay đổi cấu hình từ xa. File
// này vẫn compile bình thường (Go không báo lỗi với hàm/type không dùng tới,
// chỉ báo lỗi với import/biến không dùng), nên an toàn để giữ lại "chờ" mà
// không ảnh hưởng build hay runtime hiện tại.
//
// Khi bật lại, xem 4 điểm cần rà trong comment của StartAdaptiveMonitor
// (internal/utils/monitor.go), đặc biệt là việc tách pprof khỏi route này và
// rà lại cơ chế auth cho khớp với accounts.yaml / configs/<username>.yaml.
type ConfigHandler struct {
	AppConfig *config.AppConfig
}

// NewConfigHandler creates a new handler with a reference to the AppConfig.
func NewConfigHandler(cfg *config.AppConfig) *ConfigHandler {
	return &ConfigHandler{
		AppConfig: cfg,
	}
}

// ServeHTTP is the main entry point for handling HTTP requests.
func (h *ConfigHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Authenticate the request first.
	if !h.authenticate(w, r) {
		return // Error is handled within the authenticate method.
	}

	// Process the request if authentication was successful.
	switch r.Method {
	case http.MethodGet:
		h.handleGet(w, r)
	case http.MethodPost:
		h.handlePost(w, r)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// authenticate checks for a valid API key in the request header.
func (h *ConfigHandler) authenticate(w http.ResponseWriter, r *http.Request) bool {
	// If no API keys are configured, skip authentication.
	// This is intended for development or testing environments only.
	if len(h.AppConfig.Monitor.HashedAPIKeys) == 0 {
		return true
	}

	apiKey := r.Header.Get("X-API-Key")
	if apiKey == "" {
		http.Error(w, "Unauthorized: X-API-Key header missing", http.StatusUnauthorized)
		return false
	}

	// Hash the incoming API key and check for its existence in the valid keys map.
	hashedIncomingKey := HashAPIKey(apiKey)
	if _, ok := h.AppConfig.Monitor.HashedAPIKeys[hashedIncomingKey]; !ok {
		http.Error(w, "Unauthorized: Invalid API Key", http.StatusUnauthorized)
		return false
	}
	return true
}

// handleGet returns the current values of the tunable configuration parameters.
func (h *ConfigHandler) handleGet(w http.ResponseWriter, r *http.Request) {
	currentConfig := map[string]any{
		"data_processing_worker_count": h.AppConfig.DataProcessing.DataProcessingWorkerCount.Load(),
		"batch_max_size":               h.AppConfig.Batch.BatchMaxSize.Load(),
		"batch_timeout_ms":             h.AppConfig.Batch.BatchTimeout.Load(),
		"bag_max_size":                 h.AppConfig.Bag.BagMaxSize.Load(),
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(currentConfig)
}

// handlePost receives and applies new configuration values.
func (h *ConfigHandler) handlePost(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Cannot read body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	var updates map[string]int64
	if err := json.Unmarshal(body, &updates); err != nil {
		http.Error(w, "Invalid JSON format", http.StatusBadRequest)
		return
	}

	slog.Info("API: Nhận được yêu cầu cập nhật cấu hình", "updates", updates)

	// Atomically update the configuration values.
	for key, value := range updates {
		switch key {
		case "data_processing_worker_count":
			h.AppConfig.DataProcessing.DataProcessingWorkerCount.Store(int32(value))
		case "batch_max_size":
			h.AppConfig.Batch.BatchMaxSize.Store(value)
		case "batch_timeout_ms":
			h.AppConfig.Batch.BatchTimeout.Store(value)
		case "bag_max_size":
			h.AppConfig.Bag.BagMaxSize.Store(value)
		default:
			slog.Warn("API: Bỏ qua khóa cấu hình không xác định", "key", key)
		}
	}

	response := map[string]string{"status": "success", "message": "Configuration updated"}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

// HashAPIKey hashes an API key string using SHA-256 and returns its hex representation.
//
// LƯU Ý: hàm này vẫn ĐANG được dùng ở nơi khác (cmd/cli/run.go — xác thực
// login TUI và tạo tài khoản mới), không phụ thuộc vào việc ConfigHandler có
// được bật hay không.
func HashAPIKey(key string) string {
	hasher := sha256.New()
	hasher.Write([]byte(key))
	return hex.EncodeToString(hasher.Sum(nil))
}

// GenerateNewAPIKey creates a cryptographically secure random API key and its SHA-256 hash.
// It returns the raw key (for the user), the hex-encoded hashed key (for storage), and any error.
//
// LƯU Ý: cũng đang được dùng cho việc tạo user mới trong TUI, độc lập với
// ConfigHandler.
func GenerateNewAPIKey() (rawKey string, hashedKey string, err error) {
	randomBytes := make([]byte, 32)
	if _, err := rand.Read(randomBytes); err != nil {
		return "", "", err
	}

	// Encode the random bytes into a URL-safe base64 string.
	rawKey = base64.URLEncoding.EncodeToString(randomBytes)

	// Hash the raw key for storage.
	hashedKey = HashAPIKey(rawKey)

	return rawKey, hashedKey, nil
}