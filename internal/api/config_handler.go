package utils

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"encoding/hex"

	"my-cdc/internal/config"
)

// ConfigHandler xử lý các yêu cầu HTTP để xem và cập nhật cấu hình "nóng".
type ConfigHandler struct {
	AppConfig *config.AppConfig
}

// NewConfigHandler tạo một handler mới với tham chiếu đến AppConfig.
func NewConfigHandler(cfg *config.AppConfig) *ConfigHandler {
	return &ConfigHandler{
		AppConfig: cfg,
	}
}

// ServeHTTP là phương thức chính xử lý request.
func (h *ConfigHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Kiểm tra API Key
	if !h.authenticate(w, r) {
		return // Lỗi đã được xử lý trong authenticate
	}

	// Xử lý request nếu đã xác thực thành công
	switch r.Method {
	case http.MethodGet:
		h.handleGet(w, r)
	case http.MethodPost:
		h.handlePost(w, r)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// authenticate kiểm tra API Key từ header.
func (h *ConfigHandler) authenticate(w http.ResponseWriter, r *http.Request) bool {
	// Nếu không có API Key được cấu hình, bỏ qua xác thực (chỉ dùng cho dev/test)
	if h.AppConfig.Monitor.HashedAPIKey == "" {
		return true
	}

	apiKey := r.Header.Get("X-API-Key")
	if apiKey == "" {
		http.Error(w, "Unauthorized: X-API-Key header missing", http.StatusUnauthorized)
		return false
	}

	// Băm API Key nhận được và so sánh bằng hàm hằng số thời gian
	hashedIncomingKey := hashAPIKey(apiKey)
	if subtle.ConstantTimeCompare([]byte(hashedIncomingKey), []byte(h.AppConfig.Monitor.HashedAPIKey)) == 0 {
		http.Error(w, "Unauthorized: Invalid API Key", http.StatusUnauthorized)
		return false
	}
	return true
}

// handleGet trả về các giá trị cấu hình có thể điều chỉnh hiện tại.
func (h *ConfigHandler) handleGet(w http.ResponseWriter, r *http.Request) {
	// Lấy các giá trị hiện tại từ các biến atomic
	currentConfig := map[string]any{
		"data_processing_worker_count": h.AppConfig.DataProcessing.DataProcessingWorkerCount.Load(),
		"batch_max_size":               h.AppConfig.Batch.BatchMaxSize.Load(),
		"batch_timeout_ms":             h.AppConfig.Batch.BatchTimeout.Load(),
		"bag_max_size":                 h.AppConfig.Bag.BagMaxSize.Load(),
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(currentConfig)
}

// handlePost nhận và áp dụng các giá trị cấu hình mới.
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

	// Cập nhật các giá trị cấu hình một cách an toàn
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

// hashAPIKey băm chuỗi API Key bằng SHA-256 và trả về dạng hex string.
func hashAPIKey(key string) string {
	hasher := sha256.New()
	hasher.Write([]byte(key))
	return hex.EncodeToString(hasher.Sum(nil))
}
