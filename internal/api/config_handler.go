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
	// Nếu không có API Key nào được cấu hình, bỏ qua xác thực (chỉ dùng cho dev/test)
	if len(h.AppConfig.Monitor.HashedAPIKeys) == 0 {
		return true
	}

	apiKey := r.Header.Get("X-API-Key")
	if apiKey == "" {
		http.Error(w, "Unauthorized: X-API-Key header missing", http.StatusUnauthorized)
		return false
	}

	// Băm API Key nhận được và so sánh bằng hàm hằng số thời gian
	hashedIncomingKey := HashAPIKey(apiKey)
	// Kiểm tra xem hash có tồn tại trong map các key hợp lệ không
	if _, ok := h.AppConfig.Monitor.HashedAPIKeys[hashedIncomingKey]; !ok {
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

// HashAPIKey băm chuỗi API Key bằng SHA-256 và trả về dạng hex string.
func HashAPIKey(key string) string {
	hasher := sha256.New()
	hasher.Write([]byte(key))
	return hex.EncodeToString(hasher.Sum(nil))
}

// GenerateNewAPIKey tạo một API key ngẫu nhiên an toàn và hash SHA-256 của nó.
func GenerateNewAPIKey() (rawKey string, hashedKey string, err error) {
	// Tạo 32 byte ngẫu nhiên cho key
	randomBytes := make([]byte, 32)
	if _, err := rand.Read(randomBytes); err != nil {
		return "", "", err
	}

	// Mã hóa các byte ngẫu nhiên thành chuỗi base64 an toàn cho URL
	rawKey = base64.URLEncoding.EncodeToString(randomBytes)

	// Băm key gốc
	hashedKey = HashAPIKey(rawKey)

	return rawKey, hashedKey, nil
}
