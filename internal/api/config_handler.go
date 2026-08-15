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

// ConfigHandler handles HTTP requests for viewing and updating application
// configuration. The remote HTTP server is currently disabled; review the
// monitor startup path and authentication configuration before re-enabling it.
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

	slog.Info("API: Received configuration update request", "updates", updates)

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
			slog.Warn("API: Skipping unknown configuration key", "key", key)
		}
	}

	response := map[string]string{"status": "success", "message": "Configuration updated"}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

// HashAPIKey hashes an API key using SHA-256 and returns its hexadecimal form.
func HashAPIKey(key string) string {
	hasher := sha256.New()
	hasher.Write([]byte(key))
	return hex.EncodeToString(hasher.Sum(nil))
}

// GenerateNewAPIKey creates a cryptographically secure random API key and its
// SHA-256 hash. It returns the raw key, the stored hash, and any error.
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