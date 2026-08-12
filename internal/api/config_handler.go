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
// STATUS: NOT called/started anywhere in the app currently — see
// internal/utils/monitor.go, the "DISABLED: remote HTTP server" block. Kept
// as-is (not deleted) because this is reserve infrastructure for the future
// use case of CDC running on a different machine from the admin, which will
// need a way to push configuration changes remotely. This file still compiles
// normally (Go doesn't error on unused functions/types, only on unused
// imports/variables), so it's safe to keep "waiting" without affecting the
// current build or runtime.
//
// When re-enabling it, check the 4 points noted in StartAdaptiveMonitor's
// comment (internal/utils/monitor.go), especially separating pprof from this
// route and re-checking the auth mechanism to match accounts.yaml /
// configs/<username>.yaml.
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

// HashAPIKey hashes an API key string using SHA-256 and returns its hex representation.
//
// NOTE: this function is still ACTIVELY used elsewhere (cmd/cli/run.go — for
// TUI login authentication and creating new accounts), independent of
// whether ConfigHandler is enabled or not.
func HashAPIKey(key string) string {
	hasher := sha256.New()
	hasher.Write([]byte(key))
	return hex.EncodeToString(hasher.Sum(nil))
}

// GenerateNewAPIKey creates a cryptographically secure random API key and its SHA-256 hash.
// It returns the raw key (for the user), the hex-encoded hashed key (for storage), and any error.
//
// NOTE: also used for creating new users in the TUI, independent of
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