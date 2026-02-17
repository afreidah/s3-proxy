// -------------------------------------------------------------------------------
// Manager - Multi-Backend Object Storage Manager
//
// Project: Munchbox / Author: Alex Freidah
//
// Core type and constructor for the backend manager. Object CRUD operations are
// in manager_objects.go, multipart operations in manager_multipart.go, quota
// metrics in manager_metrics.go, rebalancing in rebalancer.go, and replication
// in replicator.go.
// -------------------------------------------------------------------------------

package storage

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"sync"
	"time"

	"github.com/munchbox/s3-proxy/internal/telemetry"
)

// -------------------------------------------------------------------------
// ERRORS
// -------------------------------------------------------------------------

var (
	// ErrInsufficientStorage is returned when no backend has enough quota.
	ErrInsufficientStorage = &S3Error{StatusCode: 507, Code: "InsufficientStorage", Message: "no backend has sufficient quota"}
)

// -------------------------------------------------------------------------
// BACKEND MANAGER
// -------------------------------------------------------------------------

// BackendManager manages multiple storage backends with quota tracking.
type BackendManager struct {
	backends       map[string]ObjectBackend      // name -> backend
	store          MetadataStore                 // metadata persistence (Store or CircuitBreakerStore)
	order          []string                      // backend selection order
	locationCache  map[string]locationCacheEntry // key -> cached backend (for degraded reads)
	cacheMu        sync.RWMutex
	cacheTTL       time.Duration
	backendTimeout time.Duration // per-operation timeout for backend S3 calls
}

// locationCacheEntry holds a cached key-to-backend mapping with TTL.
type locationCacheEntry struct {
	backendName string
	expiry      time.Time
}

// NewBackendManager creates a new backend manager with the given backends and store.
func NewBackendManager(backends map[string]ObjectBackend, store MetadataStore, order []string, cacheTTL, backendTimeout time.Duration) *BackendManager {
	return &BackendManager{
		backends:       backends,
		store:          store,
		order:          order,
		locationCache:  make(map[string]locationCacheEntry),
		cacheTTL:       cacheTTL,
		backendTimeout: backendTimeout,
	}
}

// GetParts returns all parts for a multipart upload. Delegates to the metadata
// store, keeping the store behind the interface.
func (m *BackendManager) GetParts(ctx context.Context, uploadID string) ([]MultipartPart, error) {
	return m.store.GetParts(ctx, uploadID)
}

// cacheGet returns the cached backend for a key, or false if not cached or expired.
func (m *BackendManager) cacheGet(key string) (string, bool) {
	m.cacheMu.RLock()
	defer m.cacheMu.RUnlock()
	entry, ok := m.locationCache[key]
	if !ok || time.Now().After(entry.expiry) {
		return "", false
	}
	return entry.backendName, true
}

// cacheSet stores a key-to-backend mapping with the configured TTL.
func (m *BackendManager) cacheSet(key, backend string) {
	m.cacheMu.Lock()
	defer m.cacheMu.Unlock()
	m.locationCache[key] = locationCacheEntry{
		backendName: backend,
		expiry:      time.Now().Add(m.cacheTTL),
	}
}

// -------------------------------------------------------------------------
// HELPERS
// -------------------------------------------------------------------------

// withTimeout returns a context with the configured backend timeout applied.
// If no timeout is configured, the original context is returned unchanged.
func (m *BackendManager) withTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if m.backendTimeout > 0 {
		return context.WithTimeout(ctx, m.backendTimeout)
	}
	return ctx, func() {}
}

// GenerateUploadID creates a random hex string for multipart upload IDs.
func GenerateUploadID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// recordOperation updates Prometheus metrics for a manager operation.
func (m *BackendManager) recordOperation(operation, backend string, start time.Time, err error) {
	status := "success"
	if err != nil {
		status = "error"
	}

	telemetry.ManagerRequestsTotal.WithLabelValues(operation, backend, status).Inc()
	telemetry.ManagerDuration.WithLabelValues(operation, backend).Observe(time.Since(start).Seconds())
}
