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
	"crypto/rand"
	"encoding/hex"
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
	backends map[string]ObjectBackend // name -> backend
	store    *Store                   // PostgreSQL store for quota/location
	order    []string                 // backend selection order
}

// NewBackendManager creates a new backend manager with the given backends and store.
func NewBackendManager(backends map[string]ObjectBackend, store *Store, order []string) *BackendManager {
	return &BackendManager{
		backends: backends,
		store:    store,
		order:    order,
	}
}

// Store returns the underlying store for direct access when needed (e.g.,
// listing parts for multipart uploads from the server layer).
func (m *BackendManager) Store() *Store {
	return m.store
}

// -------------------------------------------------------------------------
// HELPERS
// -------------------------------------------------------------------------

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
