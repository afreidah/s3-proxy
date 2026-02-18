// -------------------------------------------------------------------------------
// Manager - Multi-Backend Object Storage Manager
//
// Author: Alex Freidah
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
	"sync/atomic"
	"time"

	"github.com/afreidah/s3-proxy/internal/telemetry"
)

// usageCounters holds atomic counters for a single backend's usage deltas.
// Incremented on the hot path (each request) and periodically flushed to the
// database.
type usageCounters struct {
	apiRequests  atomic.Int64
	egressBytes  atomic.Int64
	ingressBytes atomic.Int64
}

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
	backendTimeout time.Duration                 // per-operation timeout for backend S3 calls
	stopCache      chan struct{}                  // signals cache eviction goroutine to stop
	usage          map[string]*usageCounters     // per-backend atomic usage counters
}

// locationCacheEntry holds a cached key-to-backend mapping with TTL.
type locationCacheEntry struct {
	backendName string
	expiry      time.Time
}

// NewBackendManager creates a new backend manager with the given backends and store.
func NewBackendManager(backends map[string]ObjectBackend, store MetadataStore, order []string, cacheTTL, backendTimeout time.Duration) *BackendManager {
	usage := make(map[string]*usageCounters, len(backends))
	for name := range backends {
		usage[name] = &usageCounters{}
	}

	m := &BackendManager{
		backends:       backends,
		store:          store,
		order:          order,
		locationCache:  make(map[string]locationCacheEntry),
		cacheTTL:       cacheTTL,
		backendTimeout: backendTimeout,
		stopCache:      make(chan struct{}),
		usage:          usage,
	}

	// Periodically evict expired cache entries.
	if cacheTTL > 0 {
		go func() {
			ticker := time.NewTicker(cacheTTL)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					m.cacheEvict()
				case <-m.stopCache:
					return
				}
			}
		}()
	}

	return m
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

// cacheEvict removes expired entries from the location cache.
func (m *BackendManager) cacheEvict() {
	m.cacheMu.Lock()
	defer m.cacheMu.Unlock()
	now := time.Now()
	for key, entry := range m.locationCache {
		if now.After(entry.expiry) {
			delete(m.locationCache, key)
		}
	}
}

// ClearCache removes all entries from the location cache.
func (m *BackendManager) ClearCache() {
	m.cacheMu.Lock()
	defer m.cacheMu.Unlock()
	m.locationCache = make(map[string]locationCacheEntry)
}

// Close stops the background cache eviction goroutine.
func (m *BackendManager) Close() {
	close(m.stopCache)
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

// recordUsage increments the in-memory usage counters for a backend.
func (m *BackendManager) recordUsage(backendName string, apiCalls, egress, ingress int64) {
	c, ok := m.usage[backendName]
	if !ok {
		return
	}
	if apiCalls > 0 {
		c.apiRequests.Add(apiCalls)
	}
	if egress > 0 {
		c.egressBytes.Add(egress)
	}
	if ingress > 0 {
		c.ingressBytes.Add(ingress)
	}
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
