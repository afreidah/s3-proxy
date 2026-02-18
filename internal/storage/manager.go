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

	"github.com/afreidah/s3-orchestrator/internal/telemetry"
)

// usageCounters holds atomic counters for a single backend's usage deltas.
// Incremented on the hot path (each request) and periodically flushed to the
// database.
type usageCounters struct {
	apiRequests  atomic.Int64
	egressBytes  atomic.Int64
	ingressBytes atomic.Int64
}

// UsageLimits holds configurable monthly usage limits for a single backend.
// Zero means unlimited for that dimension.
type UsageLimits struct {
	ApiRequestLimit  int64
	EgressByteLimit  int64
	IngressByteLimit int64
}

// -------------------------------------------------------------------------
// ERRORS
// -------------------------------------------------------------------------

var (
	// ErrInsufficientStorage is returned when no backend has enough quota.
	ErrInsufficientStorage = &S3Error{StatusCode: 507, Code: "InsufficientStorage", Message: "no backend has sufficient quota"}

	// ErrUsageLimitExceeded is returned when all backends holding an object
	// have exceeded their monthly usage limits.
	ErrUsageLimitExceeded = &S3Error{StatusCode: 429, Code: "SlowDown", Message: "monthly usage limit exceeded for all backends holding this object"}
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
	usageLimits    map[string]UsageLimits        // from config, immutable after init
	usageBaseline  map[string]UsageStat          // cached DB totals, refreshed every 30s
	usageBaselineMu sync.RWMutex
}

// locationCacheEntry holds a cached key-to-backend mapping with TTL.
type locationCacheEntry struct {
	backendName string
	expiry      time.Time
}

// NewBackendManager creates a new backend manager with the given backends and store.
// The limits parameter is optional (nil means no usage limits).
func NewBackendManager(backends map[string]ObjectBackend, store MetadataStore, order []string, cacheTTL, backendTimeout time.Duration, limits map[string]UsageLimits) *BackendManager {
	usage := make(map[string]*usageCounters, len(backends))
	for name := range backends {
		usage[name] = &usageCounters{}
	}

	if limits == nil {
		limits = make(map[string]UsageLimits)
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
		usageLimits:    limits,
		usageBaseline:  make(map[string]UsageStat),
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

// withinUsageLimits checks whether the proposed operation would keep the given
// backend within its configured monthly usage limits. It computes:
//
//	effective = baseline (from DB) + unflushed counter + proposed
//
// Returns true if no non-zero limit is exceeded.
func (m *BackendManager) withinUsageLimits(backendName string, apiCalls, egress, ingress int64) bool {
	lim, ok := m.usageLimits[backendName]
	if !ok {
		return true // no limits configured
	}
	if lim.ApiRequestLimit == 0 && lim.EgressByteLimit == 0 && lim.IngressByteLimit == 0 {
		return true // all unlimited
	}

	m.usageBaselineMu.RLock()
	base := m.usageBaseline[backendName]
	m.usageBaselineMu.RUnlock()

	c := m.usage[backendName]
	if c == nil {
		return true
	}

	if lim.ApiRequestLimit > 0 {
		effective := base.ApiRequests + c.apiRequests.Load() + apiCalls
		if effective > lim.ApiRequestLimit {
			return false
		}
	}
	if lim.EgressByteLimit > 0 {
		effective := base.EgressBytes + c.egressBytes.Load() + egress
		if effective > lim.EgressByteLimit {
			return false
		}
	}
	if lim.IngressByteLimit > 0 {
		effective := base.IngressBytes + c.ingressBytes.Load() + ingress
		if effective > lim.IngressByteLimit {
			return false
		}
	}
	return true
}

// backendsWithinLimits returns the subset of m.order whose backends are within
// their monthly usage limits for the proposed operation dimensions.
func (m *BackendManager) backendsWithinLimits(apiCalls, egress, ingress int64) []string {
	eligible := make([]string, 0, len(m.order))
	for _, name := range m.order {
		if m.withinUsageLimits(name, apiCalls, egress, ingress) {
			eligible = append(eligible, name)
		}
	}
	return eligible
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
