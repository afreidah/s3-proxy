// -------------------------------------------------------------------------------
// CircuitBreakerStore - Self-Healing Database Degradation Wrapper
//
// Author: Alex Freidah
//
// Wraps a MetadataStore with a three-state circuit breaker that detects database
// outages and returns ErrDBUnavailable when the circuit is open. The manager
// uses this sentinel to trigger broadcast read fallback or reject writes with
// 503. When the database recovers, the circuit auto-closes.
//
// States: closed (healthy) → open (DB down) → half-open (probing) → closed.
// -------------------------------------------------------------------------------

package storage

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/afreidah/s3-proxy/internal/config"
	"github.com/afreidah/s3-proxy/internal/telemetry"
)

// -------------------------------------------------------------------------
// STATE
// -------------------------------------------------------------------------

type circuitState int

const (
	stateClosed   circuitState = iota // healthy — all calls pass through
	stateOpen                         // DB down — return ErrDBUnavailable
	stateHalfOpen                     // probing — one call allowed through
)

func (s circuitState) String() string {
	switch s {
	case stateClosed:
		return "closed"
	case stateOpen:
		return "open"
	case stateHalfOpen:
		return "half-open"
	default:
		return "unknown"
	}
}

// -------------------------------------------------------------------------
// CIRCUIT BREAKER STORE
// -------------------------------------------------------------------------

// CircuitBreakerStore implements MetadataStore by wrapping a real store with
// circuit breaker logic. When the database is unreachable, it returns
// ErrDBUnavailable instead of passing through to the real store.
type CircuitBreakerStore struct {
	real          MetadataStore
	mu            sync.Mutex
	state         circuitState
	failures      int
	lastFailure   time.Time
	failThreshold int
	openTimeout   time.Duration
}

// Compile-time check.
var _ MetadataStore = (*CircuitBreakerStore)(nil)

// NewCircuitBreakerStore wraps a real MetadataStore with circuit breaker logic.
func NewCircuitBreakerStore(real MetadataStore, cfg config.CircuitBreakerConfig) *CircuitBreakerStore {
	return &CircuitBreakerStore{
		real:          real,
		state:         stateClosed,
		failThreshold: cfg.FailureThreshold,
		openTimeout:   cfg.OpenTimeout,
	}
}

// IsHealthy returns true when the circuit is closed (database is reachable).
func (cb *CircuitBreakerStore) IsHealthy() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.state == stateClosed
}

// -------------------------------------------------------------------------
// STATE MACHINE
// -------------------------------------------------------------------------

// preCheck returns ErrDBUnavailable when the circuit is open. Transitions
// open → half-open when the timeout has elapsed, allowing one probe request.
func (cb *CircuitBreakerStore) preCheck() error {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case stateClosed:
		return nil
	case stateOpen:
		if time.Since(cb.lastFailure) >= cb.openTimeout {
			cb.transition(stateHalfOpen)
			return nil // allow this request as the probe
		}
		return ErrDBUnavailable
	case stateHalfOpen:
		// Only one probe at a time — reject concurrent requests during probe.
		return ErrDBUnavailable
	}
	return nil
}

// postCheck records the result of a real store call and transitions state.
// When a DB error causes the circuit to open (or reopen), the original error
// is replaced with ErrDBUnavailable so the manager always sees the canonical
// sentinel for "database down".
func (cb *CircuitBreakerStore) postCheck(err error) error {
	if !isDBError(err) {
		cb.onSuccess()
		return err
	}
	cb.onFailure()
	if !cb.IsHealthy() {
		return ErrDBUnavailable
	}
	return err
}

// onSuccess resets failures and transitions half-open → closed.
func (cb *CircuitBreakerStore) onSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	if cb.state == stateHalfOpen {
		cb.transition(stateClosed)
	}
	cb.failures = 0
}

// onFailure increments the failure counter and transitions to open if the
// threshold is reached.
func (cb *CircuitBreakerStore) onFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.failures++
	cb.lastFailure = time.Now()

	switch cb.state {
	case stateHalfOpen:
		cb.transition(stateOpen)
	case stateClosed:
		if cb.failures >= cb.failThreshold {
			cb.transition(stateOpen)
		}
	}
}

// transition changes the circuit state and emits metrics + logs.
// Caller must hold cb.mu.
func (cb *CircuitBreakerStore) transition(to circuitState) {
	from := cb.state
	cb.state = to
	telemetry.CircuitBreakerState.Set(float64(to))
	telemetry.CircuitBreakerTransitionsTotal.WithLabelValues(from.String(), to.String()).Inc()

	switch to {
	case stateClosed:
		slog.Info("Circuit breaker: database recovered, circuit closed")
	case stateOpen:
		slog.Warn("Circuit breaker: database unreachable, circuit opened",
			"failures", cb.failures)
	case stateHalfOpen:
		slog.Warn("Circuit breaker: probing database")
	}
}

// isDBError returns true for genuine database failures. Application-level
// errors (S3Error, ErrNoSpaceAvailable) do not trip the circuit breaker.
func isDBError(err error) bool {
	if err == nil {
		return false
	}
	var s3err *S3Error
	if errors.As(err, &s3err) {
		return false
	}
	if errors.Is(err, ErrNoSpaceAvailable) {
		return false
	}
	return true
}

// -------------------------------------------------------------------------
// FORWARDING METHODS
// -------------------------------------------------------------------------

// Each method follows the pattern: preCheck → real.Method → postCheck.

func (cb *CircuitBreakerStore) GetAllObjectLocations(ctx context.Context, key string) ([]ObjectLocation, error) {
	if err := cb.preCheck(); err != nil {
		return nil, err
	}
	result, err := cb.real.GetAllObjectLocations(ctx, key)
	err = cb.postCheck(err)
	return result, err
}

func (cb *CircuitBreakerStore) RecordObject(ctx context.Context, key, backend string, size int64) error {
	if err := cb.preCheck(); err != nil {
		return err
	}
	err := cb.real.RecordObject(ctx, key, backend, size)
	err = cb.postCheck(err)
	return err
}

func (cb *CircuitBreakerStore) DeleteObject(ctx context.Context, key string) ([]DeletedCopy, error) {
	if err := cb.preCheck(); err != nil {
		return nil, err
	}
	result, err := cb.real.DeleteObject(ctx, key)
	err = cb.postCheck(err)
	return result, err
}

func (cb *CircuitBreakerStore) ListObjects(ctx context.Context, prefix, startAfter string, maxKeys int) (*ListObjectsResult, error) {
	if err := cb.preCheck(); err != nil {
		return nil, err
	}
	result, err := cb.real.ListObjects(ctx, prefix, startAfter, maxKeys)
	err = cb.postCheck(err)
	return result, err
}

func (cb *CircuitBreakerStore) GetBackendWithSpace(ctx context.Context, size int64, backendOrder []string) (string, error) {
	if err := cb.preCheck(); err != nil {
		return "", err
	}
	result, err := cb.real.GetBackendWithSpace(ctx, size, backendOrder)
	err = cb.postCheck(err)
	return result, err
}

func (cb *CircuitBreakerStore) CreateMultipartUpload(ctx context.Context, uploadID, key, backend, contentType string) error {
	if err := cb.preCheck(); err != nil {
		return err
	}
	err := cb.real.CreateMultipartUpload(ctx, uploadID, key, backend, contentType)
	err = cb.postCheck(err)
	return err
}

func (cb *CircuitBreakerStore) GetMultipartUpload(ctx context.Context, uploadID string) (*MultipartUpload, error) {
	if err := cb.preCheck(); err != nil {
		return nil, err
	}
	result, err := cb.real.GetMultipartUpload(ctx, uploadID)
	err = cb.postCheck(err)
	return result, err
}

func (cb *CircuitBreakerStore) RecordPart(ctx context.Context, uploadID string, partNumber int, etag string, size int64) error {
	if err := cb.preCheck(); err != nil {
		return err
	}
	err := cb.real.RecordPart(ctx, uploadID, partNumber, etag, size)
	err = cb.postCheck(err)
	return err
}

func (cb *CircuitBreakerStore) GetParts(ctx context.Context, uploadID string) ([]MultipartPart, error) {
	if err := cb.preCheck(); err != nil {
		return nil, err
	}
	result, err := cb.real.GetParts(ctx, uploadID)
	err = cb.postCheck(err)
	return result, err
}

func (cb *CircuitBreakerStore) DeleteMultipartUpload(ctx context.Context, uploadID string) error {
	if err := cb.preCheck(); err != nil {
		return err
	}
	err := cb.real.DeleteMultipartUpload(ctx, uploadID)
	err = cb.postCheck(err)
	return err
}

func (cb *CircuitBreakerStore) GetQuotaStats(ctx context.Context) (map[string]QuotaStat, error) {
	if err := cb.preCheck(); err != nil {
		return nil, err
	}
	result, err := cb.real.GetQuotaStats(ctx)
	err = cb.postCheck(err)
	return result, err
}

func (cb *CircuitBreakerStore) GetObjectCounts(ctx context.Context) (map[string]int64, error) {
	if err := cb.preCheck(); err != nil {
		return nil, err
	}
	result, err := cb.real.GetObjectCounts(ctx)
	err = cb.postCheck(err)
	return result, err
}

func (cb *CircuitBreakerStore) GetActiveMultipartCounts(ctx context.Context) (map[string]int64, error) {
	if err := cb.preCheck(); err != nil {
		return nil, err
	}
	result, err := cb.real.GetActiveMultipartCounts(ctx)
	err = cb.postCheck(err)
	return result, err
}

func (cb *CircuitBreakerStore) GetStaleMultipartUploads(ctx context.Context, olderThan time.Duration) ([]MultipartUpload, error) {
	if err := cb.preCheck(); err != nil {
		return nil, err
	}
	result, err := cb.real.GetStaleMultipartUploads(ctx, olderThan)
	err = cb.postCheck(err)
	return result, err
}

func (cb *CircuitBreakerStore) ListObjectsByBackend(ctx context.Context, backendName string, limit int) ([]ObjectLocation, error) {
	if err := cb.preCheck(); err != nil {
		return nil, err
	}
	result, err := cb.real.ListObjectsByBackend(ctx, backendName, limit)
	err = cb.postCheck(err)
	return result, err
}

func (cb *CircuitBreakerStore) MoveObjectLocation(ctx context.Context, key, fromBackend, toBackend string) (int64, error) {
	if err := cb.preCheck(); err != nil {
		return 0, err
	}
	result, err := cb.real.MoveObjectLocation(ctx, key, fromBackend, toBackend)
	err = cb.postCheck(err)
	return result, err
}

func (cb *CircuitBreakerStore) GetUnderReplicatedObjects(ctx context.Context, factor, limit int) ([]ObjectLocation, error) {
	if err := cb.preCheck(); err != nil {
		return nil, err
	}
	result, err := cb.real.GetUnderReplicatedObjects(ctx, factor, limit)
	err = cb.postCheck(err)
	return result, err
}

func (cb *CircuitBreakerStore) RecordReplica(ctx context.Context, key, targetBackend, sourceBackend string, size int64) (bool, error) {
	if err := cb.preCheck(); err != nil {
		return false, err
	}
	result, err := cb.real.RecordReplica(ctx, key, targetBackend, sourceBackend, size)
	err = cb.postCheck(err)
	return result, err
}
