package storage

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/afreidah/s3-proxy/internal/config"
)

func newTestCB(mock *mockStore, threshold int, timeout time.Duration) *CircuitBreakerStore {
	return NewCircuitBreakerStore(mock, config.CircuitBreakerConfig{
		FailureThreshold: threshold,
		OpenTimeout:      timeout,
	})
}

func TestCircuitBreaker_ClosedPassesThrough(t *testing.T) {
	mock := &mockStore{
		getAllLocationsResp: []ObjectLocation{{ObjectKey: "test", BackendName: "b1"}},
	}
	cb := newTestCB(mock, 3, time.Minute)

	result, err := cb.GetAllObjectLocations(context.Background(), "key")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 1 || result[0].BackendName != "b1" {
		t.Fatalf("unexpected result: %v", result)
	}
	if mock.callCount != 1 {
		t.Fatalf("expected 1 call, got %d", mock.callCount)
	}
}

func TestCircuitBreaker_OpensAfterThreshold(t *testing.T) {
	dbErr := errors.New("connection refused")
	mock := &mockStore{getAllLocationsErr: dbErr}
	cb := newTestCB(mock, 3, time.Minute)

	ctx := context.Background()

	// First 2 calls should pass through and return the raw DB error
	for i := 0; i < 2; i++ {
		_, err := cb.GetAllObjectLocations(ctx, "key")
		if err != dbErr {
			t.Fatalf("call %d: expected dbErr, got %v", i, err)
		}
	}

	// 3rd call trips the threshold — circuit opens, returns ErrDBUnavailable
	_, err := cb.GetAllObjectLocations(ctx, "key")
	if !errors.Is(err, ErrDBUnavailable) {
		t.Fatalf("call 2: expected ErrDBUnavailable, got %v", err)
	}
	if mock.callCount != 3 {
		t.Fatalf("expected 3 calls, got %d", mock.callCount)
	}

	// 4th call should return ErrDBUnavailable without hitting mock
	_, err = cb.GetAllObjectLocations(ctx, "key")
	if !errors.Is(err, ErrDBUnavailable) {
		t.Fatalf("expected ErrDBUnavailable, got %v", err)
	}
	if mock.callCount != 3 {
		t.Fatalf("expected mock not called again, got %d", mock.callCount)
	}
}

func TestCircuitBreaker_HalfOpenAfterTimeout(t *testing.T) {
	dbErr := errors.New("connection refused")
	mock := &mockStore{getAllLocationsErr: dbErr}
	cb := newTestCB(mock, 1, 10*time.Millisecond)

	ctx := context.Background()

	// Trip the circuit
	_, _ = cb.GetAllObjectLocations(ctx, "key")

	// Should be open
	_, err := cb.GetAllObjectLocations(ctx, "key")
	if !errors.Is(err, ErrDBUnavailable) {
		t.Fatalf("expected ErrDBUnavailable, got %v", err)
	}

	// Wait for timeout
	time.Sleep(15 * time.Millisecond)

	// Next call should probe (pass through to mock)
	mock.mu.Lock()
	mock.getAllLocationsErr = nil
	mock.getAllLocationsResp = []ObjectLocation{{ObjectKey: "test", BackendName: "b1"}}
	mock.mu.Unlock()

	result, err := cb.GetAllObjectLocations(ctx, "key")
	if err != nil {
		t.Fatalf("probe should succeed: %v", err)
	}
	if len(result) != 1 {
		t.Fatalf("expected result from probe, got %v", result)
	}

	// Circuit should be closed again
	if !cb.IsHealthy() {
		t.Fatal("expected circuit to be closed after successful probe")
	}
}

func TestCircuitBreaker_HalfOpenFailureReopens(t *testing.T) {
	dbErr := errors.New("connection refused")
	mock := &mockStore{getAllLocationsErr: dbErr}
	cb := newTestCB(mock, 1, 10*time.Millisecond)

	ctx := context.Background()

	// Trip the circuit
	_, _ = cb.GetAllObjectLocations(ctx, "key")

	// Wait for timeout
	time.Sleep(15 * time.Millisecond)

	// Probe should fail — circuit reopens, returns ErrDBUnavailable
	_, err := cb.GetAllObjectLocations(ctx, "key")
	if !errors.Is(err, ErrDBUnavailable) {
		t.Fatalf("expected ErrDBUnavailable on failed probe, got %v", err)
	}

	// Circuit should be open again
	_, err = cb.GetAllObjectLocations(ctx, "key")
	if !errors.Is(err, ErrDBUnavailable) {
		t.Fatalf("expected ErrDBUnavailable after failed probe, got %v", err)
	}
}

func TestCircuitBreaker_AppErrorsDontTrip(t *testing.T) {
	mock := &mockStore{getAllLocationsErr: ErrObjectNotFound}
	cb := newTestCB(mock, 1, time.Minute)

	ctx := context.Background()

	// Application errors should not trip the circuit
	for i := 0; i < 5; i++ {
		_, err := cb.GetAllObjectLocations(ctx, "key")
		if !errors.Is(err, ErrObjectNotFound) {
			t.Fatalf("expected ErrObjectNotFound, got %v", err)
		}
	}

	// Circuit should still be closed
	if !cb.IsHealthy() {
		t.Fatal("circuit should remain closed for application errors")
	}
	if mock.callCount != 5 {
		t.Fatalf("all 5 calls should have passed through, got %d", mock.callCount)
	}
}

func TestCircuitBreaker_IsHealthy(t *testing.T) {
	mock := &mockStore{getAllLocationsErr: errors.New("down")}
	cb := newTestCB(mock, 1, time.Minute)

	if !cb.IsHealthy() {
		t.Fatal("should start healthy")
	}

	_, _ = cb.GetAllObjectLocations(context.Background(), "key")

	if cb.IsHealthy() {
		t.Fatal("should be unhealthy after tripping")
	}
}

func TestCircuitBreaker_SuccessResetsFailures(t *testing.T) {
	mock := &mockStore{
		getAllLocationsResp: []ObjectLocation{{ObjectKey: "test", BackendName: "b1"}},
	}
	cb := newTestCB(mock, 3, time.Minute)

	ctx := context.Background()
	dbErr := errors.New("temporary")

	// 2 failures (below threshold)
	mock.mu.Lock()
	mock.getAllLocationsErr = dbErr
	mock.mu.Unlock()
	_, _ = cb.GetAllObjectLocations(ctx, "key")
	_, _ = cb.GetAllObjectLocations(ctx, "key")

	// 1 success resets the counter
	mock.mu.Lock()
	mock.getAllLocationsErr = nil
	mock.mu.Unlock()
	_, _ = cb.GetAllObjectLocations(ctx, "key")

	// 2 more failures should not trip (counter was reset)
	mock.mu.Lock()
	mock.getAllLocationsErr = dbErr
	mock.mu.Unlock()
	_, _ = cb.GetAllObjectLocations(ctx, "key")
	_, _ = cb.GetAllObjectLocations(ctx, "key")

	if !cb.IsHealthy() {
		t.Fatal("circuit should still be closed after reset + 2 failures")
	}
}
