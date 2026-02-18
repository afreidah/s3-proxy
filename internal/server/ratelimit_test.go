package server

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/afreidah/s3-proxy/internal/config"
)

func TestRateLimiter_AllowAndBlock(t *testing.T) {
	rl := NewRateLimiter(config.RateLimitConfig{
		Enabled:        true,
		RequestsPerSec: 1,
		Burst:          2,
	})

	// First 2 requests (burst) should be allowed
	if !rl.Allow("10.0.0.1") {
		t.Error("first request should be allowed")
	}
	if !rl.Allow("10.0.0.1") {
		t.Error("second request (within burst) should be allowed")
	}

	// Third request should be blocked (burst exhausted, rate is 1/s)
	if rl.Allow("10.0.0.1") {
		t.Error("third request should be blocked (burst exhausted)")
	}

	// Different IP should still be allowed
	if !rl.Allow("10.0.0.2") {
		t.Error("different IP should have its own bucket")
	}
}

func TestRateLimiter_Middleware429(t *testing.T) {
	rl := NewRateLimiter(config.RateLimitConfig{
		Enabled:        true,
		RequestsPerSec: 1,
		Burst:          1,
	})

	ok := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	handler := rl.Middleware(ok)

	// First request succeeds
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/test-bucket/key", nil)
	req.RemoteAddr = "10.0.0.1:12345"
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("first request: got %d, want 200", rec.Code)
	}

	// Second request should be rate-limited
	rec2 := httptest.NewRecorder()
	handler.ServeHTTP(rec2, req)
	if rec2.Code != http.StatusTooManyRequests {
		t.Errorf("second request: got %d, want 429", rec2.Code)
	}
}

func TestExtractIP(t *testing.T) {
	tests := []struct {
		name       string
		remoteAddr string
		xff        string
		want       string
	}{
		{"ip:port", "10.0.0.1:12345", "", "10.0.0.1"},
		{"ip only", "10.0.0.1", "", "10.0.0.1"},
		{"xff single", "10.0.0.1:12345", "192.168.1.1", "192.168.1.1"},
		{"xff chain", "10.0.0.1:12345", "192.168.1.1, 10.0.0.2, 10.0.0.3", "192.168.1.1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := httptest.NewRequest("GET", "/", nil)
			r.RemoteAddr = tt.remoteAddr
			if tt.xff != "" {
				r.Header.Set("X-Forwarded-For", tt.xff)
			}
			got := extractIP(r)
			if got != tt.want {
				t.Errorf("extractIP() = %q, want %q", got, tt.want)
			}
		})
	}
}
