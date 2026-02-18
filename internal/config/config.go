// -------------------------------------------------------------------------------
// Configuration - S3 Proxy Settings
//
// Author: Alex Freidah
//
// Configuration types and loader for the S3 proxy. Supports environment variable
// expansion in YAML values using ${VAR} syntax. Validates required fields before
// returning to catch misconfiguration early.
// -------------------------------------------------------------------------------

package config

import (
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// -------------------------------------------------------------------------
// CONFIGURATION TYPES
// -------------------------------------------------------------------------

// Config holds the complete service configuration.
type Config struct {
	Server         ServerConfig         `yaml:"server"`
	Auth           AuthConfig           `yaml:"auth"`
	Database       DatabaseConfig       `yaml:"database"`
	Backends       []BackendConfig      `yaml:"backends"`
	Telemetry      TelemetryConfig      `yaml:"telemetry"`
	Rebalance      RebalanceConfig      `yaml:"rebalance"`
	Replication    ReplicationConfig    `yaml:"replication"`
	RateLimit      RateLimitConfig      `yaml:"rate_limit"`
	CircuitBreaker CircuitBreakerConfig `yaml:"circuit_breaker"`
}

// DatabaseConfig holds PostgreSQL connection settings.
type DatabaseConfig struct {
	Host            string        `yaml:"host"`
	Port            int           `yaml:"port"`
	Database        string        `yaml:"database"`
	User            string        `yaml:"user"`
	Password        string        `yaml:"password"`
	SSLMode         string        `yaml:"ssl_mode"`
	MaxConns        int32         `yaml:"max_conns"`         // Max pool connections (default: 10)
	MinConns        int32         `yaml:"min_conns"`         // Min idle connections (default: 5)
	MaxConnLifetime time.Duration `yaml:"max_conn_lifetime"` // Max connection age (default: 5m)
}

// ServerConfig holds HTTP server settings.
type ServerConfig struct {
	ListenAddr     string        `yaml:"listen_addr"`
	VirtualBucket  string        `yaml:"virtual_bucket"`
	MaxObjectSize  int64         `yaml:"max_object_size"`  // Max upload size in bytes (default: 5GB)
	BackendTimeout time.Duration `yaml:"backend_timeout"`  // Per-operation timeout for backend S3 calls (default: 30s)
}

// AuthConfig holds authentication settings. Supports both AWS SigV4 (for S3
// client compatibility) and a simple token for backward compatibility.
type AuthConfig struct {
	Token           string `yaml:"token"`
	AccessKeyID     string `yaml:"access_key_id"`
	SecretAccessKey string `yaml:"secret_access_key"`
}

// BackendConfig holds configuration for an S3-compatible storage backend.
type BackendConfig struct {
	Name            string `yaml:"name"`              // Identifier for metrics/tracing
	Endpoint        string `yaml:"endpoint"`          // S3-compatible endpoint URL
	Region          string `yaml:"region"`            // AWS region or equivalent
	Bucket          string `yaml:"bucket"`            // Target bucket name
	AccessKeyID     string `yaml:"access_key_id"`     // AWS access key ID
	SecretAccessKey string `yaml:"secret_access_key"` // AWS secret access key
	ForcePathStyle  bool   `yaml:"force_path_style"`  // Use path-style URLs
	QuotaBytes      int64  `yaml:"quota_bytes"`       // Maximum bytes allowed on this backend
}

// TelemetryConfig holds observability settings.
type TelemetryConfig struct {
	Metrics MetricsConfig `yaml:"metrics"`
	Tracing TracingConfig `yaml:"tracing"`
}

// MetricsConfig holds Prometheus metrics settings.
type MetricsConfig struct {
	Enabled bool   `yaml:"enabled"`
	Path    string `yaml:"path"`
}

// TracingConfig holds OpenTelemetry tracing settings.
type TracingConfig struct {
	Enabled    bool    `yaml:"enabled"`
	Endpoint   string  `yaml:"endpoint"`
	SampleRate float64 `yaml:"sample_rate"`
	Insecure   bool    `yaml:"insecure"` // Use insecure connection (no TLS)
}

// RebalanceConfig holds settings for the periodic backend rebalancer.
// Disabled by default to avoid unexpected API calls and egress charges.
type RebalanceConfig struct {
	Enabled   bool          `yaml:"enabled"`
	Strategy  string        `yaml:"strategy"`   // "pack" or "spread"
	Interval  time.Duration `yaml:"interval"`
	BatchSize int           `yaml:"batch_size"`
	Threshold float64       `yaml:"threshold"`  // min utilization spread to trigger
}

// ReplicationConfig holds settings for the background replication worker.
// When factor is 1, replication is disabled and behavior is identical to
// the single-copy default.
type ReplicationConfig struct {
	Factor         int           `yaml:"factor"`
	WorkerInterval time.Duration `yaml:"worker_interval"`
	BatchSize      int           `yaml:"batch_size"`
}

// RateLimitConfig holds per-IP rate limiting settings. Disabled by default.
type RateLimitConfig struct {
	Enabled       bool    `yaml:"enabled"`
	RequestsPerSec float64 `yaml:"requests_per_sec"` // Token refill rate (default: 100)
	Burst         int     `yaml:"burst"`             // Max burst size (default: 200)
}

// CircuitBreakerConfig holds settings for the database circuit breaker. When
// the database becomes unreachable, the proxy enters degraded mode: reads
// broadcast to all backends, writes return 503.
type CircuitBreakerConfig struct {
	FailureThreshold int           `yaml:"failure_threshold"` // Consecutive failures before opening (default: 3)
	OpenTimeout      time.Duration `yaml:"open_timeout"`      // Delay before probing recovery (default: 15s)
	CacheTTL         time.Duration `yaml:"cache_ttl"`         // TTL for key→backend cache during degraded reads (default: 60s)
}

// -------------------------------------------------------------------------
// CONFIGURATION LOADER
// -------------------------------------------------------------------------

// LoadConfig reads and parses the configuration file with environment variable
// expansion. Returns an error if the file cannot be read, parsed, or validated.
func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	// --- Expand environment variables ---
	expanded := os.Expand(string(data), func(key string) string {
		return os.Getenv(key)
	})

	var cfg Config
	if err := yaml.Unmarshal([]byte(expanded), &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}

	if err := cfg.SetDefaultsAndValidate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	return &cfg, nil
}

// -------------------------------------------------------------------------
// VALIDATION
// -------------------------------------------------------------------------

// SetDefaultsAndValidate applies default values for optional fields and checks
// that all required configuration values are present.
func (c *Config) SetDefaultsAndValidate() error {
	var errors []string

	// --- Server validation ---
	if c.Server.ListenAddr == "" {
		errors = append(errors, "server.listen_addr is required")
	}
	if c.Server.VirtualBucket == "" {
		errors = append(errors, "server.virtual_bucket is required")
	}

	if c.Server.MaxObjectSize == 0 {
		c.Server.MaxObjectSize = 5 * 1024 * 1024 * 1024 // 5 GB
	}

	if c.Server.BackendTimeout == 0 {
		c.Server.BackendTimeout = 30 * time.Second
	}

	// --- Database validation ---
	if c.Database.Host == "" {
		errors = append(errors, "database.host is required")
	}
	if c.Database.Database == "" {
		errors = append(errors, "database.database is required")
	}
	if c.Database.User == "" {
		errors = append(errors, "database.user is required")
	}

	// --- Database defaults ---
	if c.Database.Port == 0 {
		c.Database.Port = 5432
	}
	if c.Database.SSLMode == "" {
		c.Database.SSLMode = "disable"
	}
	if c.Database.MaxConns == 0 {
		c.Database.MaxConns = 10
	}
	if c.Database.MinConns == 0 {
		c.Database.MinConns = 5
	}
	if c.Database.MaxConnLifetime == 0 {
		c.Database.MaxConnLifetime = 5 * time.Minute
	}

	// --- Backends validation ---
	if len(c.Backends) == 0 {
		errors = append(errors, "at least one backend is required")
	}

	names := make(map[string]bool)
	for i := range c.Backends {
		b := &c.Backends[i]
		prefix := fmt.Sprintf("backends[%d]", i)

		if b.Name == "" {
			b.Name = fmt.Sprintf("backend-%d", i)
		}
		if names[b.Name] {
			errors = append(errors, fmt.Sprintf("%s: duplicate backend name '%s'", prefix, b.Name))
		}
		names[b.Name] = true

		if b.Endpoint == "" {
			errors = append(errors, fmt.Sprintf("%s: endpoint is required", prefix))
		}
		if b.Bucket == "" {
			errors = append(errors, fmt.Sprintf("%s: bucket is required", prefix))
		}
		if b.AccessKeyID == "" {
			errors = append(errors, fmt.Sprintf("%s: access_key_id is required", prefix))
		}
		if b.SecretAccessKey == "" {
			errors = append(errors, fmt.Sprintf("%s: secret_access_key is required", prefix))
		}
		if b.QuotaBytes <= 0 {
			errors = append(errors, fmt.Sprintf("%s: quota_bytes must be positive", prefix))
		}
	}

	// --- Telemetry defaults ---
	if c.Telemetry.Metrics.Path == "" {
		c.Telemetry.Metrics.Path = "/metrics"
	}
	if c.Telemetry.Tracing.SampleRate == 0 && c.Telemetry.Tracing.Enabled {
		c.Telemetry.Tracing.SampleRate = 1.0
	}

	// --- Validate tracing config ---
	if c.Telemetry.Tracing.Enabled && c.Telemetry.Tracing.Endpoint == "" {
		errors = append(errors, "telemetry.tracing.endpoint is required when tracing is enabled")
	}

	// --- Rebalance defaults ---
	if c.Rebalance.Enabled {
		if c.Rebalance.Strategy == "" {
			c.Rebalance.Strategy = "pack"
		}
		if c.Rebalance.Interval == 0 {
			c.Rebalance.Interval = 6 * time.Hour
		}
		if c.Rebalance.BatchSize == 0 {
			c.Rebalance.BatchSize = 100
		}
		if c.Rebalance.Threshold == 0 {
			c.Rebalance.Threshold = 0.1
		}

		// --- Rebalance validation ---
		if c.Rebalance.Strategy != "pack" && c.Rebalance.Strategy != "spread" {
			errors = append(errors, "rebalance.strategy must be 'pack' or 'spread'")
		}
		if c.Rebalance.Interval <= 0 {
			errors = append(errors, "rebalance.interval must be positive")
		}
		if c.Rebalance.BatchSize <= 0 {
			errors = append(errors, "rebalance.batch_size must be positive")
		}
		if c.Rebalance.Threshold < 0 || c.Rebalance.Threshold > 1 {
			errors = append(errors, "rebalance.threshold must be between 0 and 1")
		}
	}

	// --- Replication defaults ---
	if c.Replication.Factor == 0 {
		c.Replication.Factor = 1
	}
	if c.Replication.Factor > 1 {
		if c.Replication.WorkerInterval == 0 {
			c.Replication.WorkerInterval = 5 * time.Minute
		}
		if c.Replication.BatchSize == 0 {
			c.Replication.BatchSize = 50
		}

		// --- Replication validation ---
		if c.Replication.Factor > len(c.Backends) {
			errors = append(errors, fmt.Sprintf(
				"replication.factor (%d) cannot exceed number of backends (%d)",
				c.Replication.Factor, len(c.Backends)))
		}
		if c.Replication.WorkerInterval <= 0 {
			errors = append(errors, "replication.worker_interval must be positive")
		}
		if c.Replication.BatchSize <= 0 {
			errors = append(errors, "replication.batch_size must be positive")
		}
	}
	if c.Replication.Factor < 1 {
		errors = append(errors, "replication.factor must be at least 1")
	}

	// --- Rate limit defaults ---
	if c.RateLimit.Enabled {
		if c.RateLimit.RequestsPerSec == 0 {
			c.RateLimit.RequestsPerSec = 100
		}
		if c.RateLimit.Burst == 0 {
			c.RateLimit.Burst = 200
		}
		if c.RateLimit.RequestsPerSec <= 0 {
			errors = append(errors, "rate_limit.requests_per_sec must be positive")
		}
		if c.RateLimit.Burst <= 0 {
			errors = append(errors, "rate_limit.burst must be positive")
		}
	}

	// --- Circuit breaker defaults ---
	if c.CircuitBreaker.FailureThreshold == 0 {
		c.CircuitBreaker.FailureThreshold = 3
	}
	if c.CircuitBreaker.OpenTimeout == 0 {
		c.CircuitBreaker.OpenTimeout = 15 * time.Second
	}
	if c.CircuitBreaker.CacheTTL == 0 {
		c.CircuitBreaker.CacheTTL = 60 * time.Second
	}

	if len(errors) > 0 {
		return fmt.Errorf("%s", strings.Join(errors, "; "))
	}
	return nil
}

// ConnectionString returns a PostgreSQL connection URI with properly escaped
// credentials, safe for passwords containing special characters.
func (c *DatabaseConfig) ConnectionString() string {
	u := &url.URL{
		Scheme:   "postgres",
		User:     url.UserPassword(c.User, c.Password),
		Host:     fmt.Sprintf("%s:%d", c.Host, c.Port),
		Path:     c.Database,
		RawQuery: fmt.Sprintf("sslmode=%s", url.QueryEscape(c.SSLMode)),
	}
	return u.String()
}
