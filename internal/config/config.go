// -------------------------------------------------------------------------------
// Configuration - S3 Proxy Settings
//
// Project: Munchbox / Author: Alex Freidah
//
// Configuration types and loader for the S3 proxy. Supports environment variable
// expansion in YAML values using ${VAR} syntax. Validates required fields before
// returning to catch misconfiguration early.
// -------------------------------------------------------------------------------

package config

import (
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

// -------------------------------------------------------------------------
// CONFIGURATION TYPES
// -------------------------------------------------------------------------

// Config holds the complete service configuration.
type Config struct {
	Server    ServerConfig    `yaml:"server"`
	Auth      AuthConfig      `yaml:"auth"`
	Database  DatabaseConfig  `yaml:"database"`
	Backends  []BackendConfig `yaml:"backends"`
	Telemetry TelemetryConfig `yaml:"telemetry"`
}

// DatabaseConfig holds PostgreSQL connection settings.
type DatabaseConfig struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	Database string `yaml:"database"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	SSLMode  string `yaml:"ssl_mode"`
}

// ServerConfig holds HTTP server settings.
type ServerConfig struct {
	ListenAddr    string `yaml:"listen_addr"`
	VirtualBucket string `yaml:"virtual_bucket"`
}

// AuthConfig holds authentication settings. Supports both AWS SigV4 (for S3
// client compatibility) and a simple token for backward compatibility.
type AuthConfig struct {
	Token          string `yaml:"token"`
	AccessKeyID    string `yaml:"access_key_id"`
	SecretAccessKey string `yaml:"secret_access_key"`
}

// BackendConfig holds configuration for an S3-compatible storage backend.
type BackendConfig struct {
	Name            string `yaml:"name"`             // Identifier for metrics/tracing
	Endpoint        string `yaml:"endpoint"`         // S3-compatible endpoint URL
	Region          string `yaml:"region"`           // AWS region or equivalent
	Bucket          string `yaml:"bucket"`           // Target bucket name
	AccessKeyID     string `yaml:"access_key_id"`    // AWS access key ID
	SecretAccessKey string `yaml:"secret_access_key"` // AWS secret access key
	ForcePathStyle  bool   `yaml:"force_path_style"` // Use path-style URLs
	QuotaBytes      int64  `yaml:"quota_bytes"`      // Maximum bytes allowed on this backend
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

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	return &cfg, nil
}

// -------------------------------------------------------------------------
// VALIDATION
// -------------------------------------------------------------------------

// Validate checks that required configuration values are present and sets defaults.
func (c *Config) Validate() error {
	var errors []string

	// --- Server validation ---
	if c.Server.ListenAddr == "" {
		errors = append(errors, "server.listen_addr is required")
	}
	if c.Server.VirtualBucket == "" {
		errors = append(errors, "server.virtual_bucket is required")
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

	if len(errors) > 0 {
		return fmt.Errorf("%s", strings.Join(errors, "; "))
	}
	return nil
}

// ConnectionString returns the PostgreSQL connection string.
func (c *DatabaseConfig) ConnectionString() string {
	return fmt.Sprintf(
		"host=%s port=%d dbname=%s user=%s password=%s sslmode=%s",
		c.Host, c.Port, c.Database, c.User, c.Password, c.SSLMode,
	)
}
