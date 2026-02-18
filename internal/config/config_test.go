// -------------------------------------------------------------------------------
// Configuration Tests - Validation and Defaults
//
// Author: Alex Freidah
//
// Unit tests for configuration validation, default value application, duplicate
// backend detection, and PostgreSQL connection string generation.
// -------------------------------------------------------------------------------

package config

import (
	"testing"
	"time"
)

func TestConfigValidation_MinimalValid(t *testing.T) {
	cfg := Config{
		Server: ServerConfig{
			ListenAddr:    "0.0.0.0:9000",
			VirtualBucket: "unified",
		},
		Database: DatabaseConfig{
			Host:     "localhost",
			Database: "s3proxy",
			User:     "s3proxy",
		},
		Backends: []BackendConfig{
			{
				Name:            "test",
				Endpoint:        "https://s3.example.com",
				Bucket:          "mybucket",
				AccessKeyID:     "AKID",
				SecretAccessKey: "secret",
				QuotaBytes:      1024,
			},
		},
	}

	if err := cfg.SetDefaultsAndValidate(); err != nil {
		t.Errorf("valid config should pass validation: %v", err)
	}

	// Check defaults were set
	if cfg.Database.Port != 5432 {
		t.Errorf("database port default = %d, want 5432", cfg.Database.Port)
	}
	if cfg.Database.SSLMode != "disable" {
		t.Errorf("database ssl_mode default = %q, want 'disable'", cfg.Database.SSLMode)
	}
}

func TestConfigValidation_MissingRequired(t *testing.T) {
	cfg := Config{}
	err := cfg.SetDefaultsAndValidate()
	if err == nil {
		t.Error("empty config should fail validation")
	}
}

func TestConfigValidation_DuplicateBackendNames(t *testing.T) {
	cfg := Config{
		Server:   ServerConfig{ListenAddr: ":9000", VirtualBucket: "b"},
		Database: DatabaseConfig{Host: "h", Database: "d", User: "u"},
		Backends: []BackendConfig{
			{Name: "dup", Endpoint: "e", Bucket: "b", AccessKeyID: "a", SecretAccessKey: "s", QuotaBytes: 1},
			{Name: "dup", Endpoint: "e", Bucket: "b", AccessKeyID: "a", SecretAccessKey: "s", QuotaBytes: 1},
		},
	}

	err := cfg.SetDefaultsAndValidate()
	if err == nil {
		t.Error("duplicate backend names should fail validation")
	}
}

func TestConfigValidation_NegativeQuota(t *testing.T) {
	cfg := Config{
		Server:   ServerConfig{ListenAddr: ":9000", VirtualBucket: "b"},
		Database: DatabaseConfig{Host: "h", Database: "d", User: "u"},
		Backends: []BackendConfig{
			{Name: "bad", Endpoint: "e", Bucket: "b", AccessKeyID: "a", SecretAccessKey: "s", QuotaBytes: -1},
		},
	}

	err := cfg.SetDefaultsAndValidate()
	if err == nil {
		t.Error("negative quota should fail validation")
	}
}

func TestConnectionString(t *testing.T) {
	db := DatabaseConfig{
		Host:     "localhost",
		Port:     5433,
		Database: "s3proxy",
		User:     "s3proxy",
		Password: "secret",
		SSLMode:  "require",
	}

	got := db.ConnectionString()
	want := "postgres://s3proxy:secret@localhost:5433/s3proxy?sslmode=require"
	if got != want {
		t.Errorf("ConnectionString() = %q, want %q", got, want)
	}
}

func TestConnectionString_SpecialChars(t *testing.T) {
	db := DatabaseConfig{
		Host:     "db.example.com",
		Port:     5432,
		Database: "mydb",
		User:     "admin",
		Password: "p@ss=w ord&special",
		SSLMode:  "disable",
	}

	got := db.ConnectionString()
	// url.UserPassword percent-encodes @ but preserves = and &
	want := "postgres://admin:p%40ss=w%20ord&special@db.example.com:5432/mydb?sslmode=disable"
	if got != want {
		t.Errorf("ConnectionString() = %q, want %q", got, want)
	}
}

func TestRebalanceConfig_Defaults(t *testing.T) {
	cfg := validBaseConfig()
	cfg.Rebalance = RebalanceConfig{Enabled: true}

	if err := cfg.SetDefaultsAndValidate(); err != nil {
		t.Fatalf("valid rebalance config should pass: %v", err)
	}

	if cfg.Rebalance.Strategy != "pack" {
		t.Errorf("strategy default = %q, want %q", cfg.Rebalance.Strategy, "pack")
	}
	if cfg.Rebalance.Interval != 6*time.Hour {
		t.Errorf("interval default = %v, want %v", cfg.Rebalance.Interval, 6*time.Hour)
	}
	if cfg.Rebalance.BatchSize != 100 {
		t.Errorf("batch_size default = %d, want 100", cfg.Rebalance.BatchSize)
	}
	if cfg.Rebalance.Threshold != 0.1 {
		t.Errorf("threshold default = %f, want 0.1", cfg.Rebalance.Threshold)
	}
}

func TestRebalanceConfig_InvalidStrategy(t *testing.T) {
	cfg := validBaseConfig()
	cfg.Rebalance = RebalanceConfig{
		Enabled:   true,
		Strategy:  "invalid",
		Interval:  time.Hour,
		BatchSize: 10,
		Threshold: 0.1,
	}

	if err := cfg.SetDefaultsAndValidate(); err == nil {
		t.Error("invalid strategy should fail validation")
	}
}

func TestRebalanceConfig_DisabledSkipsValidation(t *testing.T) {
	cfg := validBaseConfig()
	cfg.Rebalance = RebalanceConfig{
		Enabled:  false,
		Strategy: "garbage",
	}

	if err := cfg.SetDefaultsAndValidate(); err != nil {
		t.Errorf("disabled rebalance should skip validation: %v", err)
	}
}

func TestRebalanceConfig_InvalidThreshold(t *testing.T) {
	cfg := validBaseConfig()
	cfg.Rebalance = RebalanceConfig{
		Enabled:   true,
		Strategy:  "spread",
		Interval:  time.Hour,
		BatchSize: 10,
		Threshold: 1.5,
	}

	if err := cfg.SetDefaultsAndValidate(); err == nil {
		t.Error("threshold > 1 should fail validation")
	}
}

func TestReplicationConfig_DefaultsWhenDisabled(t *testing.T) {
	cfg := validBaseConfig()
	// factor=0 should default to 1 (disabled)

	if err := cfg.SetDefaultsAndValidate(); err != nil {
		t.Fatalf("disabled replication should pass: %v", err)
	}

	if cfg.Replication.Factor != 1 {
		t.Errorf("factor default = %d, want 1", cfg.Replication.Factor)
	}
}

func TestReplicationConfig_DefaultsWhenEnabled(t *testing.T) {
	cfg := validBaseConfigTwoBackends()
	cfg.Replication = ReplicationConfig{Factor: 2}

	if err := cfg.SetDefaultsAndValidate(); err != nil {
		t.Fatalf("valid replication config should pass: %v", err)
	}

	if cfg.Replication.WorkerInterval != 5*time.Minute {
		t.Errorf("worker_interval default = %v, want %v", cfg.Replication.WorkerInterval, 5*time.Minute)
	}
	if cfg.Replication.BatchSize != 50 {
		t.Errorf("batch_size default = %d, want 50", cfg.Replication.BatchSize)
	}
}

func TestReplicationConfig_FactorExceedsBackends(t *testing.T) {
	cfg := validBaseConfig() // 1 backend
	cfg.Replication = ReplicationConfig{Factor: 2}

	if err := cfg.SetDefaultsAndValidate(); err == nil {
		t.Error("factor > backends should fail validation")
	}
}

func TestReplicationConfig_FactorNegative(t *testing.T) {
	cfg := validBaseConfig()
	cfg.Replication = ReplicationConfig{Factor: -1}

	if err := cfg.SetDefaultsAndValidate(); err == nil {
		t.Error("negative factor should fail validation")
	}
}

func TestReplicationConfig_DisabledSkipsValidation(t *testing.T) {
	cfg := validBaseConfig()
	cfg.Replication = ReplicationConfig{Factor: 1, WorkerInterval: -1}

	if err := cfg.SetDefaultsAndValidate(); err != nil {
		t.Errorf("factor=1 should skip interval validation: %v", err)
	}
}

func TestCircuitBreakerDefaults(t *testing.T) {
	cfg := validBaseConfig()

	if err := cfg.SetDefaultsAndValidate(); err != nil {
		t.Fatalf("valid config should pass: %v", err)
	}

	if cfg.CircuitBreaker.FailureThreshold != 3 {
		t.Errorf("failure_threshold default = %d, want 3", cfg.CircuitBreaker.FailureThreshold)
	}
	if cfg.CircuitBreaker.OpenTimeout != 15*time.Second {
		t.Errorf("open_timeout default = %v, want 15s", cfg.CircuitBreaker.OpenTimeout)
	}
	if cfg.CircuitBreaker.CacheTTL != 60*time.Second {
		t.Errorf("cache_ttl default = %v, want 60s", cfg.CircuitBreaker.CacheTTL)
	}
}

// validBaseConfig returns a Config with all required fields populated (1 backend).
func validBaseConfig() Config {
	return Config{
		Server:   ServerConfig{ListenAddr: ":9000", VirtualBucket: "b"},
		Database: DatabaseConfig{Host: "h", Database: "d", User: "u"},
		Backends: []BackendConfig{
			{Name: "b1", Endpoint: "e", Bucket: "b", AccessKeyID: "a", SecretAccessKey: "s", QuotaBytes: 1024},
		},
	}
}

// validBaseConfigTwoBackends returns a Config with 2 backends for replication tests.
func validBaseConfigTwoBackends() Config {
	return Config{
		Server:   ServerConfig{ListenAddr: ":9000", VirtualBucket: "b"},
		Database: DatabaseConfig{Host: "h", Database: "d", User: "u"},
		Backends: []BackendConfig{
			{Name: "b1", Endpoint: "e", Bucket: "b", AccessKeyID: "a", SecretAccessKey: "s", QuotaBytes: 1024},
			{Name: "b2", Endpoint: "e", Bucket: "b", AccessKeyID: "a", SecretAccessKey: "s", QuotaBytes: 2048},
		},
	}
}
