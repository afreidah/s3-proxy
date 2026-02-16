package config

import (
	"testing"
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

	if err := cfg.Validate(); err != nil {
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
	err := cfg.Validate()
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

	err := cfg.Validate()
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

	err := cfg.Validate()
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
	want := "host=localhost port=5433 dbname=s3proxy user=s3proxy password=secret sslmode=require"
	if got != want {
		t.Errorf("ConnectionString() = %q, want %q", got, want)
	}
}
