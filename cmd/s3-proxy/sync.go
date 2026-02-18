// -------------------------------------------------------------------------------
// Sync Subcommand - Import Pre-Existing Bucket Objects
//
// Author: Alex Freidah
//
// Scans a backend S3 bucket via ListObjectsV2 and imports discovered objects
// into the proxy's metadata database. Objects already tracked for the backend
// are skipped. Useful when bringing an existing bucket under proxy management.
// -------------------------------------------------------------------------------

package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"

	"github.com/afreidah/s3-proxy/internal/config"
	"github.com/afreidah/s3-proxy/internal/storage"
)

func runSync() {
	fs := flag.NewFlagSet("sync", flag.ExitOnError)
	configPath := fs.String("config", "config.yaml", "Path to configuration file")
	backendName := fs.String("backend", "", "Backend name to sync (required)")
	prefix := fs.String("prefix", "", "Only sync objects with this key prefix")
	dryRun := fs.Bool("dry-run", false, "Preview what would be imported without writing")
	_ = fs.Parse(os.Args[1:])

	if *backendName == "" {
		fmt.Fprintln(os.Stderr, "error: --backend is required")
		fs.Usage()
		os.Exit(1)
	}

	// --- Initialize structured logger ---
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))

	// --- Load configuration ---
	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		slog.Error("Failed to load config", "error", err)
		os.Exit(1)
	}

	// --- Find the target backend ---
	var backendCfg *config.BackendConfig
	for i := range cfg.Backends {
		if cfg.Backends[i].Name == *backendName {
			backendCfg = &cfg.Backends[i]
			break
		}
	}
	if backendCfg == nil {
		slog.Error("Backend not found in config", "backend", *backendName)
		os.Exit(1)
	}

	ctx := context.Background()

	// --- Initialize store ---
	store, err := storage.NewStore(ctx, cfg.Database)
	if err != nil {
		slog.Error("Failed to connect to database", "error", err)
		os.Exit(1)
	}
	defer store.Close()

	if err := store.RunMigrations(ctx); err != nil {
		slog.Error("Failed to run migrations", "error", err)
		os.Exit(1)
	}

	if err := store.SyncQuotaLimits(ctx, cfg.Backends); err != nil {
		slog.Error("Failed to sync quota limits", "error", err)
		os.Exit(1)
	}

	// --- Initialize backend ---
	backend, err := storage.NewS3Backend(*backendCfg)
	if err != nil {
		slog.Error("Failed to initialize backend", "backend", backendCfg.Name, "error", err)
		os.Exit(1)
	}

	// --- Scan and import ---
	var totalImported, totalSkipped int
	var totalBytes int64
	pageNum := 0

	mode := "sync"
	if *dryRun {
		mode = "dry-run"
	}

	slog.Info("Starting sync",
		"backend", backendCfg.Name,
		"bucket", backendCfg.Bucket,
		"prefix", *prefix,
		"mode", mode,
	)

	err = backend.ListObjects(ctx, *prefix, func(objects []storage.ListedObject) error {
		pageNum++
		pageImported := 0
		pageSkipped := 0

		for _, obj := range objects {
			if *dryRun {
				slog.Info("Would import", "key", obj.Key, "size", obj.SizeBytes)
				pageImported++
				totalBytes += obj.SizeBytes
				continue
			}

			imported, err := store.ImportObject(ctx, obj.Key, backendCfg.Name, obj.SizeBytes)
			if err != nil {
				return fmt.Errorf("failed to import %s: %w", obj.Key, err)
			}

			if imported {
				pageImported++
				totalBytes += obj.SizeBytes
			} else {
				pageSkipped++
			}
		}

		totalImported += pageImported
		totalSkipped += pageSkipped

		slog.Info("Synced page",
			"page", pageNum,
			"imported", pageImported,
			"skipped", pageSkipped,
			"total_imported", totalImported,
			"total_skipped", totalSkipped,
		)

		return nil
	})
	if err != nil {
		slog.Error("Sync failed", "error", err)
		os.Exit(1)
	}

	slog.Info("Sync complete",
		"backend", backendCfg.Name,
		"imported", totalImported,
		"skipped", totalSkipped,
		"bytes_imported", totalBytes,
		"mode", mode,
	)
}
