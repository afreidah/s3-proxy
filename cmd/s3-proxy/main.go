// -------------------------------------------------------------------------------
// S3 Proxy - Unified S3 Endpoint with Quota Management
//
// Project: Munchbox / Author: Alex Freidah
//
// Entry point for the S3 proxy service. Dispatches to subcommands: "serve"
// (default) starts the HTTP server, "sync" imports pre-existing bucket objects
// into the proxy's metadata database.
// -------------------------------------------------------------------------------

package main

import (
	"context"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"syscall"
	"time"

	"github.com/munchbox/s3-proxy/internal/auth"
	"github.com/munchbox/s3-proxy/internal/config"
	"github.com/munchbox/s3-proxy/internal/server"
	"github.com/munchbox/s3-proxy/internal/storage"
	"github.com/munchbox/s3-proxy/internal/telemetry"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "sync" {
		os.Args = os.Args[1:]
		runSync()
		return
	}
	runServe()
}

func runServe() {
	configPath := flag.String("config", "config.yaml", "Path to configuration file")
	flag.Parse()

	// --- Initialize structured logger ---
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))

	// --- Load configuration ---
	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		slog.Error("Failed to load config", "error", err)
		os.Exit(1)
	}

	// --- Initialize tracing ---
	ctx := context.Background()
	shutdownTracer, err := telemetry.InitTracer(ctx, cfg.Telemetry.Tracing)
	if err != nil {
		slog.Error("Failed to initialize tracer", "error", err)
		os.Exit(1)
	}

	// --- Set build info metric ---
	telemetry.BuildInfo.WithLabelValues(telemetry.Version, runtime.Version()).Set(1)

	// --- Initialize PostgreSQL store ---
	store, err := storage.NewStore(ctx, cfg.Database)
	if err != nil {
		slog.Error("Failed to connect to database", "error", err)
		os.Exit(1)
	}
	slog.Info("Connected to PostgreSQL",
		"host", cfg.Database.Host,
		"port", cfg.Database.Port,
		"database", cfg.Database.Database,
	)

	// --- Run database migrations ---
	if err := store.RunMigrations(ctx); err != nil {
		slog.Error("Failed to run migrations", "error", err)
		os.Exit(1)
	}
	slog.Info("Database migrations applied")

	// --- Sync quota limits from config to database ---
	if err := store.SyncQuotaLimits(ctx, cfg.Backends); err != nil {
		slog.Error("Failed to sync quota limits", "error", err)
		os.Exit(1)
	}

	// --- Initialize backends ---
	backends := make(map[string]storage.ObjectBackend)
	backendOrder := make([]string, 0, len(cfg.Backends))

	for _, bcfg := range cfg.Backends {
		backend, err := storage.NewS3Backend(bcfg)
		if err != nil {
			slog.Error("Failed to initialize backend", "backend", bcfg.Name, "error", err)
			os.Exit(1)
		}
		backends[bcfg.Name] = backend
		backendOrder = append(backendOrder, bcfg.Name)
		slog.Info("Backend initialized",
			"backend", bcfg.Name,
			"endpoint", bcfg.Endpoint,
			"bucket", bcfg.Bucket,
			"quota_bytes", bcfg.QuotaBytes,
		)
	}

	// --- Create backend manager ---
	manager := storage.NewBackendManager(backends, store, backendOrder)

	// --- Initial quota metrics update ---
	if err := manager.UpdateQuotaMetrics(ctx); err != nil {
		slog.Warn("Failed to update initial quota metrics", "error", err)
	}

	// --- Start background tasks with cancellable context ---
	bgCtx, bgCancel := context.WithCancel(context.Background())
	defer bgCancel()
	var bgWg sync.WaitGroup

	bgWg.Add(1)
	go func() {
		defer bgWg.Done()
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if err := manager.UpdateQuotaMetrics(bgCtx); err != nil {
					slog.Error("Failed to update quota metrics", "error", err)
				}
			case <-bgCtx.Done():
				return
			}
		}
	}()

	bgWg.Add(1)
	go func() {
		defer bgWg.Done()
		ticker := time.NewTicker(1 * time.Hour)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				manager.CleanupStaleMultipartUploads(bgCtx, 24*time.Hour)
			case <-bgCtx.Done():
				return
			}
		}
	}()

	// --- Rebalancer background task ---
	if cfg.Rebalance.Enabled {
		slog.Info("Rebalancer enabled",
			"strategy", cfg.Rebalance.Strategy,
			"interval", cfg.Rebalance.Interval,
			"batch_size", cfg.Rebalance.BatchSize,
			"threshold", cfg.Rebalance.Threshold,
		)
		bgWg.Add(1)
		go func() {
			defer bgWg.Done()
			ticker := time.NewTicker(cfg.Rebalance.Interval)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					moved, err := manager.Rebalance(bgCtx, cfg.Rebalance)
					if err != nil {
						slog.Error("Rebalance failed", "error", err)
					} else if moved > 0 {
						slog.Info("Rebalance completed", "objects_moved", moved)
						manager.UpdateQuotaMetrics(bgCtx)
					}
				case <-bgCtx.Done():
					return
				}
			}
		}()
	}

	// --- Replication worker background task ---
	if cfg.Replication.Factor > 1 {
		slog.Info("Replication worker enabled",
			"factor", cfg.Replication.Factor,
			"interval", cfg.Replication.WorkerInterval,
			"batch_size", cfg.Replication.BatchSize,
		)
		bgWg.Add(1)
		go func() {
			defer bgWg.Done()
			// Run once at startup to catch up on pending replicas
			created, err := manager.Replicate(bgCtx, cfg.Replication)
			if err != nil {
				slog.Error("Replication startup run failed", "error", err)
			} else if created > 0 {
				slog.Info("Replication startup run completed", "copies_created", created)
				manager.UpdateQuotaMetrics(bgCtx)
			}

			ticker := time.NewTicker(cfg.Replication.WorkerInterval)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					created, err := manager.Replicate(bgCtx, cfg.Replication)
					if err != nil {
						slog.Error("Replication failed", "error", err)
					} else if created > 0 {
						slog.Info("Replication completed", "copies_created", created)
						manager.UpdateQuotaMetrics(bgCtx)
					}
				case <-bgCtx.Done():
					return
				}
			}
		}()
	}

	// --- Create server ---
	srv := &server.Server{
		Manager:       manager,
		VirtualBucket: cfg.Server.VirtualBucket,
		AuthConfig:    cfg.Auth,
		MaxObjectSize: cfg.Server.MaxObjectSize,
	}

	// --- Setup HTTP mux ---
	mux := http.NewServeMux()

	// Metrics endpoint
	if cfg.Telemetry.Metrics.Enabled {
		mux.Handle(cfg.Telemetry.Metrics.Path, promhttp.Handler())
		slog.Info("Metrics endpoint enabled", "path", cfg.Telemetry.Metrics.Path)
	}

	// Health check endpoint
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	// S3 proxy handler (all other paths), optionally rate-limited
	var s3Handler http.Handler = srv
	if cfg.RateLimit.Enabled {
		rl := server.NewRateLimiter(cfg.RateLimit)
		s3Handler = rl.Middleware(srv)
		slog.Info("Rate limiting enabled",
			"requests_per_sec", cfg.RateLimit.RequestsPerSec,
			"burst", cfg.RateLimit.Burst,
		)
	}
	mux.Handle("/", s3Handler)

	httpServer := &http.Server{
		Addr:         cfg.Server.ListenAddr,
		Handler:      mux,
		ReadTimeout:  5 * time.Minute,
		WriteTimeout: 5 * time.Minute,
		IdleTimeout:  120 * time.Second,
	}

	// --- Handle graceful shutdown ---
	go func() {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
		<-sigChan

		slog.Info("Shutting down")

		// Stop background goroutines and wait for them to finish
		bgCancel()
		bgWg.Wait()

		// Shutdown HTTP server with timeout
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		if err := httpServer.Shutdown(shutdownCtx); err != nil {
			slog.Error("HTTP server shutdown error", "error", err)
		}

		// Close database connection
		store.Close()

		// Flush traces
		if err := shutdownTracer(shutdownCtx); err != nil {
			slog.Error("Tracer shutdown error", "error", err)
		}
	}()

	// --- Log startup info ---
	slog.Info("S3 Proxy starting",
		"version", telemetry.Version,
		"listen_addr", cfg.Server.ListenAddr,
		"virtual_bucket", cfg.Server.VirtualBucket,
		"backends", len(cfg.Backends),
	)

	if !auth.NeedsAuth(cfg.Auth) {
		slog.Warn("Authentication is disabled")
	} else if cfg.Auth.AccessKeyID != "" {
		slog.Info("Authentication enabled", "method", "SigV4")
	} else {
		slog.Info("Authentication enabled", "method", "legacy_token")
	}

	if cfg.Telemetry.Tracing.Enabled {
		slog.Info("Tracing enabled",
			"endpoint", cfg.Telemetry.Tracing.Endpoint,
			"sample_rate", cfg.Telemetry.Tracing.SampleRate,
			"insecure", cfg.Telemetry.Tracing.Insecure,
		)
	}

	// --- Start server ---
	if err := httpServer.ListenAndServe(); err != http.ErrServerClosed {
		slog.Error("Server error", "error", err)
		os.Exit(1)
	}

	slog.Info("Server stopped")
}
