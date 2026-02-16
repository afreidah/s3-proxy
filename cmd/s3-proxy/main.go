// -------------------------------------------------------------------------------
// S3 Proxy - Unified S3 Endpoint with Quota Management
//
// Project: Munchbox / Author: Alex Freidah
//
// Entry point for the S3 proxy service. Loads configuration, initializes multiple
// backends with quota tracking, and starts the HTTP server. Objects are transparently
// routed to backends based on available quota.
// -------------------------------------------------------------------------------

package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"runtime"
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
	configPath := flag.String("config", "config.yaml", "Path to configuration file")
	flag.Parse()

	// --- Load configuration ---
	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// --- Initialize tracing ---
	ctx := context.Background()
	shutdownTracer, err := telemetry.InitTracer(ctx, cfg.Telemetry.Tracing)
	if err != nil {
		log.Fatalf("Failed to initialize tracer: %v", err)
	}

	// --- Set build info metric ---
	telemetry.BuildInfo.WithLabelValues(telemetry.Version, runtime.Version()).Set(1)

	// --- Initialize PostgreSQL store ---
	store, err := storage.NewStore(cfg.Database.ConnectionString())
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	log.Printf("Connected to PostgreSQL: %s:%d/%s", cfg.Database.Host, cfg.Database.Port, cfg.Database.Database)

	// --- Run database migrations ---
	if err := store.RunMigrations(ctx); err != nil {
		log.Fatalf("Failed to run migrations: %v", err)
	}
	log.Println("Database migrations applied")

	// --- Sync quota limits from config to database ---
	if err := store.SyncQuotaLimits(ctx, cfg.Backends); err != nil {
		log.Fatalf("Failed to sync quota limits: %v", err)
	}

	// --- Initialize backends ---
	backends := make(map[string]*storage.S3Backend)
	backendOrder := make([]string, 0, len(cfg.Backends))

	for _, bcfg := range cfg.Backends {
		backend, err := storage.NewS3Backend(bcfg)
		if err != nil {
			log.Fatalf("Failed to initialize backend %s: %v", bcfg.Name, err)
		}
		backends[bcfg.Name] = backend
		backendOrder = append(backendOrder, bcfg.Name)
		log.Printf("Backend [%s]: %s/%s (quota: %d bytes)", bcfg.Name, bcfg.Endpoint, bcfg.Bucket, bcfg.QuotaBytes)
	}

	// --- Create backend manager ---
	manager := storage.NewBackendManager(backends, store, backendOrder)

	// --- Initial quota metrics update ---
	if err := manager.UpdateQuotaMetrics(ctx); err != nil {
		log.Printf("Warning: Failed to update initial quota metrics: %v", err)
	}

	// --- Start periodic quota metrics updater ---
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			if err := manager.UpdateQuotaMetrics(context.Background()); err != nil {
				log.Printf("Failed to update quota metrics: %v", err)
			}
		}
	}()

	// --- Start periodic multipart upload cleanup ---
	go func() {
		ticker := time.NewTicker(1 * time.Hour)
		defer ticker.Stop()
		for range ticker.C {
			manager.CleanupStaleMultipartUploads(context.Background(), 24*time.Hour)
		}
	}()

	// --- Create server ---
	srv := &server.Server{
		Manager:       manager,
		VirtualBucket: cfg.Server.VirtualBucket,
		AuthConfig:    cfg.Auth,
	}

	// --- Setup HTTP mux ---
	mux := http.NewServeMux()

	// Metrics endpoint
	if cfg.Telemetry.Metrics.Enabled {
		mux.Handle(cfg.Telemetry.Metrics.Path, promhttp.Handler())
		log.Printf("Metrics endpoint: %s", cfg.Telemetry.Metrics.Path)
	}

	// Health check endpoint
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})

	// S3 proxy handler (all other paths)
	mux.Handle("/", srv)

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

		log.Println("Shutting down...")

		// Shutdown HTTP server with timeout
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		if err := httpServer.Shutdown(shutdownCtx); err != nil {
			log.Printf("HTTP server shutdown error: %v", err)
		}

		// Close database connection
		if err := store.Close(); err != nil {
			log.Printf("Database close error: %v", err)
		}

		// Flush traces
		if err := shutdownTracer(shutdownCtx); err != nil {
			log.Printf("Tracer shutdown error: %v", err)
		}
	}()

	// --- Log startup info ---
	log.Printf("S3 Proxy v%s starting on %s", telemetry.Version, cfg.Server.ListenAddr)
	log.Printf("Virtual bucket: %s", cfg.Server.VirtualBucket)
	log.Printf("Backends configured: %d", len(cfg.Backends))

	if !auth.NeedsAuth(cfg.Auth) {
		log.Println("WARNING: Authentication is disabled")
	} else if cfg.Auth.AccessKeyID != "" {
		log.Println("Authentication: AWS SigV4 enabled")
	} else {
		log.Println("Authentication: legacy token enabled")
	}

	if cfg.Telemetry.Tracing.Enabled {
		log.Printf("Tracing enabled: %s (sample rate: %.2f, insecure: %v)",
			cfg.Telemetry.Tracing.Endpoint, cfg.Telemetry.Tracing.SampleRate, cfg.Telemetry.Tracing.Insecure)
	}

	// --- Start server ---
	if err := httpServer.ListenAndServe(); err != http.ErrServerClosed {
		log.Fatalf("Server error: %v", err)
	}

	log.Println("Server stopped")
}
