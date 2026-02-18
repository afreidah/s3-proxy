//go:build integration

package integration

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/afreidah/s3-orchestrator/internal/config"
	"github.com/afreidah/s3-orchestrator/internal/server"
	"github.com/afreidah/s3-orchestrator/internal/storage"
)

const virtualBucket = "test-bucket"

var (
	proxyAddr        string
	testDB           *sql.DB
	testManager      *storage.BackendManager
	testStore        *storage.Store
	testFailableStore *FailableStore
	testCBStore      *storage.CircuitBreakerStore
)

func TestMain(m *testing.M) {
	// Silence the proxy's request logger so test output is clean.
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))

	minio1Endpoint := envOrDefault("MINIO1_ENDPOINT", "http://localhost:19000")
	minio2Endpoint := envOrDefault("MINIO2_ENDPOINT", "http://localhost:19002")
	pgHost := envOrDefault("POSTGRES_HOST", "localhost")
	pgPort := envOrDefault("POSTGRES_PORT", "15432")

	port, err := strconv.Atoi(pgPort)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid POSTGRES_PORT %q: %v\n", pgPort, err)
		os.Exit(1)
	}

	cfg := &config.Config{
		Server: config.ServerConfig{
			ListenAddr:    "127.0.0.1:0",
			VirtualBucket: virtualBucket,
		},
		Database: config.DatabaseConfig{
			Host:     pgHost,
			Port:     port,
			Database: "s3proxy_test",
			User:     "s3proxy",
			Password: "s3proxy",
			SSLMode:  "disable",
		},
		CircuitBreaker: config.CircuitBreakerConfig{
			FailureThreshold: 3,
			OpenTimeout:      500 * time.Millisecond,
			CacheTTL:         60 * time.Second,
		},
		Backends: []config.BackendConfig{
			{
				Name:            "minio-1",
				Endpoint:        minio1Endpoint,
				Region:          "us-east-1",
				Bucket:          "backend1",
				AccessKeyID:     "minioadmin",
				SecretAccessKey: "minioadmin",
				ForcePathStyle:  true,
				QuotaBytes:      1024,
			},
			{
				Name:            "minio-2",
				Endpoint:        minio2Endpoint,
				Region:          "us-east-1",
				Bucket:          "backend2",
				AccessKeyID:     "minioadmin",
				SecretAccessKey: "minioadmin",
				ForcePathStyle:  true,
				QuotaBytes:      2048,
			},
		},
	}

	if err := cfg.SetDefaultsAndValidate(); err != nil {
		fmt.Fprintf(os.Stderr, "config validation failed: %v\n", err)
		os.Exit(1)
	}

	ctx := context.Background()

	store, err := storage.NewStore(ctx, &cfg.Database)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create store: %v\n", err)
		os.Exit(1)
	}

	if err := store.RunMigrations(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "failed to run migrations: %v\n", err)
		os.Exit(1)
	}

	if err := store.SyncQuotaLimits(ctx, cfg.Backends); err != nil {
		fmt.Fprintf(os.Stderr, "failed to sync quota limits: %v\n", err)
		os.Exit(1)
	}

	backends := make(map[string]storage.ObjectBackend)
	var backendOrder []string
	for _, bcfg := range cfg.Backends {
		b, err := storage.NewS3Backend(&bcfg)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to create backend %s: %v\n", bcfg.Name, err)
			os.Exit(1)
		}
		backends[bcfg.Name] = b
		backendOrder = append(backendOrder, bcfg.Name)
	}

	testStore = store

	// Wire: store → FailableStore → CircuitBreakerStore → manager
	failableStore := &FailableStore{MetadataStore: store}
	testFailableStore = failableStore

	cbStore := storage.NewCircuitBreakerStore(failableStore, cfg.CircuitBreaker)
	testCBStore = cbStore

	manager := storage.NewBackendManager(backends, cbStore, backendOrder, 60*time.Second, 30*time.Second, nil)
	testManager = manager

	srv := &server.Server{
		Manager:       manager,
		VirtualBucket: virtualBucket,
		AuthConfig:    cfg.Auth,
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to listen: %v\n", err)
		os.Exit(1)
	}
	proxyAddr = listener.Addr().String()

	httpServer := &http.Server{
		Handler:      srv,
		ReadTimeout:  5 * time.Minute,
		WriteTimeout: 5 * time.Minute,
	}
	go httpServer.Serve(listener)

	connStr := cfg.Database.ConnectionString()
	testDB, err = sql.Open("pgx", connStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to open test db: %v\n", err)
		os.Exit(1)
	}

	code := m.Run()

	httpServer.Shutdown(ctx)
	testDB.Close()
	store.Close()

	os.Exit(code)
}

// newS3Client returns an AWS SDK v2 S3 client pointed at the in-process proxy.
func newS3Client(t *testing.T) *s3.Client {
	t.Helper()
	return s3.New(s3.Options{
		BaseEndpoint: aws.String("http://" + proxyAddr),
		Region:       "us-east-1",
		Credentials:  credentials.NewStaticCredentialsProvider("test", "test", ""),
		UsePathStyle: true,
	})
}

// queryObjectBackend returns which backend stores the given object key.
func queryObjectBackend(t *testing.T, key string) string {
	t.Helper()
	var backendName string
	err := testDB.QueryRow("SELECT backend_name FROM object_locations WHERE object_key = $1", key).Scan(&backendName)
	if err != nil {
		t.Fatalf("queryObjectBackend(%q): %v", key, err)
	}
	return backendName
}

// queryQuotaUsed returns the bytes_used value for a backend.
func queryQuotaUsed(t *testing.T, backendName string) int64 {
	t.Helper()
	var bytesUsed int64
	err := testDB.QueryRow("SELECT bytes_used FROM backend_quotas WHERE backend_name = $1", backendName).Scan(&bytesUsed)
	if err != nil {
		t.Fatalf("queryQuotaUsed(%q): %v", backendName, err)
	}
	return bytesUsed
}

// resetState truncates all object/multipart tables and resets quota counters.
func resetState(t *testing.T) {
	t.Helper()
	for _, q := range []string{
		"DELETE FROM multipart_parts",
		"DELETE FROM multipart_uploads",
		"DELETE FROM object_locations",
		"UPDATE backend_quotas SET bytes_used = 0, updated_at = NOW()",
	} {
		if _, err := testDB.Exec(q); err != nil {
			t.Fatalf("resetState: %v", err)
		}
	}
	testManager.ClearCache()
}

// uniqueKey generates a collision-free object key.
func uniqueKey(t *testing.T, prefix string) string {
	t.Helper()
	return fmt.Sprintf("%s/%s-%d", prefix, t.Name(), time.Now().UnixNano())
}

// queryObjectCopies returns the number of copies (rows) for the given object key.
func queryObjectCopies(t *testing.T, key string) int {
	t.Helper()
	var count int
	err := testDB.QueryRow(
		"SELECT COUNT(*) FROM object_locations WHERE object_key = $1", key,
	).Scan(&count)
	if err != nil {
		t.Fatalf("queryObjectCopies(%q): %v", key, err)
	}
	return count
}

// queryObjectBackends returns all backend names storing copies of the given key.
func queryObjectBackends(t *testing.T, key string) []string {
	t.Helper()
	rows, err := testDB.Query(
		"SELECT backend_name FROM object_locations WHERE object_key = $1 ORDER BY created_at ASC", key,
	)
	if err != nil {
		t.Fatalf("queryObjectBackends(%q): %v", key, err)
	}
	defer rows.Close()

	var backends []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("queryObjectBackends scan: %v", err)
		}
		backends = append(backends, name)
	}
	return backends
}

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// -------------------------------------------------------------------------
// FailableStore — injectable failure wrapper for circuit breaker tests
// -------------------------------------------------------------------------

// errSimulatedDBFailure simulates a database connection error.
var errSimulatedDBFailure = errors.New("simulated database connection failure")

// FailableStore wraps a MetadataStore and can be toggled to return connection
// errors, simulating a database outage for circuit breaker integration tests.
type FailableStore struct {
	storage.MetadataStore
	mu      sync.Mutex
	failing bool
}

func (f *FailableStore) SetFailing(v bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failing = v
}

func (f *FailableStore) isFailing() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.failing
}

func (f *FailableStore) GetAllObjectLocations(ctx context.Context, key string) ([]storage.ObjectLocation, error) {
	if f.isFailing() {
		return nil, errSimulatedDBFailure
	}
	return f.MetadataStore.GetAllObjectLocations(ctx, key)
}

func (f *FailableStore) RecordObject(ctx context.Context, key, backend string, size int64) error {
	if f.isFailing() {
		return errSimulatedDBFailure
	}
	return f.MetadataStore.RecordObject(ctx, key, backend, size)
}

func (f *FailableStore) DeleteObject(ctx context.Context, key string) ([]storage.DeletedCopy, error) {
	if f.isFailing() {
		return nil, errSimulatedDBFailure
	}
	return f.MetadataStore.DeleteObject(ctx, key)
}

func (f *FailableStore) ListObjects(ctx context.Context, prefix, startAfter string, maxKeys int) (*storage.ListObjectsResult, error) {
	if f.isFailing() {
		return nil, errSimulatedDBFailure
	}
	return f.MetadataStore.ListObjects(ctx, prefix, startAfter, maxKeys)
}

func (f *FailableStore) GetBackendWithSpace(ctx context.Context, size int64, backendOrder []string) (string, error) {
	if f.isFailing() {
		return "", errSimulatedDBFailure
	}
	return f.MetadataStore.GetBackendWithSpace(ctx, size, backendOrder)
}

func (f *FailableStore) CreateMultipartUpload(ctx context.Context, uploadID, key, backend, contentType string) error {
	if f.isFailing() {
		return errSimulatedDBFailure
	}
	return f.MetadataStore.CreateMultipartUpload(ctx, uploadID, key, backend, contentType)
}

func (f *FailableStore) GetMultipartUpload(ctx context.Context, uploadID string) (*storage.MultipartUpload, error) {
	if f.isFailing() {
		return nil, errSimulatedDBFailure
	}
	return f.MetadataStore.GetMultipartUpload(ctx, uploadID)
}

func (f *FailableStore) RecordPart(ctx context.Context, uploadID string, partNumber int, etag string, size int64) error {
	if f.isFailing() {
		return errSimulatedDBFailure
	}
	return f.MetadataStore.RecordPart(ctx, uploadID, partNumber, etag, size)
}

func (f *FailableStore) GetParts(ctx context.Context, uploadID string) ([]storage.MultipartPart, error) {
	if f.isFailing() {
		return nil, errSimulatedDBFailure
	}
	return f.MetadataStore.GetParts(ctx, uploadID)
}

func (f *FailableStore) DeleteMultipartUpload(ctx context.Context, uploadID string) error {
	if f.isFailing() {
		return errSimulatedDBFailure
	}
	return f.MetadataStore.DeleteMultipartUpload(ctx, uploadID)
}

func (f *FailableStore) GetQuotaStats(ctx context.Context) (map[string]storage.QuotaStat, error) {
	if f.isFailing() {
		return nil, errSimulatedDBFailure
	}
	return f.MetadataStore.GetQuotaStats(ctx)
}

func (f *FailableStore) GetObjectCounts(ctx context.Context) (map[string]int64, error) {
	if f.isFailing() {
		return nil, errSimulatedDBFailure
	}
	return f.MetadataStore.GetObjectCounts(ctx)
}

func (f *FailableStore) GetActiveMultipartCounts(ctx context.Context) (map[string]int64, error) {
	if f.isFailing() {
		return nil, errSimulatedDBFailure
	}
	return f.MetadataStore.GetActiveMultipartCounts(ctx)
}

func (f *FailableStore) GetStaleMultipartUploads(ctx context.Context, olderThan time.Duration) ([]storage.MultipartUpload, error) {
	if f.isFailing() {
		return nil, errSimulatedDBFailure
	}
	return f.MetadataStore.GetStaleMultipartUploads(ctx, olderThan)
}

func (f *FailableStore) ListObjectsByBackend(ctx context.Context, backendName string, limit int) ([]storage.ObjectLocation, error) {
	if f.isFailing() {
		return nil, errSimulatedDBFailure
	}
	return f.MetadataStore.ListObjectsByBackend(ctx, backendName, limit)
}

func (f *FailableStore) MoveObjectLocation(ctx context.Context, key, fromBackend, toBackend string) (int64, error) {
	if f.isFailing() {
		return 0, errSimulatedDBFailure
	}
	return f.MetadataStore.MoveObjectLocation(ctx, key, fromBackend, toBackend)
}

func (f *FailableStore) GetUnderReplicatedObjects(ctx context.Context, factor, limit int) ([]storage.ObjectLocation, error) {
	if f.isFailing() {
		return nil, errSimulatedDBFailure
	}
	return f.MetadataStore.GetUnderReplicatedObjects(ctx, factor, limit)
}

func (f *FailableStore) RecordReplica(ctx context.Context, key, targetBackend, sourceBackend string, size int64) (bool, error) {
	if f.isFailing() {
		return false, errSimulatedDBFailure
	}
	return f.MetadataStore.RecordReplica(ctx, key, targetBackend, sourceBackend, size)
}

// tripCircuitBreaker makes enough failing requests to trip the circuit breaker open.
func tripCircuitBreaker(t *testing.T) {
	t.Helper()
	client := newS3Client(t)
	ctx := context.Background()
	// The default failure threshold is 3 — make enough failing requests
	for i := 0; i < 5; i++ {
		client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(virtualBucket),
			Key:    aws.String(fmt.Sprintf("trip-circuit-%d", i)),
		})
	}
}

// waitForRecovery waits for the circuit to probe and close after the open timeout.
// Polls until the circuit is healthy or the timeout expires.
func waitForRecovery(t *testing.T) {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatal("circuit breaker did not recover within 5s")
			return
		default:
			// Wait at least the open timeout (500ms) before probing
			time.Sleep(600 * time.Millisecond)
			// Make a request to trigger the half-open probe
			client := newS3Client(t)
			client.HeadObject(context.Background(), &s3.HeadObjectInput{
				Bucket: aws.String(virtualBucket),
				Key:    aws.String("probe-recovery"),
			})
			if testCBStore.IsHealthy() {
				return
			}
		}
	}
}

// newTestS3Backend creates an S3Backend for a test MinIO instance, avoiding
// duplicate endpoint/credential wiring across tests.
func newTestS3Backend(t *testing.T, name string) *storage.S3Backend {
	t.Helper()

	cfgs := map[string]config.BackendConfig{
		"minio-1": {
			Name:            "minio-1",
			Endpoint:        envOrDefault("MINIO1_ENDPOINT", "http://localhost:19000"),
			Region:          "us-east-1",
			Bucket:          "backend1",
			AccessKeyID:     "minioadmin",
			SecretAccessKey: "minioadmin",
			ForcePathStyle:  true,
		},
		"minio-2": {
			Name:            "minio-2",
			Endpoint:        envOrDefault("MINIO2_ENDPOINT", "http://localhost:19002"),
			Region:          "us-east-1",
			Bucket:          "backend2",
			AccessKeyID:     "minioadmin",
			SecretAccessKey: "minioadmin",
			ForcePathStyle:  true,
		},
	}

	cfg, ok := cfgs[name]
	if !ok {
		t.Fatalf("unknown backend %q", name)
	}

	backend, err := storage.NewS3Backend(&cfg)
	if err != nil {
		t.Fatalf("NewS3Backend(%s): %v", name, err)
	}
	return backend
}
