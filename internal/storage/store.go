// -------------------------------------------------------------------------------
// Store - PostgreSQL Quota and Object Location Storage
//
// Author: Alex Freidah
//
// Manages quota tracking and object location storage in PostgreSQL. Tracks which
// backend stores each object and how much quota each backend has used. Provides
// atomic operations to ensure quota limits are respected.
// -------------------------------------------------------------------------------

package storage

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/afreidah/s3-proxy/internal/config"
	db "github.com/afreidah/s3-proxy/internal/storage/sqlc"
)

//go:embed migration.sql
var migrationSQL string

// -------------------------------------------------------------------------
// ERRORS
// -------------------------------------------------------------------------

// S3Error is a structured error that carries an HTTP status code and S3 error
// code, allowing the server layer to translate storage errors into S3 XML
// responses without per-handler error mapping.
type S3Error struct {
	StatusCode int    // HTTP status code (e.g. 404, 507)
	Code       string // S3 error code (e.g. "NoSuchKey")
	Message    string // Human-readable message
}

func (e *S3Error) Error() string {
	return e.Message
}

var (
	// ErrNoSpaceAvailable is an internal error used between store and manager.
	ErrNoSpaceAvailable = errors.New("no backend has sufficient quota")

	// ErrObjectNotFound is returned when an object is not in the location table.
	ErrObjectNotFound = &S3Error{StatusCode: 404, Code: "NoSuchKey", Message: "object not found"}

	// ErrMultipartUploadNotFound is returned when a multipart upload ID is not found.
	ErrMultipartUploadNotFound = &S3Error{StatusCode: 404, Code: "NoSuchUpload", Message: "multipart upload not found"}
)

// -------------------------------------------------------------------------
// TYPES
// -------------------------------------------------------------------------

// Store manages quota and object location data in PostgreSQL.
type Store struct {
	pool    *pgxpool.Pool
	queries *db.Queries
}

// QuotaStat holds quota statistics for a single backend.
type QuotaStat struct {
	BackendName string
	BytesUsed   int64
	BytesLimit  int64
	UpdatedAt   time.Time
}

// DeletedCopy holds information about a single deleted copy of an object.
type DeletedCopy struct {
	BackendName string
	SizeBytes   int64
}

// ObjectLocation holds information about where an object is stored.
type ObjectLocation struct {
	ObjectKey   string
	BackendName string
	SizeBytes   int64
	CreatedAt   time.Time
}

// -------------------------------------------------------------------------
// CONSTRUCTOR
// -------------------------------------------------------------------------

// NewStore creates a new PostgreSQL store connection using pgxpool.
func NewStore(ctx context.Context, dbCfg *config.DatabaseConfig) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(dbCfg.ConnectionString())
	if err != nil {
		return nil, fmt.Errorf("failed to parse connection string: %w", err)
	}

	cfg.MaxConns = dbCfg.MaxConns
	cfg.MinConns = dbCfg.MinConns
	cfg.MaxConnLifetime = dbCfg.MaxConnLifetime

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection pool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	return &Store{
		pool:    pool,
		queries: db.New(pool),
	}, nil
}

// Close closes the connection pool.
func (s *Store) Close() {
	s.pool.Close()
}

// RunMigrations applies the embedded schema DDL. All statements use IF NOT
// EXISTS so this is safe to call on every startup.
func (s *Store) RunMigrations(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, migrationSQL)
	if err != nil {
		return fmt.Errorf("failed to run migrations: %w", err)
	}
	return nil
}

// -------------------------------------------------------------------------
// TRANSACTION HELPERS
// -------------------------------------------------------------------------

// withTx executes fn within a transaction, committing on success or rolling
// back on error.
func (s *Store) withTx(ctx context.Context, fn func(*db.Queries) error) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if err := fn(s.queries.WithTx(tx)); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// withTxVal executes fn within a transaction and returns its result,
// committing on success or rolling back on error.
func withTxVal[T any](s *Store, ctx context.Context, fn func(*db.Queries) (T, error)) (T, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		var zero T
		return zero, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	val, err := fn(s.queries.WithTx(tx))
	if err != nil {
		var zero T
		return zero, err
	}
	if err := tx.Commit(ctx); err != nil {
		var zero T
		return zero, fmt.Errorf("failed to commit: %w", err)
	}
	return val, nil
}

// toObjectLocations converts sqlc ObjectLocation rows to storage ObjectLocations.
func toObjectLocations(rows []db.ObjectLocation) []ObjectLocation {
	out := make([]ObjectLocation, len(rows))
	for i, row := range rows {
		out[i] = ObjectLocation{
			ObjectKey:   row.ObjectKey,
			BackendName: row.BackendName,
			SizeBytes:   row.SizeBytes,
			CreatedAt:   row.CreatedAt.Time,
		}
	}
	return out
}

// -------------------------------------------------------------------------
// QUOTA OPERATIONS
// -------------------------------------------------------------------------

// SyncQuotaLimits ensures the backend_quotas table has entries for all configured
// backends with their quota limits. Creates new entries or updates existing limits.
func (s *Store) SyncQuotaLimits(ctx context.Context, backends []config.BackendConfig) error {
	for _, b := range backends {
		err := s.queries.UpsertQuotaLimit(ctx, db.UpsertQuotaLimitParams{
			BackendName: b.Name,
			BytesLimit:  b.QuotaBytes,
		})
		if err != nil {
			return fmt.Errorf("failed to sync quota for backend %s: %w", b.Name, err)
		}
	}
	return nil
}

// GetBackendWithSpace finds a backend with enough quota for the given size.
// Returns the backend name or ErrNoSpaceAvailable if none have enough space.
func (s *Store) GetBackendWithSpace(ctx context.Context, size int64, backendOrder []string) (string, error) {
	for _, name := range backendOrder {
		available, err := s.queries.GetBackendAvailableSpace(ctx, name)
		if errors.Is(err, pgx.ErrNoRows) {
			continue
		}
		if err != nil {
			return "", fmt.Errorf("failed to check quota for %s: %w", name, err)
		}

		if available >= size {
			return name, nil
		}
	}

	return "", ErrNoSpaceAvailable
}

// GetQuotaStats returns quota statistics for all backends.
func (s *Store) GetQuotaStats(ctx context.Context) (map[string]QuotaStat, error) {
	rows, err := s.queries.GetAllQuotaStats(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to query quota stats: %w", err)
	}

	stats := make(map[string]QuotaStat)
	for _, row := range rows {
		stats[row.BackendName] = QuotaStat{
			BackendName: row.BackendName,
			BytesUsed:   row.BytesUsed,
			BytesLimit:  row.BytesLimit,
			UpdatedAt:   row.UpdatedAt.Time,
		}
	}

	return stats, nil
}

// GetObjectCounts returns the number of objects stored on each backend.
func (s *Store) GetObjectCounts(ctx context.Context) (map[string]int64, error) {
	rows, err := s.queries.GetObjectCountsByBackend(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to query object counts: %w", err)
	}

	counts := make(map[string]int64)
	for _, row := range rows {
		counts[row.BackendName] = row.ObjectCount
	}
	return counts, nil
}

// GetActiveMultipartCounts returns the number of in-progress multipart uploads
// per backend.
func (s *Store) GetActiveMultipartCounts(ctx context.Context) (map[string]int64, error) {
	rows, err := s.queries.GetActiveMultipartCountsByBackend(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to query multipart counts: %w", err)
	}

	counts := make(map[string]int64)
	for _, row := range rows {
		counts[row.BackendName] = row.UploadCount
	}
	return counts, nil
}

// -------------------------------------------------------------------------
// OBJECT LOCATION OPERATIONS
// -------------------------------------------------------------------------

// RecordObject records an object's location and updates the backend quota.
// On overwrite, all existing copies (including replicas) are removed and their
// quotas decremented before inserting the new primary copy.
func (s *Store) RecordObject(ctx context.Context, key, backend string, size int64) error {
	return s.withTx(ctx, func(qtx *db.Queries) error {
		// --- Collect all existing copies for this key ---
		existing, err := qtx.GetExistingCopiesForUpdate(ctx, key)
		if err != nil {
			return fmt.Errorf("failed to query existing copies: %w", err)
		}

		// --- Delete all existing copies and decrement their quotas ---
		if len(existing) > 0 {
			if err := qtx.DeleteObjectCopies(ctx, key); err != nil {
				return fmt.Errorf("failed to delete existing copies: %w", err)
			}

			for _, ec := range existing {
				if err := qtx.DecrementQuota(ctx, db.DecrementQuotaParams{
					Amount:      ec.SizeBytes,
					BackendName: ec.BackendName,
				}); err != nil {
					return fmt.Errorf("failed to decrement quota for %s: %w", ec.BackendName, err)
				}
			}
		}

		// --- Insert new primary copy ---
		if err := qtx.InsertObjectLocation(ctx, db.InsertObjectLocationParams{
			ObjectKey:   key,
			BackendName: backend,
			SizeBytes:   size,
		}); err != nil {
			return fmt.Errorf("failed to insert object location: %w", err)
		}

		// --- Increment quota for new backend ---
		if err := qtx.IncrementQuota(ctx, db.IncrementQuotaParams{
			Amount:      size,
			BackendName: backend,
		}); err != nil {
			return fmt.Errorf("failed to update quota: %w", err)
		}

		return nil
	})
}

// DeleteObject removes all copies of an object and decrements their quotas.
// Returns all deleted copies, or ErrObjectNotFound if the object doesn't exist.
func (s *Store) DeleteObject(ctx context.Context, key string) ([]DeletedCopy, error) {
	return withTxVal(s, ctx, func(qtx *db.Queries) ([]DeletedCopy, error) {
		// --- Get all copies ---
		existing, err := qtx.GetExistingCopiesForUpdate(ctx, key)
		if err != nil {
			return nil, fmt.Errorf("failed to get object locations: %w", err)
		}

		if len(existing) == 0 {
			return nil, ErrObjectNotFound
		}

		// --- Delete all location records ---
		if err := qtx.DeleteObjectCopies(ctx, key); err != nil {
			return nil, fmt.Errorf("failed to delete object locations: %w", err)
		}

		// --- Decrement quota for each backend ---
		copies := make([]DeletedCopy, len(existing))
		for i, ec := range existing {
			copies[i] = DeletedCopy{
				BackendName: ec.BackendName,
				SizeBytes:   ec.SizeBytes,
			}
			if err := qtx.DecrementQuota(ctx, db.DecrementQuotaParams{
				Amount:      ec.SizeBytes,
				BackendName: ec.BackendName,
			}); err != nil {
				return nil, fmt.Errorf("failed to decrement quota for %s: %w", ec.BackendName, err)
			}
		}

		return copies, nil
	})
}

// ListObjectsByBackend returns objects stored on a specific backend, ordered by
// size ascending (smallest first). Used by the rebalancer to find movable objects.
func (s *Store) ListObjectsByBackend(ctx context.Context, backendName string, limit int) ([]ObjectLocation, error) {
	rows, err := s.queries.ListObjectsByBackend(ctx, db.ListObjectsByBackendParams{
		BackendName: backendName,
		Limit:       int32(limit),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list objects by backend: %w", err)
	}
	return toObjectLocations(rows), nil
}

// MoveObjectLocation atomically moves a copy of an object from one backend to
// another. Uses SELECT FOR UPDATE to prevent races. Returns (0, nil) if the
// source copy is gone or the target already has a copy.
func (s *Store) MoveObjectLocation(ctx context.Context, key, fromBackend, toBackend string) (int64, error) {
	return withTxVal(s, ctx, func(qtx *db.Queries) (int64, error) {
		// --- Check if target backend already has a copy ---
		exists, err := qtx.CheckObjectExistsOnBackend(ctx, db.CheckObjectExistsOnBackendParams{
			ObjectKey:   key,
			BackendName: toBackend,
		})
		if err != nil {
			return 0, fmt.Errorf("failed to check target: %w", err)
		}
		if exists {
			return 0, nil
		}

		// --- Lock the source row and verify it still belongs to the source ---
		sizeBytes, err := qtx.LockObjectOnBackend(ctx, db.LockObjectOnBackendParams{
			ObjectKey:   key,
			BackendName: fromBackend,
		})
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, nil
		}
		if err != nil {
			return 0, fmt.Errorf("failed to lock object: %w", err)
		}

		// --- Delete source row ---
		if err := qtx.DeleteObjectFromBackend(ctx, db.DeleteObjectFromBackendParams{
			ObjectKey:   key,
			BackendName: fromBackend,
		}); err != nil {
			return 0, fmt.Errorf("failed to delete source location: %w", err)
		}

		// --- Insert destination row ---
		if err := qtx.InsertObjectLocation(ctx, db.InsertObjectLocationParams{
			ObjectKey:   key,
			BackendName: toBackend,
			SizeBytes:   sizeBytes,
		}); err != nil {
			return 0, fmt.Errorf("failed to insert destination location: %w", err)
		}

		// --- Decrement source quota ---
		if err := qtx.DecrementQuota(ctx, db.DecrementQuotaParams{
			Amount:      sizeBytes,
			BackendName: fromBackend,
		}); err != nil {
			return 0, fmt.Errorf("failed to decrement source quota: %w", err)
		}

		// --- Increment destination quota ---
		if err := qtx.IncrementQuota(ctx, db.IncrementQuotaParams{
			Amount:      sizeBytes,
			BackendName: toBackend,
		}); err != nil {
			return 0, fmt.Errorf("failed to increment destination quota: %w", err)
		}

		return sizeBytes, nil
	})
}

// ListObjectsResult holds the result of a list objects query.
type ListObjectsResult struct {
	Objects               []ObjectLocation
	IsTruncated           bool
	NextContinuationToken string
}

// ListObjects returns objects matching the given prefix, sorted by key.
// Supports pagination via startAfter and maxKeys. Returns one extra row to
// detect truncation.
func (s *Store) ListObjects(ctx context.Context, prefix, startAfter string, maxKeys int) (*ListObjectsResult, error) {
	if maxKeys <= 0 {
		maxKeys = 1000
	}

	// --- Escape LIKE wildcards in prefix ---
	escapedPrefix := strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`).Replace(prefix)

	// Fetch one extra to detect truncation
	rows, err := s.queries.ListObjectsByPrefix(ctx, db.ListObjectsByPrefixParams{
		Prefix:     escapedPrefix,
		StartAfter: startAfter,
		MaxKeys:    int32(maxKeys + 1),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list objects: %w", err)
	}

	objects := toObjectLocations(rows)

	result := &ListObjectsResult{}
	if len(objects) > maxKeys {
		result.IsTruncated = true
		result.NextContinuationToken = objects[maxKeys-1].ObjectKey
		result.Objects = objects[:maxKeys]
	} else {
		result.Objects = objects
	}

	return result, nil
}

// -------------------------------------------------------------------------
// REPLICATION OPERATIONS
// -------------------------------------------------------------------------

// GetAllObjectLocations returns all copies of an object, ordered by created_at
// ascending (oldest/primary first). Used for read failover.
func (s *Store) GetAllObjectLocations(ctx context.Context, key string) ([]ObjectLocation, error) {
	rows, err := s.queries.GetAllObjectLocations(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("failed to get object locations: %w", err)
	}

	if len(rows) == 0 {
		return nil, ErrObjectNotFound
	}

	return toObjectLocations(rows), nil
}

// GetUnderReplicatedObjects finds objects with fewer copies than the target
// replication factor. Returns all rows for those objects so callers know which
// backends already have copies.
func (s *Store) GetUnderReplicatedObjects(ctx context.Context, factor, limit int) ([]ObjectLocation, error) {
	rows, err := s.queries.GetUnderReplicatedObjects(ctx, db.GetUnderReplicatedObjectsParams{
		Factor:  int64(factor),
		MaxKeys: int32(limit),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to query under-replicated objects: %w", err)
	}

	return toObjectLocations(rows), nil
}

// RecordReplica inserts a replica copy of an object, but only if the source
// copy still exists. This prevents stale replicas when an object is overwritten
// or deleted during the (potentially slow) replication copy. Returns true if
// the replica was inserted, false if skipped.
func (s *Store) RecordReplica(ctx context.Context, key, targetBackend, sourceBackend string, size int64) (bool, error) {
	return withTxVal(s, ctx, func(qtx *db.Queries) (bool, error) {
		// --- Conditional insert: only if source copy still exists ---
		inserted, err := qtx.InsertReplicaConditional(ctx, db.InsertReplicaConditionalParams{
			ObjectKey:     key,
			BackendName:   targetBackend,
			BackendName_2: sourceBackend,
		})
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		if err != nil {
			return false, fmt.Errorf("failed to insert replica: %w", err)
		}

		if !inserted {
			return false, nil
		}

		// --- Increment quota for target backend ---
		if err := qtx.IncrementQuota(ctx, db.IncrementQuotaParams{
			Amount:      size,
			BackendName: targetBackend,
		}); err != nil {
			return false, fmt.Errorf("failed to update quota: %w", err)
		}

		return true, nil
	})
}

// -------------------------------------------------------------------------
// MULTIPART UPLOAD OPERATIONS
// -------------------------------------------------------------------------

// MultipartUpload holds metadata for an in-progress multipart upload.
type MultipartUpload struct {
	UploadID    string
	ObjectKey   string
	BackendName string
	ContentType string
	CreatedAt   time.Time
}

// MultipartPart holds metadata for a single uploaded part.
type MultipartPart struct {
	PartNumber int
	ETag       string
	SizeBytes  int64
	CreatedAt  time.Time
}

// CreateMultipartUpload records a new multipart upload in the database.
func (s *Store) CreateMultipartUpload(ctx context.Context, uploadID, key, backend, contentType string) error {
	err := s.queries.CreateMultipartUpload(ctx, db.CreateMultipartUploadParams{
		UploadID:    uploadID,
		ObjectKey:   key,
		BackendName: backend,
		ContentType: &contentType,
	})
	if err != nil {
		return fmt.Errorf("failed to create multipart upload: %w", err)
	}
	return nil
}

// GetMultipartUpload retrieves metadata for a multipart upload.
func (s *Store) GetMultipartUpload(ctx context.Context, uploadID string) (*MultipartUpload, error) {
	row, err := s.queries.GetMultipartUpload(ctx, uploadID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrMultipartUploadNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get multipart upload: %w", err)
	}

	ct := ""
	if row.ContentType != nil {
		ct = *row.ContentType
	}

	return &MultipartUpload{
		UploadID:    row.UploadID,
		ObjectKey:   row.ObjectKey,
		BackendName: row.BackendName,
		ContentType: ct,
		CreatedAt:   row.CreatedAt.Time,
	}, nil
}

// RecordPart records a completed part for a multipart upload.
func (s *Store) RecordPart(ctx context.Context, uploadID string, partNumber int, etag string, size int64) error {
	err := s.queries.UpsertPart(ctx, db.UpsertPartParams{
		UploadID:   uploadID,
		PartNumber: int32(partNumber),
		Etag:       etag,
		SizeBytes:  size,
	})
	if err != nil {
		return fmt.Errorf("failed to record part: %w", err)
	}
	return nil
}

// GetParts returns all parts for a multipart upload, ordered by part number.
func (s *Store) GetParts(ctx context.Context, uploadID string) ([]MultipartPart, error) {
	rows, err := s.queries.GetParts(ctx, uploadID)
	if err != nil {
		return nil, fmt.Errorf("failed to get parts: %w", err)
	}

	parts := make([]MultipartPart, len(rows))
	for i, row := range rows {
		parts[i] = MultipartPart{
			PartNumber: int(row.PartNumber),
			ETag:       row.Etag,
			SizeBytes:  row.SizeBytes,
			CreatedAt:  row.CreatedAt.Time,
		}
	}
	return parts, nil
}

// DeleteMultipartUpload removes a multipart upload and its parts (cascading).
func (s *Store) DeleteMultipartUpload(ctx context.Context, uploadID string) error {
	err := s.queries.DeleteMultipartUpload(ctx, uploadID)
	if err != nil {
		return fmt.Errorf("failed to delete multipart upload: %w", err)
	}
	return nil
}

// GetStaleMultipartUploads returns uploads older than the given duration.
func (s *Store) GetStaleMultipartUploads(ctx context.Context, olderThan time.Duration) ([]MultipartUpload, error) {
	cutoff := time.Now().Add(-olderThan)
	rows, err := s.queries.GetStaleMultipartUploads(ctx, pgTimestamptz(cutoff))
	if err != nil {
		return nil, fmt.Errorf("failed to get stale uploads: %w", err)
	}

	uploads := make([]MultipartUpload, len(rows))
	for i, row := range rows {
		ct := ""
		if row.ContentType != nil {
			ct = *row.ContentType
		}
		uploads[i] = MultipartUpload{
			UploadID:    row.UploadID,
			ObjectKey:   row.ObjectKey,
			BackendName: row.BackendName,
			ContentType: ct,
			CreatedAt:   row.CreatedAt.Time,
		}
	}
	return uploads, nil
}

// -------------------------------------------------------------------------
// SYNC OPERATIONS
// -------------------------------------------------------------------------

// ImportObject records a pre-existing object in the database without overwriting.
// Returns true if the object was imported, false if it already existed for this
// backend. Used by the sync subcommand to bring existing bucket objects under
// proxy management.
func (s *Store) ImportObject(ctx context.Context, key, backend string, size int64) (bool, error) {
	return withTxVal(s, ctx, func(qtx *db.Queries) (bool, error) {
		inserted, err := qtx.InsertObjectLocationIfNotExists(ctx, db.InsertObjectLocationIfNotExistsParams{
			ObjectKey:   key,
			BackendName: backend,
			SizeBytes:   size,
		})
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		if err != nil {
			return false, fmt.Errorf("failed to import object %s: %w", key, err)
		}

		if !inserted {
			return false, nil
		}

		if err := qtx.IncrementQuota(ctx, db.IncrementQuotaParams{
			Amount:      size,
			BackendName: backend,
		}); err != nil {
			return false, fmt.Errorf("failed to increment quota for %s: %w", backend, err)
		}

		return true, nil
	})
}

// -------------------------------------------------------------------------
// USAGE TRACKING
// -------------------------------------------------------------------------

// FlushUsageDeltas atomically adds accumulated usage deltas to the persistent
// usage row. Creates the row if it doesn't exist for this (backend, period).
func (s *Store) FlushUsageDeltas(ctx context.Context, backendName, period string, apiRequests, egressBytes, ingressBytes int64) error {
	return s.queries.FlushUsageDeltas(ctx, db.FlushUsageDeltasParams{
		BackendName:  backendName,
		Period:       period,
		ApiRequests:  apiRequests,
		EgressBytes:  egressBytes,
		IngressBytes: ingressBytes,
	})
}

// GetUsageForPeriod returns usage statistics for all backends in the given period.
func (s *Store) GetUsageForPeriod(ctx context.Context, period string) (map[string]UsageStat, error) {
	rows, err := s.queries.GetUsageForPeriod(ctx, period)
	if err != nil {
		return nil, err
	}

	stats := make(map[string]UsageStat, len(rows))
	for _, row := range rows {
		stats[row.BackendName] = UsageStat{
			ApiRequests:  row.ApiRequests,
			EgressBytes:  row.EgressBytes,
			IngressBytes: row.IngressBytes,
		}
	}
	return stats, nil
}

// pgTimestamptz converts a time.Time to pgtype.Timestamptz for use with sqlc.
func pgTimestamptz(t time.Time) pgtype.Timestamptz {
	return pgtype.Timestamptz{Time: t, Valid: true}
}
