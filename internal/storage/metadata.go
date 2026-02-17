// -------------------------------------------------------------------------------
// MetadataStore - Interface for Object Metadata Persistence
//
// Project: Munchbox / Author: Alex Freidah
//
// Defines the contract between the BackendManager and its metadata store.
// Implemented by Store (real PostgreSQL) and CircuitBreakerStore (degraded
// wrapper). Startup-only methods (RunMigrations, SyncQuotaLimits, Close) and
// CLI-only methods (ImportObject) live on the concrete Store, not the interface.
// -------------------------------------------------------------------------------

package storage

import (
	"context"
	"errors"
	"time"
)

// -------------------------------------------------------------------------
// SENTINEL ERRORS
// -------------------------------------------------------------------------

var (
	// ErrDBUnavailable is returned by CircuitBreakerStore when the circuit is
	// open (database is known to be down). The manager uses errors.Is checks
	// to trigger broadcast fallback (reads) or 503 rejection (writes).
	ErrDBUnavailable = errors.New("database unavailable")

	// ErrServiceUnavailable is returned to S3 clients when writes are rejected
	// during a database outage. The server layer's writeStorageError helper
	// translates this into the appropriate S3 XML error response.
	ErrServiceUnavailable = &S3Error{
		StatusCode: 503,
		Code:       "ServiceUnavailable",
		Message:    "database unavailable, writes are temporarily rejected",
	}
)

// -------------------------------------------------------------------------
// INTERFACE
// -------------------------------------------------------------------------

// MetadataStore defines the contract for object metadata and quota persistence.
// All methods called by BackendManager at request time or in background tasks
// are included. Startup-only operations remain on the concrete Store type.
type MetadataStore interface {
	// --- Object location operations ---
	GetAllObjectLocations(ctx context.Context, key string) ([]ObjectLocation, error)
	RecordObject(ctx context.Context, key, backend string, size int64) error
	DeleteObject(ctx context.Context, key string) ([]DeletedCopy, error)
	ListObjects(ctx context.Context, prefix, startAfter string, maxKeys int) (*ListObjectsResult, error)

	// --- Quota operations ---
	GetBackendWithSpace(ctx context.Context, size int64, backendOrder []string) (string, error)

	// --- Multipart operations ---
	CreateMultipartUpload(ctx context.Context, uploadID, key, backend, contentType string) error
	GetMultipartUpload(ctx context.Context, uploadID string) (*MultipartUpload, error)
	RecordPart(ctx context.Context, uploadID string, partNumber int, etag string, size int64) error
	GetParts(ctx context.Context, uploadID string) ([]MultipartPart, error)
	DeleteMultipartUpload(ctx context.Context, uploadID string) error

	// --- Background operations (metrics, cleanup, rebalance, replication) ---
	GetQuotaStats(ctx context.Context) (map[string]QuotaStat, error)
	GetObjectCounts(ctx context.Context) (map[string]int64, error)
	GetActiveMultipartCounts(ctx context.Context) (map[string]int64, error)
	GetStaleMultipartUploads(ctx context.Context, olderThan time.Duration) ([]MultipartUpload, error)
	ListObjectsByBackend(ctx context.Context, backendName string, limit int) ([]ObjectLocation, error)
	MoveObjectLocation(ctx context.Context, key, fromBackend, toBackend string) (int64, error)
	GetUnderReplicatedObjects(ctx context.Context, factor, limit int) ([]ObjectLocation, error)
	RecordReplica(ctx context.Context, key, targetBackend, sourceBackend string, size int64) (bool, error)
}

// Compile-time check: *Store satisfies MetadataStore.
var _ MetadataStore = (*Store)(nil)
