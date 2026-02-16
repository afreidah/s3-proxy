// -------------------------------------------------------------------------------
// Manager - Multi-Backend Object Storage Manager
//
// Project: Munchbox / Author: Alex Freidah
//
// Manages multiple S3-compatible backends with quota-aware routing. Implements the
// ObjectBackend interface so it can be used as a drop-in replacement for a single
// backend. Automatically selects backends based on available quota and routes
// GET/HEAD/DELETE requests to the correct backend.
// -------------------------------------------------------------------------------

package storage

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/munchbox/s3-proxy/internal/telemetry"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

// -------------------------------------------------------------------------
// ERRORS
// -------------------------------------------------------------------------

var (
	// ErrInsufficientStorage is returned when no backend has enough quota.
	ErrInsufficientStorage = errors.New("insufficient storage: all backends at capacity")
)

// -------------------------------------------------------------------------
// BACKEND MANAGER
// -------------------------------------------------------------------------

// BackendManager manages multiple storage backends with quota tracking.
type BackendManager struct {
	backends map[string]*S3Backend // name -> backend
	store    *Store                // PostgreSQL store for quota/location
	order    []string              // backend selection order
}

// NewBackendManager creates a new backend manager with the given backends and store.
func NewBackendManager(backends map[string]*S3Backend, store *Store, order []string) *BackendManager {
	return &BackendManager{
		backends: backends,
		store:    store,
		order:    order,
	}
}

// Store returns the underlying store for direct access when needed (e.g.,
// listing parts for multipart uploads from the server layer).
func (m *BackendManager) Store() *Store {
	return m.store
}

// -------------------------------------------------------------------------
// OBJECT BACKEND INTERFACE IMPLEMENTATION
// -------------------------------------------------------------------------

// PutObject uploads an object to the first backend with available quota.
func (m *BackendManager) PutObject(ctx context.Context, key string, body io.Reader, size int64, contentType string) (string, error) {
	const operation = "PutObject"
	start := time.Now()

	// --- Start tracing span ---
	ctx, span := telemetry.StartSpan(ctx, "Manager "+operation,
		telemetry.AttrObjectKey.String(key),
		telemetry.AttrObjectSize.Int64(size),
	)
	defer span.End()

	// --- Find backend with available quota ---
	backendName, err := m.store.GetBackendWithSpace(ctx, size, m.order)
	if err != nil {
		if errors.Is(err, ErrNoSpaceAvailable) {
			span.SetStatus(codes.Error, "insufficient storage")
			span.SetAttributes(attribute.String("error.type", "quota_exceeded"))
			return "", ErrInsufficientStorage
		}
		span.SetStatus(codes.Error, err.Error())
		span.RecordError(err)
		return "", fmt.Errorf("failed to find backend: %w", err)
	}

	span.SetAttributes(telemetry.AttrBackendName.String(backendName))

	// --- Get the backend ---
	backend, ok := m.backends[backendName]
	if !ok {
		err := fmt.Errorf("backend %s not found", backendName)
		span.SetStatus(codes.Error, err.Error())
		return "", err
	}

	// --- Upload to backend ---
	etag, err := backend.PutObject(ctx, key, body, size, contentType)
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		span.RecordError(err)
		return "", err
	}

	// --- Record object location and update quota ---
	if err := m.store.RecordObject(ctx, key, backendName, size); err != nil {
		// Object was uploaded but we failed to record it
		// This is a critical error - log it but still return success since data is stored
		span.SetAttributes(attribute.Bool("quota.record_failed", true))
		span.RecordError(err)
		// TODO: Consider cleanup or retry logic
	}

	// --- Record metrics ---
	m.recordOperation(operation, backendName, start, nil)

	span.SetStatus(codes.Ok, "")
	return etag, nil
}

// GetObject retrieves an object from the backend where it's stored.
func (m *BackendManager) GetObject(ctx context.Context, key string) (io.ReadCloser, int64, string, string, error) {
	const operation = "GetObject"
	start := time.Now()

	// --- Start tracing span ---
	ctx, span := telemetry.StartSpan(ctx, "Manager "+operation,
		telemetry.AttrObjectKey.String(key),
	)
	defer span.End()

	// --- Find object location ---
	loc, err := m.store.GetObjectLocation(ctx, key)
	if err != nil {
		if errors.Is(err, ErrObjectNotFound) {
			span.SetStatus(codes.Error, "object not found")
			return nil, 0, "", "", err
		}
		span.SetStatus(codes.Error, err.Error())
		span.RecordError(err)
		return nil, 0, "", "", fmt.Errorf("failed to find object location: %w", err)
	}

	span.SetAttributes(telemetry.AttrBackendName.String(loc.BackendName))

	// --- Get the backend ---
	backend, ok := m.backends[loc.BackendName]
	if !ok {
		err := fmt.Errorf("backend %s not found", loc.BackendName)
		span.SetStatus(codes.Error, err.Error())
		return nil, 0, "", "", err
	}

	// --- Get from backend ---
	body, size, contentType, etag, err := backend.GetObject(ctx, key)
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		span.RecordError(err)
		return nil, 0, "", "", err
	}

	// --- Record metrics ---
	m.recordOperation(operation, loc.BackendName, start, nil)

	span.SetAttributes(telemetry.AttrObjectSize.Int64(size))
	span.SetStatus(codes.Ok, "")
	return body, size, contentType, etag, nil
}

// HeadObject retrieves object metadata from the backend where it's stored.
func (m *BackendManager) HeadObject(ctx context.Context, key string) (int64, string, string, error) {
	const operation = "HeadObject"
	start := time.Now()

	// --- Start tracing span ---
	ctx, span := telemetry.StartSpan(ctx, "Manager "+operation,
		telemetry.AttrObjectKey.String(key),
	)
	defer span.End()

	// --- Find object location ---
	loc, err := m.store.GetObjectLocation(ctx, key)
	if err != nil {
		if errors.Is(err, ErrObjectNotFound) {
			span.SetStatus(codes.Error, "object not found")
			return 0, "", "", err
		}
		span.SetStatus(codes.Error, err.Error())
		span.RecordError(err)
		return 0, "", "", fmt.Errorf("failed to find object location: %w", err)
	}

	span.SetAttributes(telemetry.AttrBackendName.String(loc.BackendName))

	// --- Get the backend ---
	backend, ok := m.backends[loc.BackendName]
	if !ok {
		err := fmt.Errorf("backend %s not found", loc.BackendName)
		span.SetStatus(codes.Error, err.Error())
		return 0, "", "", err
	}

	// --- Head from backend ---
	size, contentType, etag, err := backend.HeadObject(ctx, key)
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		span.RecordError(err)
		return 0, "", "", err
	}

	// --- Record metrics ---
	m.recordOperation(operation, loc.BackendName, start, nil)

	span.SetAttributes(telemetry.AttrObjectSize.Int64(size))
	span.SetStatus(codes.Ok, "")
	return size, contentType, etag, nil
}

// DeleteObject removes an object from the backend where it's stored.
func (m *BackendManager) DeleteObject(ctx context.Context, key string) error {
	const operation = "DeleteObject"
	start := time.Now()

	// --- Start tracing span ---
	ctx, span := telemetry.StartSpan(ctx, "Manager "+operation,
		telemetry.AttrObjectKey.String(key),
	)
	defer span.End()

	// --- Delete from store (get location first) ---
	backendName, size, err := m.store.DeleteObject(ctx, key)
	if err != nil {
		if errors.Is(err, ErrObjectNotFound) {
			// Object not in our tracking - treat as success (idempotent delete)
			span.SetStatus(codes.Ok, "object not found - treating as success")
			return nil
		}
		span.SetStatus(codes.Error, err.Error())
		span.RecordError(err)
		return fmt.Errorf("failed to delete object record: %w", err)
	}

	span.SetAttributes(
		telemetry.AttrBackendName.String(backendName),
		telemetry.AttrObjectSize.Int64(size),
	)

	// --- Get the backend ---
	backend, ok := m.backends[backendName]
	if !ok {
		err := fmt.Errorf("backend %s not found", backendName)
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	// --- Delete from backend ---
	if err := backend.DeleteObject(ctx, key); err != nil {
		// Log error but don't fail - quota is already updated
		span.RecordError(err)
		span.SetAttributes(attribute.Bool("backend.delete_failed", true))
	}

	// --- Record metrics ---
	m.recordOperation(operation, backendName, start, nil)

	span.SetStatus(codes.Ok, "")
	return nil
}

// ListObjectsV2Result holds the processed result for the S3 ListObjectsV2 response.
type ListObjectsV2Result struct {
	Objects               []ObjectLocation
	CommonPrefixes        []string
	IsTruncated           bool
	NextContinuationToken string
	KeyCount              int
}

// ListObjects returns objects matching the given prefix with optional delimiter
// support for virtual directory grouping.
func (m *BackendManager) ListObjects(ctx context.Context, prefix, delimiter, startAfter string, maxKeys int) (*ListObjectsV2Result, error) {
	const operation = "ListObjects"
	start := time.Now()

	ctx, span := telemetry.StartSpan(ctx, "Manager "+operation,
		attribute.String("s3.prefix", prefix),
		attribute.String("s3.delimiter", delimiter),
		attribute.Int("s3.max_keys", maxKeys),
	)
	defer span.End()

	storeResult, err := m.store.ListObjects(ctx, prefix, startAfter, maxKeys)
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		span.RecordError(err)
		return nil, fmt.Errorf("failed to list objects: %w", err)
	}

	result := &ListObjectsV2Result{
		IsTruncated:           storeResult.IsTruncated,
		NextContinuationToken: storeResult.NextContinuationToken,
	}

	// Apply delimiter grouping if requested
	if delimiter != "" {
		seen := make(map[string]bool)
		prefixLen := len(prefix)

		for _, obj := range storeResult.Objects {
			rest := obj.ObjectKey[prefixLen:]
			idx := indexOf(rest, delimiter)
			if idx >= 0 {
				// Object is in a "subdirectory" - collapse to common prefix
				cp := obj.ObjectKey[:prefixLen+idx+len(delimiter)]
				if !seen[cp] {
					seen[cp] = true
					result.CommonPrefixes = append(result.CommonPrefixes, cp)
				}
			} else {
				result.Objects = append(result.Objects, obj)
			}
		}
	} else {
		result.Objects = storeResult.Objects
	}

	result.KeyCount = len(result.Objects) + len(result.CommonPrefixes)

	m.recordOperation(operation, "", start, nil)
	span.SetStatus(codes.Ok, "")
	span.SetAttributes(attribute.Int("s3.key_count", result.KeyCount))
	return result, nil
}

// indexOf returns the index of the first occurrence of substr in s, or -1.
func indexOf(s, substr string) int {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}

// GenerateUploadID creates a random hex string for multipart upload IDs.
func GenerateUploadID() string {
	b := make([]byte, 16)
	rand.Read(b)
	return hex.EncodeToString(b)
}

// -------------------------------------------------------------------------
// MULTIPART UPLOAD OPERATIONS
// -------------------------------------------------------------------------

// CreateMultipartUpload initiates a multipart upload by selecting a backend
// with available quota and recording the upload in the database.
func (m *BackendManager) CreateMultipartUpload(ctx context.Context, key, contentType string) (string, string, error) {
	const operation = "CreateMultipartUpload"
	start := time.Now()

	ctx, span := telemetry.StartSpan(ctx, "Manager "+operation,
		telemetry.AttrObjectKey.String(key),
	)
	defer span.End()

	// Pick a backend (estimate 0 bytes since final size is unknown)
	backendName, err := m.store.GetBackendWithSpace(ctx, 0, m.order)
	if err != nil {
		if errors.Is(err, ErrNoSpaceAvailable) {
			span.SetStatus(codes.Error, "insufficient storage")
			return "", "", ErrInsufficientStorage
		}
		span.SetStatus(codes.Error, err.Error())
		return "", "", fmt.Errorf("failed to find backend: %w", err)
	}

	uploadID := GenerateUploadID()
	if err := m.store.CreateMultipartUpload(ctx, uploadID, key, backendName, contentType); err != nil {
		span.SetStatus(codes.Error, err.Error())
		return "", "", err
	}

	span.SetAttributes(telemetry.AttrBackendName.String(backendName))
	m.recordOperation(operation, backendName, start, nil)
	span.SetStatus(codes.Ok, "")
	return uploadID, backendName, nil
}

// UploadPart uploads a single part to the backend. Parts are stored under a
// temporary key prefix and reassembled on completion.
func (m *BackendManager) UploadPart(ctx context.Context, uploadID string, partNumber int, body io.Reader, size int64) (string, error) {
	const operation = "UploadPart"
	start := time.Now()

	ctx, span := telemetry.StartSpan(ctx, "Manager "+operation,
		attribute.String("s3.upload_id", uploadID),
		attribute.Int("s3.part_number", partNumber),
	)
	defer span.End()

	mu, err := m.store.GetMultipartUpload(ctx, uploadID)
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		return "", err
	}

	backend, ok := m.backends[mu.BackendName]
	if !ok {
		err := fmt.Errorf("backend %s not found", mu.BackendName)
		span.SetStatus(codes.Error, err.Error())
		return "", err
	}

	// Store part under a temp key
	partKey := fmt.Sprintf("__multipart/%s/%d", uploadID, partNumber)
	etag, err := backend.PutObject(ctx, partKey, body, size, "application/octet-stream")
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		return "", fmt.Errorf("failed to upload part: %w", err)
	}

	if err := m.store.RecordPart(ctx, uploadID, partNumber, etag, size); err != nil {
		span.RecordError(err)
	}

	m.recordOperation(operation, mu.BackendName, start, nil)
	span.SetStatus(codes.Ok, "")
	return etag, nil
}

// CompleteMultipartUpload reassembles parts into the final object. Downloads
// each part, concatenates them into a single upload, then cleans up temp keys
// and records the final object location with quota tracking.
func (m *BackendManager) CompleteMultipartUpload(ctx context.Context, uploadID string, partNumbers []int) (string, error) {
	const operation = "CompleteMultipartUpload"
	start := time.Now()

	ctx, span := telemetry.StartSpan(ctx, "Manager "+operation,
		attribute.String("s3.upload_id", uploadID),
	)
	defer span.End()

	mu, err := m.store.GetMultipartUpload(ctx, uploadID)
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		return "", err
	}

	backend, ok := m.backends[mu.BackendName]
	if !ok {
		err := fmt.Errorf("backend %s not found", mu.BackendName)
		span.SetStatus(codes.Error, err.Error())
		return "", err
	}

	parts, err := m.store.GetParts(ctx, uploadID)
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		return "", err
	}

	// Calculate total size and build a multi-reader from all parts
	var totalSize int64
	var readers []io.Reader
	var closers []io.ReadCloser

	for _, part := range parts {
		partKey := fmt.Sprintf("__multipart/%s/%d", uploadID, part.PartNumber)
		body, _, _, _, err := backend.GetObject(ctx, partKey)
		if err != nil {
			// Clean up already-opened readers
			for _, c := range closers {
				c.Close()
			}
			span.SetStatus(codes.Error, err.Error())
			return "", fmt.Errorf("failed to read part %d: %w", part.PartNumber, err)
		}
		readers = append(readers, body)
		closers = append(closers, body)
		totalSize += part.SizeBytes
	}

	// Upload the concatenated object
	combined := io.MultiReader(readers...)
	etag, err := backend.PutObject(ctx, mu.ObjectKey, combined, totalSize, mu.ContentType)

	// Close all part readers
	for _, c := range closers {
		c.Close()
	}

	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		return "", fmt.Errorf("failed to upload final object: %w", err)
	}

	// Record the final object location and update quota
	if err := m.store.RecordObject(ctx, mu.ObjectKey, mu.BackendName, totalSize); err != nil {
		span.RecordError(err)
	}

	// Clean up part objects from backend
	for _, part := range parts {
		partKey := fmt.Sprintf("__multipart/%s/%d", uploadID, part.PartNumber)
		if delErr := backend.DeleteObject(ctx, partKey); delErr != nil {
			log.Printf("Warning: failed to delete part key %s: %v", partKey, delErr)
		}
	}

	// Clean up multipart records from database
	if err := m.store.DeleteMultipartUpload(ctx, uploadID); err != nil {
		span.RecordError(err)
	}

	m.recordOperation(operation, mu.BackendName, start, nil)
	span.SetStatus(codes.Ok, "")
	return etag, nil
}

// AbortMultipartUpload cleans up an in-progress multipart upload, removing
// all part objects from the backend and the upload records from the database.
func (m *BackendManager) AbortMultipartUpload(ctx context.Context, uploadID string) error {
	const operation = "AbortMultipartUpload"
	start := time.Now()

	ctx, span := telemetry.StartSpan(ctx, "Manager "+operation,
		attribute.String("s3.upload_id", uploadID),
	)
	defer span.End()

	mu, err := m.store.GetMultipartUpload(ctx, uploadID)
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	backend, ok := m.backends[mu.BackendName]
	if !ok {
		err := fmt.Errorf("backend %s not found", mu.BackendName)
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	parts, err := m.store.GetParts(ctx, uploadID)
	if err != nil {
		span.RecordError(err)
	}

	// Delete part objects from backend
	for _, part := range parts {
		partKey := fmt.Sprintf("__multipart/%s/%d", uploadID, part.PartNumber)
		if delErr := backend.DeleteObject(ctx, partKey); delErr != nil {
			log.Printf("Warning: failed to delete part key %s: %v", partKey, delErr)
		}
	}

	// Delete multipart records from database
	if err := m.store.DeleteMultipartUpload(ctx, uploadID); err != nil {
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	m.recordOperation(operation, mu.BackendName, start, nil)
	span.SetStatus(codes.Ok, "")
	return nil
}

// CleanupStaleMultipartUploads aborts multipart uploads older than the given
// duration. Run periodically to prevent quota leaks from abandoned uploads.
func (m *BackendManager) CleanupStaleMultipartUploads(ctx context.Context, olderThan time.Duration) {
	uploads, err := m.store.GetStaleMultipartUploads(ctx, olderThan)
	if err != nil {
		log.Printf("Failed to get stale multipart uploads: %v", err)
		return
	}

	for _, mu := range uploads {
		log.Printf("Cleaning up stale multipart upload %s for key %s", mu.UploadID, mu.ObjectKey)
		if err := m.AbortMultipartUpload(ctx, mu.UploadID); err != nil {
			log.Printf("Failed to clean up upload %s: %v", mu.UploadID, err)
		}
	}
}

// -------------------------------------------------------------------------
// METRICS
// -------------------------------------------------------------------------

// recordOperation updates Prometheus metrics for a manager operation.
func (m *BackendManager) recordOperation(operation, backend string, start time.Time, err error) {
	status := "success"
	if err != nil {
		status = "error"
	}

	telemetry.ManagerRequestsTotal.WithLabelValues(operation, backend, status).Inc()
	telemetry.ManagerDuration.WithLabelValues(operation, backend).Observe(time.Since(start).Seconds())
}

// -------------------------------------------------------------------------
// QUOTA STATS
// -------------------------------------------------------------------------

// UpdateQuotaMetrics fetches quota stats, object counts, and active multipart
// upload counts, then updates the corresponding Prometheus gauges.
func (m *BackendManager) UpdateQuotaMetrics(ctx context.Context) error {
	stats, err := m.store.GetQuotaStats(ctx)
	if err != nil {
		return err
	}

	for name, stat := range stats {
		telemetry.QuotaBytesUsed.WithLabelValues(name).Set(float64(stat.BytesUsed))
		telemetry.QuotaBytesLimit.WithLabelValues(name).Set(float64(stat.BytesLimit))
		telemetry.QuotaBytesAvailable.WithLabelValues(name).Set(float64(stat.BytesLimit - stat.BytesUsed))
	}

	// --- Object counts per backend ---
	objCounts, err := m.store.GetObjectCounts(ctx)
	if err != nil {
		log.Printf("Failed to get object counts: %v", err)
	} else {
		// Reset to zero for backends with no objects, then set actual counts
		for name := range stats {
			telemetry.ObjectCount.WithLabelValues(name).Set(0)
		}
		for name, count := range objCounts {
			telemetry.ObjectCount.WithLabelValues(name).Set(float64(count))
		}
	}

	// --- Active multipart uploads per backend ---
	mpCounts, err := m.store.GetActiveMultipartCounts(ctx)
	if err != nil {
		log.Printf("Failed to get multipart upload counts: %v", err)
	} else {
		for name := range stats {
			telemetry.ActiveMultipartUploads.WithLabelValues(name).Set(0)
		}
		for name, count := range mpCounts {
			telemetry.ActiveMultipartUploads.WithLabelValues(name).Set(float64(count))
		}
	}

	return nil
}
