package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"time"

	"github.com/munchbox/s3-proxy/internal/telemetry"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// -------------------------------------------------------------------------
// OBJECT CRUD
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
		if errors.Is(err, ErrDBUnavailable) {
			span.SetStatus(codes.Error, "database unavailable")
			telemetry.DegradedWriteRejectionsTotal.WithLabelValues(operation).Inc()
			return "", ErrServiceUnavailable
		}
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
		slog.Error("RecordObject failed, cleaning up backend object",
			"key", key, "backend", backendName, "error", err)
		if delErr := backend.DeleteObject(ctx, key); delErr != nil {
			slog.Error("Failed to clean up orphaned object", "key", key, "backend", backendName, "error", delErr)
		}
		span.SetStatus(codes.Error, err.Error())
		span.RecordError(err)
		return "", fmt.Errorf("failed to record object: %w", err)
	}

	// --- Record metrics ---
	m.recordOperation(operation, backendName, start, nil)

	span.SetStatus(codes.Ok, "")
	return etag, nil
}

// withReadFailover looks up all copies of an object and tries each backend in
// order until one succeeds. The tryBackend callback should attempt the operation
// and return the object size (for span attributes) or an error to try the next copy.
func (m *BackendManager) withReadFailover(ctx context.Context, operation, key string, tryBackend func(ctx context.Context, backend ObjectBackend) (int64, error)) error {
	start := time.Now()

	ctx, span := telemetry.StartSpan(ctx, "Manager "+operation,
		telemetry.AttrObjectKey.String(key),
	)
	defer span.End()

	locations, err := m.store.GetAllObjectLocations(ctx, key)
	if err != nil {
		if errors.Is(err, ErrObjectNotFound) {
			span.SetStatus(codes.Error, "object not found")
			return err
		}
		if errors.Is(err, ErrDBUnavailable) {
			return m.broadcastRead(ctx, operation, key, start, span, tryBackend)
		}
		span.SetStatus(codes.Error, err.Error())
		span.RecordError(err)
		return fmt.Errorf("failed to find object location: %w", err)
	}

	var lastErr error
	for i, loc := range locations {
		span.SetAttributes(telemetry.AttrBackendName.String(loc.BackendName))

		backend, ok := m.backends[loc.BackendName]
		if !ok {
			lastErr = fmt.Errorf("backend %s not found", loc.BackendName)
			continue
		}

		size, err := tryBackend(ctx, backend)
		if err != nil {
			lastErr = err
			if i < len(locations)-1 {
				slog.Warn(operation+": copy failed, trying next",
					"key", key, "failed_backend", loc.BackendName, "error", err)
			}
			continue
		}

		m.recordOperation(operation, loc.BackendName, start, nil)
		if i > 0 {
			span.SetAttributes(attribute.Bool("s3proxy.failover", true))
		}
		span.SetAttributes(telemetry.AttrObjectSize.Int64(size))
		span.SetStatus(codes.Ok, "")
		return nil
	}

	span.SetStatus(codes.Error, lastErr.Error())
	span.RecordError(lastErr)
	return lastErr
}

// broadcastRead tries all backends when the DB is unavailable. Checks the
// location cache first for a known-good backend, then falls back to trying
// every backend in order.
func (m *BackendManager) broadcastRead(ctx context.Context, operation, key string, start time.Time, span trace.Span, tryBackend func(ctx context.Context, backend ObjectBackend) (int64, error)) error {
	span.SetAttributes(attribute.Bool("s3proxy.degraded_mode", true))
	telemetry.DegradedReadsTotal.WithLabelValues(operation).Inc()

	// --- Check location cache first ---
	if cachedBackend, ok := m.cacheGet(key); ok {
		if backend, exists := m.backends[cachedBackend]; exists {
			size, err := tryBackend(ctx, backend)
			if err == nil {
				m.recordOperation(operation, cachedBackend, start, nil)
				span.SetAttributes(attribute.Bool("s3proxy.cache_hit", true))
				span.SetAttributes(telemetry.AttrObjectSize.Int64(size))
				span.SetStatus(codes.Ok, "")
				telemetry.DegradedCacheHitsTotal.Inc()
				return nil
			}
			// Cache hit but backend failed — fall through to broadcast
		}
	}

	// --- Broadcast to all backends ---
	var lastErr error
	for _, name := range m.order {
		backend, ok := m.backends[name]
		if !ok {
			continue
		}
		size, err := tryBackend(ctx, backend)
		if err != nil {
			lastErr = err
			continue
		}

		// Success — cache the result for future degraded reads
		m.cacheSet(key, name)
		m.recordOperation(operation, name, start, nil)
		span.SetAttributes(telemetry.AttrBackendName.String(name))
		span.SetAttributes(telemetry.AttrObjectSize.Int64(size))
		span.SetStatus(codes.Ok, "")
		return nil
	}

	// All backends failed
	if lastErr != nil {
		span.SetStatus(codes.Error, lastErr.Error())
		return ErrObjectNotFound
	}

	span.SetStatus(codes.Error, "object not found on any backend")
	return ErrObjectNotFound
}

// GetObject retrieves an object from the backend where it's stored. Tries the
// primary copy first, then falls back to replicas if the primary fails.
func (m *BackendManager) GetObject(ctx context.Context, key string, rangeHeader string) (io.ReadCloser, int64, string, string, string, error) {
	var (
		rBody         io.ReadCloser
		rSize         int64
		rContentType  string
		rETag         string
		rContentRange string
	)

	err := m.withReadFailover(ctx, "GetObject", key, func(ctx context.Context, backend ObjectBackend) (int64, error) {
		body, size, contentType, etag, contentRange, err := backend.GetObject(ctx, key, rangeHeader)
		if err != nil {
			return 0, err
		}
		rBody, rSize, rContentType, rETag, rContentRange = body, size, contentType, etag, contentRange
		return size, nil
	})
	if err != nil {
		return nil, 0, "", "", "", err
	}
	return rBody, rSize, rContentType, rETag, rContentRange, nil
}

// HeadObject retrieves object metadata. Tries the primary copy first, then
// falls back to replicas if the primary fails.
func (m *BackendManager) HeadObject(ctx context.Context, key string) (int64, string, string, error) {
	var (
		rSize        int64
		rContentType string
		rETag        string
	)

	err := m.withReadFailover(ctx, "HeadObject", key, func(ctx context.Context, backend ObjectBackend) (int64, error) {
		size, contentType, etag, err := backend.HeadObject(ctx, key)
		if err != nil {
			return 0, err
		}
		rSize, rContentType, rETag = size, contentType, etag
		return size, nil
	})
	if err != nil {
		return 0, "", "", err
	}
	return rSize, rContentType, rETag, nil
}

// CopyObject copies an object from sourceKey to destKey. Streams the source
// through a pipe to avoid buffering the entire object. Supports cross-backend
// copies and read failover from replicas.
func (m *BackendManager) CopyObject(ctx context.Context, sourceKey, destKey string) (string, error) {
	const operation = "CopyObject"
	start := time.Now()

	ctx, span := telemetry.StartSpan(ctx, "Manager "+operation,
		attribute.String("s3.source_key", sourceKey),
		attribute.String("s3.dest_key", destKey),
	)
	defer span.End()

	// --- Find all source locations (for failover) ---
	locations, err := m.store.GetAllObjectLocations(ctx, sourceKey)
	if err != nil {
		if errors.Is(err, ErrDBUnavailable) {
			span.SetStatus(codes.Error, "database unavailable")
			telemetry.DegradedWriteRejectionsTotal.WithLabelValues(operation).Inc()
			return "", ErrServiceUnavailable
		}
		if errors.Is(err, ErrObjectNotFound) {
			span.SetStatus(codes.Error, "source object not found")
			return "", err
		}
		span.SetStatus(codes.Error, err.Error())
		span.RecordError(err)
		return "", fmt.Errorf("failed to find source object: %w", err)
	}

	// --- Get source metadata (try each copy) ---
	var size int64
	var contentType string
	var srcFound bool
	for _, loc := range locations {
		backend, ok := m.backends[loc.BackendName]
		if !ok {
			continue
		}
		s, ct, _, err := backend.HeadObject(ctx, sourceKey)
		if err != nil {
			continue
		}
		size = s
		contentType = ct
		srcFound = true
		break
	}
	if !srcFound {
		err := fmt.Errorf("failed to head source object from any copy")
		span.SetStatus(codes.Error, err.Error())
		return "", err
	}

	span.SetAttributes(telemetry.AttrObjectSize.Int64(size))

	// --- Find destination backend with available quota ---
	destBackendName, err := m.store.GetBackendWithSpace(ctx, size, m.order)
	if err != nil {
		if errors.Is(err, ErrDBUnavailable) {
			span.SetStatus(codes.Error, "database unavailable")
			telemetry.DegradedWriteRejectionsTotal.WithLabelValues(operation).Inc()
			return "", ErrServiceUnavailable
		}
		if errors.Is(err, ErrNoSpaceAvailable) {
			span.SetStatus(codes.Error, "insufficient storage")
			return "", ErrInsufficientStorage
		}
		span.SetStatus(codes.Error, err.Error())
		return "", fmt.Errorf("failed to find destination backend: %w", err)
	}

	destBackend, ok := m.backends[destBackendName]
	if !ok {
		err := fmt.Errorf("destination backend %s not found", destBackendName)
		span.SetStatus(codes.Error, err.Error())
		return "", err
	}

	// --- Stream source to destination via pipe (with failover) ---
	pr, pw := io.Pipe()

	go func() {
		defer func() { _ = pw.Close() }()
		for _, loc := range locations {
			srcBackend, ok := m.backends[loc.BackendName]
			if !ok {
				continue
			}
			body, _, _, _, _, err := srcBackend.GetObject(ctx, sourceKey, "")
			if err != nil {
				continue
			}
			_, copyErr := io.Copy(pw, body)
			_ = body.Close()
			if copyErr != nil {
				pw.CloseWithError(fmt.Errorf("failed to stream source: %w", copyErr))
			}
			return
		}
		pw.CloseWithError(fmt.Errorf("failed to read source from any copy"))
	}()

	etag, err := destBackend.PutObject(ctx, destKey, pr, size, contentType)
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		span.RecordError(err)
		return "", fmt.Errorf("failed to write destination: %w", err)
	}

	// --- Record destination location and update quota ---
	if err := m.store.RecordObject(ctx, destKey, destBackendName, size); err != nil {
		slog.Error("RecordObject failed after copy, cleaning up",
			"key", destKey, "backend", destBackendName, "error", err)
		if delErr := destBackend.DeleteObject(ctx, destKey); delErr != nil {
			slog.Error("Failed to clean up orphaned copy", "key", destKey, "backend", destBackendName, "error", delErr)
		}
		span.SetStatus(codes.Error, err.Error())
		span.RecordError(err)
		return "", fmt.Errorf("failed to record copied object: %w", err)
	}

	m.recordOperation(operation, destBackendName, start, nil)
	span.SetStatus(codes.Ok, "")
	return etag, nil
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

	// --- Delete all copies from store ---
	copies, err := m.store.DeleteObject(ctx, key)
	if err != nil {
		if errors.Is(err, ErrDBUnavailable) {
			span.SetStatus(codes.Error, "database unavailable")
			telemetry.DegradedWriteRejectionsTotal.WithLabelValues(operation).Inc()
			return ErrServiceUnavailable
		}
		if errors.Is(err, ErrObjectNotFound) {
			// Object not in our tracking - treat as success (idempotent delete)
			span.SetStatus(codes.Ok, "object not found - treating as success")
			return nil
		}
		span.SetStatus(codes.Error, err.Error())
		span.RecordError(err)
		return fmt.Errorf("failed to delete object record: %w", err)
	}

	span.SetAttributes(attribute.Int("copies.deleted", len(copies)))

	// --- Delete from each backend that held a copy ---
	for _, copy := range copies {
		backend, ok := m.backends[copy.BackendName]
		if !ok {
			slog.Warn("Backend not found for delete",
				"backend", copy.BackendName, "key", key)
			continue
		}
		if err := backend.DeleteObject(ctx, key); err != nil {
			// Log error but don't fail - quota is already updated
			slog.Warn("Failed to delete object from backend",
				"backend", copy.BackendName, "key", key, "error", err)
			span.RecordError(err)
		}
	}

	// --- Record metrics (use first copy's backend for primary) ---
	if len(copies) > 0 {
		m.recordOperation(operation, copies[0].BackendName, start, nil)
	}

	span.SetStatus(codes.Ok, "")
	return nil
}

// -------------------------------------------------------------------------
// LIST
// -------------------------------------------------------------------------

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
		if errors.Is(err, ErrDBUnavailable) {
			span.SetStatus(codes.Error, "database unavailable")
			return nil, &S3Error{StatusCode: 503, Code: "ServiceUnavailable", Message: "listing unavailable during database outage"}
		}
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
			idx := strings.Index(rest, delimiter)
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
