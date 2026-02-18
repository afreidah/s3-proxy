package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/afreidah/s3-orchestrator/internal/telemetry"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

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

	// Filter backends within usage limits before selecting
	eligible := m.backendsWithinLimits(1, 0, 0)
	if len(eligible) == 0 {
		span.SetStatus(codes.Error, "usage limits exceeded on all backends")
		return "", "", ErrInsufficientStorage
	}

	// Pick a backend (estimate 0 bytes since final size is unknown)
	backendName, err := m.store.GetBackendWithSpace(ctx, 0, eligible)
	if err != nil {
		if errors.Is(err, ErrDBUnavailable) {
			span.SetStatus(codes.Error, "database unavailable")
			telemetry.DegradedWriteRejectionsTotal.WithLabelValues(operation).Inc()
			return "", "", ErrServiceUnavailable
		}
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
		if errors.Is(err, ErrDBUnavailable) {
			telemetry.DegradedWriteRejectionsTotal.WithLabelValues(operation).Inc()
			return "", ErrServiceUnavailable
		}
		span.SetStatus(codes.Error, err.Error())
		return "", err
	}

	backend, ok := m.backends[mu.BackendName]
	if !ok {
		err := fmt.Errorf("backend %s not found", mu.BackendName)
		span.SetStatus(codes.Error, err.Error())
		return "", err
	}

	// Check usage limits before uploading
	if !m.withinUsageLimits(mu.BackendName, 1, 0, size) {
		span.SetStatus(codes.Error, "usage limits exceeded")
		return "", ErrInsufficientStorage
	}

	// Store part under a temp key
	partKey := fmt.Sprintf("__multipart/%s/%d", uploadID, partNumber)
	bctx, bcancel := m.withTimeout(ctx)
	defer bcancel()
	etag, err := backend.PutObject(bctx, partKey, body, size, "application/octet-stream")
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		return "", fmt.Errorf("failed to upload part: %w", err)
	}

	if err := m.store.RecordPart(ctx, uploadID, partNumber, etag, size); err != nil {
		slog.Error("RecordPart failed, cleaning up part object",
			"upload_id", uploadID, "part", partNumber, "error", err)
		if delErr := backend.DeleteObject(ctx, partKey); delErr != nil {
			slog.Error("Failed to clean up orphaned part object",
				"key", partKey, "error", delErr)
		}
		span.SetStatus(codes.Error, err.Error())
		span.RecordError(err)
		return "", fmt.Errorf("failed to record part: %w", err)
	}

	m.recordOperation(operation, mu.BackendName, start, nil)
	m.recordUsage(mu.BackendName, 1, 0, size)
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
		if errors.Is(err, ErrDBUnavailable) {
			telemetry.DegradedWriteRejectionsTotal.WithLabelValues(operation).Inc()
			return "", ErrServiceUnavailable
		}
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

	// Calculate total size for the combined upload
	var totalSize int64
	for _, part := range parts {
		totalSize += part.SizeBytes
	}

	// Stream parts sequentially through a pipe to avoid opening all backend
	// connections simultaneously. The goroutine reads one part at a time and
	// writes it to the pipe; the PutObject call reads from the other end.
	pr, pw := io.Pipe()

	go func() {
		defer func() { _ = pw.Close() }()
		for _, part := range parts {
			partKey := fmt.Sprintf("__multipart/%s/%d", uploadID, part.PartNumber)
			bctx, bcancel := m.withTimeout(ctx)
			result, err := backend.GetObject(bctx, partKey, "")
			if err != nil {
				bcancel()
				pw.CloseWithError(fmt.Errorf("failed to read part %d: %w", part.PartNumber, err))
				return
			}
			_, err = io.Copy(pw, result.Body)
			_ = result.Body.Close()
			bcancel()
			if err != nil {
				pw.CloseWithError(fmt.Errorf("failed to stream part %d: %w", part.PartNumber, err))
				return
			}
		}
	}()

	pctx, pcancel := m.withTimeout(ctx)
	defer pcancel()
	etag, err := backend.PutObject(pctx, mu.ObjectKey, pr, totalSize, mu.ContentType)
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		return "", fmt.Errorf("failed to upload final object: %w", err)
	}

	// Record the final object location and update quota
	if err := m.store.RecordObject(ctx, mu.ObjectKey, mu.BackendName, totalSize); err != nil {
		slog.Error("RecordObject failed after multipart complete, cleaning up",
			"key", mu.ObjectKey, "backend", mu.BackendName, "error", err)
		if delErr := backend.DeleteObject(ctx, mu.ObjectKey); delErr != nil {
			slog.Error("Failed to clean up orphaned multipart object",
				"key", mu.ObjectKey, "backend", mu.BackendName, "error", delErr)
		}
		span.SetStatus(codes.Error, err.Error())
		span.RecordError(err)
		return "", fmt.Errorf("failed to record completed object: %w", err)
	}

	// Clean up part objects from backend
	for _, part := range parts {
		partKey := fmt.Sprintf("__multipart/%s/%d", uploadID, part.PartNumber)
		dctx, dcancel := m.withTimeout(ctx)
		delErr := backend.DeleteObject(dctx, partKey)
		dcancel()
		if delErr != nil {
			slog.Warn("Failed to delete part key", "key", partKey, "error", delErr)
		}
	}

	// Clean up multipart records from database
	if err := m.store.DeleteMultipartUpload(ctx, uploadID); err != nil {
		span.RecordError(err)
	}

	m.recordOperation(operation, mu.BackendName, start, nil)
	m.recordUsage(mu.BackendName, 1, 0, 0)
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
		if errors.Is(err, ErrDBUnavailable) {
			telemetry.DegradedWriteRejectionsTotal.WithLabelValues(operation).Inc()
			return ErrServiceUnavailable
		}
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
		span.SetStatus(codes.Error, err.Error())
		span.RecordError(err)
		return fmt.Errorf("failed to get parts for abort: %w", err)
	}

	// Delete part objects from backend
	for _, part := range parts {
		partKey := fmt.Sprintf("__multipart/%s/%d", uploadID, part.PartNumber)
		dctx, dcancel := m.withTimeout(ctx)
		delErr := backend.DeleteObject(dctx, partKey)
		dcancel()
		if delErr != nil {
			slog.Warn("Failed to delete part key", "key", partKey, "error", delErr)
		}
	}

	// Delete multipart records from database
	if err := m.store.DeleteMultipartUpload(ctx, uploadID); err != nil {
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	m.recordOperation(operation, mu.BackendName, start, nil)
	m.recordUsage(mu.BackendName, 1, 0, 0)
	span.SetStatus(codes.Ok, "")
	return nil
}

// CleanupStaleMultipartUploads aborts multipart uploads older than the given
// duration. Run periodically to prevent quota leaks from abandoned uploads.
func (m *BackendManager) CleanupStaleMultipartUploads(ctx context.Context, olderThan time.Duration) {
	uploads, err := m.store.GetStaleMultipartUploads(ctx, olderThan)
	if err != nil {
		slog.Error("Failed to get stale multipart uploads", "error", err)
		return
	}

	for _, mu := range uploads {
		slog.Info("Cleaning up stale multipart upload", "upload_id", mu.UploadID, "key", mu.ObjectKey)
		if err := m.AbortMultipartUpload(ctx, mu.UploadID); err != nil {
			slog.Error("Failed to clean up upload", "upload_id", mu.UploadID, "error", err)
		}
	}
}
