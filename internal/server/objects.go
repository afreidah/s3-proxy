// -------------------------------------------------------------------------------
// Object Handlers - PUT, GET, HEAD, DELETE, CopyObject
//
// Project: Munchbox / Author: Alex Freidah
//
// HTTP handlers for standard S3 object operations. Streams response bodies
// directly to avoid buffering large objects in memory.
// -------------------------------------------------------------------------------

package server

import (
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/munchbox/s3-proxy/internal/telemetry"
	"go.opentelemetry.io/otel/trace"
)

// -------------------------------------------------------------------------
// XML TYPES
// -------------------------------------------------------------------------

// copyObjectResult is the XML response for CopyObject.
type copyObjectResult struct {
	XMLName      xml.Name `xml:"CopyObjectResult"`
	Xmlns        string   `xml:"xmlns,attr"`
	ETag         string   `xml:"ETag"`
	LastModified string   `xml:"LastModified"`
}

// handlePut processes PUT requests. Requires Content-Length header to enforce
// size-based policies in later phases.
func (s *Server) handlePut(ctx context.Context, w http.ResponseWriter, r *http.Request, key string) (int, error) {
	if r.ContentLength < 0 {
		writeS3Error(w, http.StatusLengthRequired, "MissingContentLength", "Content-Length header is required")
		return http.StatusLengthRequired, fmt.Errorf("missing Content-Length")
	}

	if s.MaxObjectSize > 0 && r.ContentLength > s.MaxObjectSize {
		writeS3Error(w, http.StatusRequestEntityTooLarge, "EntityTooLarge", "Object size exceeds the maximum allowed size")
		return http.StatusRequestEntityTooLarge, fmt.Errorf("object size %d exceeds max %d", r.ContentLength, s.MaxObjectSize)
	}

	if s.MaxObjectSize > 0 {
		r.Body = http.MaxBytesReader(w, r.Body, s.MaxObjectSize)
	}

	contentType := r.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	// --- Add size to span ---
	span := trace.SpanFromContext(ctx)
	span.SetAttributes(
		telemetry.AttrObjectSize.Int64(r.ContentLength),
		telemetry.AttrContentType.String(contentType),
	)

	etag, err := s.Manager.PutObject(ctx, key, r.Body, r.ContentLength, contentType)
	if err != nil {
		return writeStorageError(w, err, "Failed to store object"), err
	}

	if etag != "" {
		w.Header().Set("ETag", etag)
	}
	w.WriteHeader(http.StatusOK)
	return http.StatusOK, nil
}

// handleGet processes GET requests. Streams the response body directly to avoid
// buffering large objects in memory. Supports Range requests — when the client
// sends a Range header, the response is 206 Partial Content with Content-Range.
func (s *Server) handleGet(ctx context.Context, w http.ResponseWriter, r *http.Request, key string) (int, int64, error) {
	rangeHeader := r.Header.Get("Range")

	body, size, contentType, etag, contentRange, err := s.Manager.GetObject(ctx, key, rangeHeader)
	if err != nil {
		return writeStorageError(w, err, "Failed to retrieve object"), 0, err
	}
	defer func() { _ = body.Close() }()

	// --- Add size to span ---
	span := trace.SpanFromContext(ctx)
	span.SetAttributes(
		telemetry.AttrObjectSize.Int64(size),
		telemetry.AttrContentType.String(contentType),
	)

	w.Header().Set("Content-Type", contentType)
	if size > 0 {
		w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
	}
	if etag != "" {
		w.Header().Set("ETag", etag)
	}
	w.Header().Set("Accept-Ranges", "bytes")

	status := http.StatusOK
	if contentRange != "" {
		w.Header().Set("Content-Range", contentRange)
		status = http.StatusPartialContent
	}
	w.WriteHeader(status)

	written, copyErr := io.Copy(w, body)
	if copyErr != nil {
		return status, written, fmt.Errorf("error streaming body: %w", copyErr)
	}

	return status, written, nil
}

// handleHead processes HEAD requests.
func (s *Server) handleHead(ctx context.Context, w http.ResponseWriter, r *http.Request, key string) (int, error) {
	size, contentType, etag, err := s.Manager.HeadObject(ctx, key)
	if err != nil {
		return writeStorageError(w, err, "Failed to retrieve object metadata"), err
	}

	// --- Add size to span ---
	span := trace.SpanFromContext(ctx)
	span.SetAttributes(
		telemetry.AttrObjectSize.Int64(size),
		telemetry.AttrContentType.String(contentType),
	)

	w.Header().Set("Content-Type", contentType)
	w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
	if etag != "" {
		w.Header().Set("ETag", etag)
	}
	w.Header().Set("Accept-Ranges", "bytes")
	w.WriteHeader(http.StatusOK)
	return http.StatusOK, nil
}

// handleDelete processes DELETE requests. The manager treats missing objects as
// success (S3 idempotent delete), so any error returned is a real backend failure.
func (s *Server) handleDelete(ctx context.Context, w http.ResponseWriter, r *http.Request, key string) (int, error) {
	if err := s.Manager.DeleteObject(ctx, key); err != nil {
		return writeStorageError(w, err, "Failed to delete object"), err
	}

	w.WriteHeader(http.StatusNoContent)
	return http.StatusNoContent, nil
}

// handleCopyObject processes PUT requests with the x-amz-copy-source header.
// Copies an object from the source key to the destination key, potentially
// across backends, with atomic quota tracking.
func (s *Server) handleCopyObject(ctx context.Context, w http.ResponseWriter, r *http.Request, bucket, destKey, copySource string) (int, error) {
	// --- Parse x-amz-copy-source header ---
	source := strings.TrimPrefix(copySource, "/")
	sourceBucket, sourceKey, ok := parsePath("/" + source)
	if !ok || sourceKey == "" {
		writeS3Error(w, http.StatusBadRequest, "InvalidArgument", "Invalid x-amz-copy-source")
		return http.StatusBadRequest, fmt.Errorf("invalid copy source: %s", copySource)
	}

	// --- Validate source bucket ---
	if sourceBucket != s.VirtualBucket {
		writeS3Error(w, http.StatusNotFound, "NoSuchBucket", fmt.Sprintf("Source bucket %s not found", sourceBucket))
		return http.StatusNotFound, fmt.Errorf("unknown source bucket: %s", sourceBucket)
	}

	etag, err := s.Manager.CopyObject(ctx, sourceKey, destKey)
	if err != nil {
		return writeStorageError(w, err, "Failed to copy object"), err
	}

	result := copyObjectResult{
		Xmlns:        "http://s3.amazonaws.com/doc/2006-03-01/",
		ETag:         etag,
		LastModified: time.Now().UTC().Format(time.RFC3339),
	}

	if err := writeXML(w, http.StatusOK, result); err != nil {
		return http.StatusOK, fmt.Errorf("failed to encode copy response: %w", err)
	}
	return http.StatusOK, nil
}
