// -------------------------------------------------------------------------------
// Object Handlers - PUT, GET, HEAD, DELETE
//
// Project: Munchbox / Author: Alex Freidah
//
// HTTP handlers for standard S3 object operations. Streams response bodies
// directly to avoid buffering large objects in memory.
// -------------------------------------------------------------------------------

package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"

	"github.com/munchbox/s3-proxy/internal/storage"
	"github.com/munchbox/s3-proxy/internal/telemetry"
	"go.opentelemetry.io/otel/trace"
)

// handlePut processes PUT requests. Requires Content-Length header to enforce
// size-based policies in later phases.
func (s *Server) handlePut(ctx context.Context, w http.ResponseWriter, r *http.Request, key string) (int, error) {
	if r.ContentLength < 0 {
		writeS3Error(w, http.StatusLengthRequired, "MissingContentLength", "Content-Length header is required")
		return http.StatusLengthRequired, fmt.Errorf("missing Content-Length")
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
		if errors.Is(err, storage.ErrInsufficientStorage) {
			writeS3Error(w, http.StatusInsufficientStorage, "InsufficientStorage", "No backend has sufficient quota")
			return http.StatusInsufficientStorage, err
		}
		writeS3Error(w, http.StatusBadGateway, "InternalError", "Failed to store object")
		return http.StatusBadGateway, err
	}

	if etag != "" {
		w.Header().Set("ETag", etag)
	}
	w.WriteHeader(http.StatusOK)
	return http.StatusOK, nil
}

// handleGet processes GET requests. Streams the response body directly to avoid
// buffering large objects in memory.
func (s *Server) handleGet(ctx context.Context, w http.ResponseWriter, r *http.Request, key string) (int, int64, error) {
	body, size, contentType, etag, err := s.Manager.GetObject(ctx, key)
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotFound) {
			writeS3Error(w, http.StatusNotFound, "NoSuchKey", "Object not found")
			return http.StatusNotFound, 0, err
		}
		writeS3Error(w, http.StatusBadGateway, "InternalError", "Failed to retrieve object")
		return http.StatusBadGateway, 0, err
	}
	defer body.Close()

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

	written, copyErr := io.Copy(w, body)
	if copyErr != nil {
		return http.StatusOK, written, fmt.Errorf("error streaming body: %w", copyErr)
	}

	return http.StatusOK, written, nil
}

// handleHead processes HEAD requests.
func (s *Server) handleHead(ctx context.Context, w http.ResponseWriter, r *http.Request, key string) (int, error) {
	size, contentType, etag, err := s.Manager.HeadObject(ctx, key)
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotFound) {
			writeS3Error(w, http.StatusNotFound, "NoSuchKey", "Object not found")
			return http.StatusNotFound, err
		}
		writeS3Error(w, http.StatusBadGateway, "InternalError", "Failed to retrieve object metadata")
		return http.StatusBadGateway, err
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
	w.WriteHeader(http.StatusOK)
	return http.StatusOK, nil
}

// handleDelete processes DELETE requests. Treats missing objects as success
// since S3 DELETE is typically idempotent.
func (s *Server) handleDelete(ctx context.Context, w http.ResponseWriter, r *http.Request, key string) (int, error) {
	err := s.Manager.DeleteObject(ctx, key)
	if err != nil {
		log.Printf("Delete error (treating as success): %v", err)
	}

	w.WriteHeader(http.StatusNoContent)
	return http.StatusNoContent, nil
}
