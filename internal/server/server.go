// -------------------------------------------------------------------------------
// HTTP Server - S3 Request Routing
//
// Project: Munchbox / Author: Alex Freidah
//
// HTTP server and request router for S3-compatible operations. Implements a
// subset of the S3 API sufficient for basic object storage: PUT, GET, HEAD,
// DELETE. Routes requests to the appropriate handler based on method and query
// parameters.
// -------------------------------------------------------------------------------

package server

import (
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/munchbox/s3-proxy/internal/auth"
	"github.com/munchbox/s3-proxy/internal/config"
	"github.com/munchbox/s3-proxy/internal/storage"
	"github.com/munchbox/s3-proxy/internal/telemetry"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

// -------------------------------------------------------------------------
// SERVER
// -------------------------------------------------------------------------

// Server handles HTTP requests and routes them to the backend manager.
type Server struct {
	Manager       *storage.BackendManager
	VirtualBucket string
	AuthConfig    config.AuthConfig
	MaxObjectSize int64 // Max upload body size in bytes
}

// ServeHTTP implements http.Handler.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	method := r.Method

	// --- Track inflight requests ---
	telemetry.InflightRequests.WithLabelValues(method).Inc()
	defer telemetry.InflightRequests.WithLabelValues(method).Dec()

	// --- Auth check ---
	if auth.NeedsAuth(s.AuthConfig) {
		if err := auth.Authenticate(r, s.AuthConfig); err != nil {
			s.recordRequest(method, http.StatusForbidden, start, 0, 0)
			slog.Warn("Auth failed", "method", method, "path", r.URL.Path, "remote", r.RemoteAddr, "error", err)
			writeS3Error(w, http.StatusForbidden, "AccessDenied", "Access denied")
			return
		}
	}

	// --- Parse path ---
	bucket, key, ok := parsePath(r.URL.Path)
	if !ok {
		s.recordRequest(method, http.StatusBadRequest, start, 0, 0)
		slog.Warn("Invalid path", "method", method, "path", r.URL.Path, "remote", r.RemoteAddr)
		writeS3Error(w, http.StatusBadRequest, "InvalidRequest", "Invalid path format")
		return
	}

	// --- Verify bucket name ---
	if bucket != s.VirtualBucket {
		s.recordRequest(method, http.StatusNotFound, start, 0, 0)
		slog.Warn("Unknown bucket", "method", method, "path", r.URL.Path, "remote", r.RemoteAddr, "bucket", bucket)
		writeS3Error(w, http.StatusNotFound, "NoSuchBucket", fmt.Sprintf("Bucket %s not found", bucket))
		return
	}

	// --- Start tracing span ---
	ctx, span := telemetry.StartSpan(r.Context(), fmt.Sprintf("HTTP %s", method),
		telemetry.RequestAttributes(method, r.URL.Path, bucket, key, r.RemoteAddr)...,
	)
	defer span.End()

	// --- Route by method ---
	var status int
	var err error
	var requestSize, responseSize int64

	// --- Bucket-level operations (no key) ---
	if key == "" {
		if method != http.MethodGet {
			s.recordRequest(method, http.StatusMethodNotAllowed, start, 0, 0)
			writeS3Error(w, http.StatusMethodNotAllowed, "MethodNotAllowed", "Method not supported for bucket")
			span.SetStatus(codes.Error, "method not allowed for bucket")
			return
		}
		status, err = s.handleListObjectsV2(ctx, w, r, bucket)
	} else {
		// --- Multipart upload routing ---
		query := r.URL.Query()
		_, hasUploads := query["uploads"]
		uploadID := query.Get("uploadId")

		if hasUploads && method == http.MethodPost {
			status, err = s.handleCreateMultipartUpload(ctx, w, r, bucket, key)
		} else if uploadID != "" {
			switch method {
			case http.MethodPut:
				requestSize = r.ContentLength
				status, err = s.handleUploadPart(ctx, w, r, key)
			case http.MethodPost:
				status, err = s.handleCompleteMultipartUpload(ctx, w, r, bucket, key)
			case http.MethodDelete:
				status, err = s.handleAbortMultipartUpload(ctx, w, uploadID)
			case http.MethodGet:
				status, err = s.handleListParts(ctx, w, r, bucket, key)
			default:
				s.recordRequest(method, http.StatusMethodNotAllowed, start, 0, 0)
				writeS3Error(w, http.StatusMethodNotAllowed, "MethodNotAllowed", "Method not supported")
				span.SetStatus(codes.Error, "method not allowed")
				return
			}
		} else {
			switch method {
			case http.MethodPut:
				if copySource := r.Header.Get("X-Amz-Copy-Source"); copySource != "" {
					status, err = s.handleCopyObject(ctx, w, r, bucket, key, copySource)
				} else {
					requestSize = r.ContentLength
					status, err = s.handlePut(ctx, w, r, key)
				}
			case http.MethodGet:
				status, responseSize, err = s.handleGet(ctx, w, r, key)
			case http.MethodHead:
				status, err = s.handleHead(ctx, w, r, key)
			case http.MethodDelete:
				status, err = s.handleDelete(ctx, w, r, key)
			default:
				s.recordRequest(method, http.StatusMethodNotAllowed, start, 0, 0)
				slog.Warn("Method not allowed", "method", method, "path", r.URL.Path, "remote", r.RemoteAddr)
				writeS3Error(w, http.StatusMethodNotAllowed, "MethodNotAllowed", "Method not supported")
				span.SetStatus(codes.Error, "method not allowed")
				return
			}
		}
	}

	// --- Record metrics ---
	s.recordRequest(method, status, start, requestSize, responseSize)

	// --- Update span status ---
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		span.RecordError(err)
	}
	span.SetAttributes(attribute.Int("http.status_code", status))

	// --- Log request ---
	elapsed := time.Since(start)
	logAttrs := []any{"method", method, "path", r.URL.Path, "remote", r.RemoteAddr, "status", status, "duration", elapsed}
	if err != nil {
		slog.Error("Request failed", append(logAttrs, "error", err)...)
	} else {
		slog.Info("Request completed", logAttrs...)
	}
}

// recordRequest updates Prometheus metrics for a completed request.
func (s *Server) recordRequest(method string, status int, start time.Time, reqSize, respSize int64) {
	statusStr := strconv.Itoa(status)
	telemetry.RequestsTotal.WithLabelValues(method, statusStr).Inc()
	telemetry.RequestDuration.WithLabelValues(method).Observe(time.Since(start).Seconds())

	if reqSize > 0 {
		telemetry.RequestSize.WithLabelValues(method).Observe(float64(reqSize))
	}
	if respSize > 0 {
		telemetry.ResponseSize.WithLabelValues(method).Observe(float64(respSize))
	}
}
