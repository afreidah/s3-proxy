// -------------------------------------------------------------------------------
// Helpers - Path Parsing and Error Formatting
//
// Project: Munchbox / Author: Alex Freidah
//
// Utility functions for the server package. Handles URL path parsing for S3-style
// bucket/key extraction and S3-compatible XML error response formatting.
// -------------------------------------------------------------------------------

package server

import (
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/munchbox/s3-proxy/internal/storage"
)

// parsePath extracts bucket and key from the URL path.
// Expected format: /{bucket} or /{bucket}/{key...}
// When no key is present, key is empty (used for bucket-level operations like
// ListObjectsV2).
func parsePath(path string) (bucket string, key string, ok bool) {
	path = strings.TrimPrefix(path, "/")
	if path == "" {
		return "", "", false
	}
	parts := strings.SplitN(path, "/", 2)
	if parts[0] == "" {
		return "", "", false
	}
	if len(parts) == 1 || parts[1] == "" {
		return parts[0], "", true
	}
	return parts[0], parts[1], true
}

// writeS3Error sends an S3-style XML error response.
func writeS3Error(w http.ResponseWriter, code int, errCode, message string) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(code)
	_, _ = fmt.Fprintf(w, `<?xml version="1.0" encoding="UTF-8"?>
<Error>
  <Code>%s</Code>
  <Message>%s</Message>
</Error>`, xmlEscape(errCode), xmlEscape(message))
}

// xmlReplacer escapes special XML characters. Allocated once at package level
// to avoid per-call allocation.
var xmlReplacer = strings.NewReplacer(
	"&", "&amp;",
	"<", "&lt;",
	">", "&gt;",
	`"`, "&quot;",
	"'", "&apos;",
)

// xmlEscape escapes special XML characters.
func xmlEscape(s string) string {
	return xmlReplacer.Replace(s)
}

// writeStorageError checks if err is an *storage.S3Error and writes the
// appropriate S3 XML error response. Falls back to 502 InternalError for
// untyped errors. Returns the HTTP status code used.
func writeStorageError(w http.ResponseWriter, err error, fallbackMsg string) int {
	var s3err *storage.S3Error
	if errors.As(err, &s3err) {
		writeS3Error(w, s3err.StatusCode, s3err.Code, s3err.Message)
		return s3err.StatusCode
	}
	writeS3Error(w, http.StatusBadGateway, "InternalError", fallbackMsg)
	return http.StatusBadGateway
}

// writeXML writes an S3-compatible XML response with the standard XML header.
func writeXML(w http.ResponseWriter, status int, v any) error {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	_, _ = fmt.Fprint(w, xml.Header)
	return xml.NewEncoder(w).Encode(v)
}
