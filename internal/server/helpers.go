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
	"fmt"
	"net/http"
	"strings"
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

// writeXML writes an S3-compatible XML response with the standard XML header.
func writeXML(w http.ResponseWriter, status int, v any) error {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	_, _ = fmt.Fprint(w, xml.Header)
	return xml.NewEncoder(w).Encode(v)
}
