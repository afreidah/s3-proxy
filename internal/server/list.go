// -------------------------------------------------------------------------------
// List Handler - S3 ListObjectsV2
//
// Project: Munchbox / Author: Alex Freidah
//
// HTTP handler for the S3 ListObjectsV2 operation. Returns XML responses
// compatible with S3 clients, supporting prefix filtering, delimiter-based
// directory grouping, and pagination via continuation tokens.
// -------------------------------------------------------------------------------

package server

import (
	"context"
	"encoding/xml"
	"fmt"
	"net/http"
	"strconv"
	"time"
)

// handleListObjectsV2 processes GET requests at the bucket level, returning an
// S3-compatible ListObjectsV2 XML response.
func (s *Server) handleListObjectsV2(ctx context.Context, w http.ResponseWriter, r *http.Request, bucket string) (int, error) {
	prefix := r.URL.Query().Get("prefix")
	delimiter := r.URL.Query().Get("delimiter")
	continuationToken := r.URL.Query().Get("continuation-token")
	maxKeysStr := r.URL.Query().Get("max-keys")

	maxKeys := 1000
	if maxKeysStr != "" {
		if mk, err := strconv.Atoi(maxKeysStr); err == nil && mk > 0 && mk <= 1000 {
			maxKeys = mk
		}
	}

	startAfter := r.URL.Query().Get("start-after")
	if continuationToken != "" {
		startAfter = continuationToken
	}

	result, err := s.Manager.ListObjects(ctx, prefix, delimiter, startAfter, maxKeys)
	if err != nil {
		writeS3Error(w, http.StatusInternalServerError, "InternalError", "Failed to list objects")
		return http.StatusInternalServerError, err
	}

	type xmlContent struct {
		Key          string `xml:"Key"`
		Size         int64  `xml:"Size"`
		LastModified string `xml:"LastModified"`
	}

	type xmlCommonPrefix struct {
		Prefix string `xml:"Prefix"`
	}

	type xmlListResult struct {
		XMLName               xml.Name          `xml:"ListBucketResult"`
		Xmlns                 string            `xml:"xmlns,attr"`
		Name                  string            `xml:"Name"`
		Prefix                string            `xml:"Prefix"`
		Delimiter             string            `xml:"Delimiter,omitempty"`
		MaxKeys               int               `xml:"MaxKeys"`
		KeyCount              int               `xml:"KeyCount"`
		IsTruncated           bool              `xml:"IsTruncated"`
		ContinuationToken     string            `xml:"ContinuationToken,omitempty"`
		NextContinuationToken string            `xml:"NextContinuationToken,omitempty"`
		Contents              []xmlContent      `xml:"Contents"`
		CommonPrefixes        []xmlCommonPrefix `xml:"CommonPrefixes,omitempty"`
	}

	xmlResult := xmlListResult{
		Xmlns:                 "http://s3.amazonaws.com/doc/2006-03-01/",
		Name:                  bucket,
		Prefix:                prefix,
		Delimiter:             delimiter,
		MaxKeys:               maxKeys,
		KeyCount:              result.KeyCount,
		IsTruncated:           result.IsTruncated,
		NextContinuationToken: result.NextContinuationToken,
	}

	if continuationToken != "" {
		xmlResult.ContinuationToken = continuationToken
	}

	for _, obj := range result.Objects {
		xmlResult.Contents = append(xmlResult.Contents, xmlContent{
			Key:          obj.ObjectKey,
			Size:         obj.SizeBytes,
			LastModified: obj.CreatedAt.UTC().Format(time.RFC3339),
		})
	}

	for _, cp := range result.CommonPrefixes {
		xmlResult.CommonPrefixes = append(xmlResult.CommonPrefixes, xmlCommonPrefix{Prefix: cp})
	}

	if err := writeXML(w, http.StatusOK, xmlResult); err != nil {
		return http.StatusOK, fmt.Errorf("failed to encode list response: %w", err)
	}

	return http.StatusOK, nil
}
