// -------------------------------------------------------------------------------
// Multipart Upload Handlers - S3 Multipart Upload Protocol
//
// Project: Munchbox / Author: Alex Freidah
//
// HTTP handlers for S3 multipart upload operations. Supports creating uploads,
// uploading parts, completing uploads (reassembly), aborting uploads, and listing
// parts. Parts are stored under temporary keys and concatenated on completion.
// -------------------------------------------------------------------------------

package server

import (
	"context"
	"encoding/xml"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"time"
)

// -------------------------------------------------------------------------
// XML TYPES
// -------------------------------------------------------------------------

// initiateMultipartUploadResult is the XML response for CreateMultipartUpload.
type initiateMultipartUploadResult struct {
	XMLName  xml.Name `xml:"InitiateMultipartUploadResult"`
	Xmlns    string   `xml:"xmlns,attr"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	UploadId string   `xml:"UploadId"`
}

// completeMultipartUploadRequest is the XML request body for CompleteMultipartUpload.
type completeMultipartUploadRequest struct {
	Parts []completePart `xml:"Part"`
}

// completePart identifies a part in a CompleteMultipartUpload request.
type completePart struct {
	PartNumber int    `xml:"PartNumber"`
	ETag       string `xml:"ETag"`
}

// completeMultipartUploadResult is the XML response for CompleteMultipartUpload.
type completeMultipartUploadResult struct {
	XMLName xml.Name `xml:"CompleteMultipartUploadResult"`
	Xmlns   string   `xml:"xmlns,attr"`
	Bucket  string   `xml:"Bucket"`
	Key     string   `xml:"Key"`
	ETag    string   `xml:"ETag"`
}

// listPartsResult is the XML response for ListParts.
type listPartsResult struct {
	XMLName  xml.Name   `xml:"ListPartsResult"`
	Xmlns    string     `xml:"xmlns,attr"`
	Bucket   string     `xml:"Bucket"`
	Key      string     `xml:"Key"`
	UploadId string     `xml:"UploadId"`
	Parts    []partInfo `xml:"Part"`
}

// partInfo holds part metadata for the ListParts response.
type partInfo struct {
	PartNumber   int    `xml:"PartNumber"`
	ETag         string `xml:"ETag"`
	Size         int64  `xml:"Size"`
	LastModified string `xml:"LastModified"`
}

// -------------------------------------------------------------------------
// HANDLERS
// -------------------------------------------------------------------------

// handleCreateMultipartUpload handles POST /{bucket}/{key}?uploads
func (s *Server) handleCreateMultipartUpload(ctx context.Context, w http.ResponseWriter, r *http.Request, bucket, key string) (int, error) {
	contentType := r.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	uploadID, _, err := s.Manager.CreateMultipartUpload(ctx, key, contentType)
	if err != nil {
		writeS3Error(w, http.StatusInternalServerError, "InternalError", "Failed to create multipart upload")
		return http.StatusInternalServerError, err
	}

	result := initiateMultipartUploadResult{
		Xmlns:    "http://s3.amazonaws.com/doc/2006-03-01/",
		Bucket:   bucket,
		Key:      key,
		UploadId: uploadID,
	}

	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, xml.Header)
	xml.NewEncoder(w).Encode(result)
	return http.StatusOK, nil
}

// handleUploadPart handles PUT /{bucket}/{key}?partNumber=N&uploadId=X
func (s *Server) handleUploadPart(ctx context.Context, w http.ResponseWriter, r *http.Request, key string) (int, error) {
	uploadID := r.URL.Query().Get("uploadId")
	partNumberStr := r.URL.Query().Get("partNumber")

	partNumber, err := strconv.Atoi(partNumberStr)
	if err != nil || partNumber < 1 {
		writeS3Error(w, http.StatusBadRequest, "InvalidArgument", "Invalid part number")
		return http.StatusBadRequest, fmt.Errorf("invalid part number: %s", partNumberStr)
	}

	if r.ContentLength < 0 {
		writeS3Error(w, http.StatusLengthRequired, "MissingContentLength", "Content-Length required")
		return http.StatusLengthRequired, fmt.Errorf("missing Content-Length for part upload")
	}

	etag, err := s.Manager.UploadPart(ctx, uploadID, partNumber, r.Body, r.ContentLength)
	if err != nil {
		writeS3Error(w, http.StatusInternalServerError, "InternalError", "Failed to upload part")
		return http.StatusInternalServerError, err
	}

	w.Header().Set("ETag", etag)
	w.WriteHeader(http.StatusOK)
	return http.StatusOK, nil
}

// handleCompleteMultipartUpload handles POST /{bucket}/{key}?uploadId=X
func (s *Server) handleCompleteMultipartUpload(ctx context.Context, w http.ResponseWriter, r *http.Request, bucket, key string) (int, error) {
	uploadID := r.URL.Query().Get("uploadId")

	var req completeMultipartUploadRequest
	if err := xml.NewDecoder(r.Body).Decode(&req); err != nil {
		writeS3Error(w, http.StatusBadRequest, "MalformedXML", "Failed to parse request body")
		return http.StatusBadRequest, fmt.Errorf("failed to decode complete request: %w", err)
	}

	var partNumbers []int
	for _, p := range req.Parts {
		partNumbers = append(partNumbers, p.PartNumber)
	}

	etag, err := s.Manager.CompleteMultipartUpload(ctx, uploadID, partNumbers)
	if err != nil {
		writeS3Error(w, http.StatusInternalServerError, "InternalError", "Failed to complete multipart upload")
		return http.StatusInternalServerError, err
	}

	result := completeMultipartUploadResult{
		Xmlns:  "http://s3.amazonaws.com/doc/2006-03-01/",
		Bucket: bucket,
		Key:    key,
		ETag:   etag,
	}

	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, xml.Header)
	xml.NewEncoder(w).Encode(result)
	return http.StatusOK, nil
}

// handleAbortMultipartUpload handles DELETE /{bucket}/{key}?uploadId=X
func (s *Server) handleAbortMultipartUpload(ctx context.Context, w http.ResponseWriter, uploadID string) (int, error) {
	err := s.Manager.AbortMultipartUpload(ctx, uploadID)
	if err != nil {
		log.Printf("Abort multipart upload %s failed: %v", uploadID, err)
	}

	w.WriteHeader(http.StatusNoContent)
	return http.StatusNoContent, nil
}

// handleListParts handles GET /{bucket}/{key}?uploadId=X
func (s *Server) handleListParts(ctx context.Context, w http.ResponseWriter, r *http.Request, bucket, key string) (int, error) {
	uploadID := r.URL.Query().Get("uploadId")

	parts, err := s.Manager.Store().GetParts(ctx, uploadID)
	if err != nil {
		writeS3Error(w, http.StatusInternalServerError, "InternalError", "Failed to list parts")
		return http.StatusInternalServerError, err
	}

	result := listPartsResult{
		Xmlns:    "http://s3.amazonaws.com/doc/2006-03-01/",
		Bucket:   bucket,
		Key:      key,
		UploadId: uploadID,
	}

	for _, p := range parts {
		result.Parts = append(result.Parts, partInfo{
			PartNumber:   p.PartNumber,
			ETag:         p.ETag,
			Size:         p.SizeBytes,
			LastModified: p.CreatedAt.UTC().Format(time.RFC3339),
		})
	}

	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, xml.Header)
	xml.NewEncoder(w).Encode(result)
	return http.StatusOK, nil
}
