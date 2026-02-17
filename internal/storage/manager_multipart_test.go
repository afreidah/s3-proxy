package storage

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"
)

// -------------------------------------------------------------------------
// CreateMultipartUpload
// -------------------------------------------------------------------------

func TestCreateMultipartUpload_Success(t *testing.T) {
	backend := newMockBackend()
	store := &mockStore{getBackendResp: "b1"}
	mgr := newTestManager(store, map[string]*mockBackend{"b1": backend})

	uploadID, backendName, err := mgr.CreateMultipartUpload(context.Background(), "multi/key", "application/zip")
	if err != nil {
		t.Fatalf("CreateMultipartUpload: %v", err)
	}
	if uploadID == "" {
		t.Error("expected non-empty upload ID")
	}
	if backendName != "b1" {
		t.Errorf("backend = %q, want %q", backendName, "b1")
	}
}

func TestCreateMultipartUpload_DBUnavailable(t *testing.T) {
	store := &mockStore{getBackendErr: ErrDBUnavailable}
	mgr := newTestManager(store, map[string]*mockBackend{"b1": newMockBackend()})

	_, _, err := mgr.CreateMultipartUpload(context.Background(), "key", "")
	if !errors.Is(err, ErrServiceUnavailable) {
		t.Fatalf("expected ErrServiceUnavailable, got %v", err)
	}
}

func TestCreateMultipartUpload_NoSpace(t *testing.T) {
	store := &mockStore{getBackendErr: ErrNoSpaceAvailable}
	mgr := newTestManager(store, map[string]*mockBackend{"b1": newMockBackend()})

	_, _, err := mgr.CreateMultipartUpload(context.Background(), "key", "")
	if !errors.Is(err, ErrInsufficientStorage) {
		t.Fatalf("expected ErrInsufficientStorage, got %v", err)
	}
}

// -------------------------------------------------------------------------
// UploadPart
// -------------------------------------------------------------------------

func TestUploadPart_Success(t *testing.T) {
	backend := newMockBackend()
	store := &mockStore{
		getMultipartResp: &MultipartUpload{
			UploadID:    "upload-1",
			ObjectKey:   "multi/key",
			BackendName: "b1",
			ContentType: "application/zip",
		},
	}
	mgr := newTestManager(store, map[string]*mockBackend{"b1": backend})

	etag, err := mgr.UploadPart(context.Background(), "upload-1", 1, bytes.NewReader([]byte("part-data")), 9)
	if err != nil {
		t.Fatalf("UploadPart: %v", err)
	}
	if etag == "" {
		t.Error("expected non-empty etag")
	}
	// Part should be stored under temp key
	if !backend.hasObject("__multipart/upload-1/1") {
		t.Error("part not found on backend")
	}
}

func TestUploadPart_DBUnavailable(t *testing.T) {
	store := &mockStore{getMultipartErr: ErrDBUnavailable}
	mgr := newTestManager(store, map[string]*mockBackend{"b1": newMockBackend()})

	_, err := mgr.UploadPart(context.Background(), "upload-1", 1, bytes.NewReader([]byte("x")), 1)
	if !errors.Is(err, ErrServiceUnavailable) {
		t.Fatalf("expected ErrServiceUnavailable, got %v", err)
	}
}

// -------------------------------------------------------------------------
// CompleteMultipartUpload
// -------------------------------------------------------------------------

func TestCompleteMultipartUpload_Success(t *testing.T) {
	backend := newMockBackend()

	// Pre-store parts on the backend
	ctx := context.Background()
	backend.PutObject(ctx, "__multipart/upload-1/1", bytes.NewReader([]byte("AAA")), 3, "application/octet-stream")
	backend.PutObject(ctx, "__multipart/upload-1/2", bytes.NewReader([]byte("BBB")), 3, "application/octet-stream")

	store := &mockStore{
		getMultipartResp: &MultipartUpload{
			UploadID:    "upload-1",
			ObjectKey:   "multi/key",
			BackendName: "b1",
			ContentType: "application/zip",
		},
		getPartsResp: []MultipartPart{
			{PartNumber: 1, ETag: "e1", SizeBytes: 3},
			{PartNumber: 2, ETag: "e2", SizeBytes: 3},
		},
	}
	mgr := newTestManager(store, map[string]*mockBackend{"b1": backend})

	etag, err := mgr.CompleteMultipartUpload(ctx, "upload-1", []int{1, 2})
	if err != nil {
		t.Fatalf("CompleteMultipartUpload: %v", err)
	}
	if etag == "" {
		t.Error("expected non-empty etag")
	}
	// Final object should exist
	if !backend.hasObject("multi/key") {
		t.Error("final object not found on backend")
	}
	// Part temp keys should be cleaned up
	if backend.hasObject("__multipart/upload-1/1") {
		t.Error("part 1 temp key should be deleted")
	}
	if backend.hasObject("__multipart/upload-1/2") {
		t.Error("part 2 temp key should be deleted")
	}
	// RecordObject should have been called
	if len(store.recordObjectCalls) != 1 {
		t.Fatalf("expected 1 RecordObject call, got %d", len(store.recordObjectCalls))
	}
	call := store.recordObjectCalls[0]
	if call.Key != "multi/key" || call.Backend != "b1" || call.Size != 6 {
		t.Errorf("RecordObject called with %+v", call)
	}
}

func TestCompleteMultipartUpload_DBUnavailable(t *testing.T) {
	store := &mockStore{getMultipartErr: ErrDBUnavailable}
	mgr := newTestManager(store, map[string]*mockBackend{"b1": newMockBackend()})

	_, err := mgr.CompleteMultipartUpload(context.Background(), "upload-1", []int{1})
	if !errors.Is(err, ErrServiceUnavailable) {
		t.Fatalf("expected ErrServiceUnavailable, got %v", err)
	}
}

// -------------------------------------------------------------------------
// AbortMultipartUpload
// -------------------------------------------------------------------------

func TestAbortMultipartUpload_Success(t *testing.T) {
	backend := newMockBackend()
	ctx := context.Background()
	backend.PutObject(ctx, "__multipart/upload-1/1", bytes.NewReader([]byte("AAA")), 3, "application/octet-stream")

	store := &mockStore{
		getMultipartResp: &MultipartUpload{
			UploadID:    "upload-1",
			ObjectKey:   "multi/key",
			BackendName: "b1",
		},
		getPartsResp: []MultipartPart{
			{PartNumber: 1, ETag: "e1", SizeBytes: 3, CreatedAt: time.Now()},
		},
	}
	mgr := newTestManager(store, map[string]*mockBackend{"b1": backend})

	err := mgr.AbortMultipartUpload(ctx, "upload-1")
	if err != nil {
		t.Fatalf("AbortMultipartUpload: %v", err)
	}
	// Part should be cleaned up
	if backend.hasObject("__multipart/upload-1/1") {
		t.Error("part temp key should be deleted")
	}
}

func TestAbortMultipartUpload_DBUnavailable(t *testing.T) {
	store := &mockStore{getMultipartErr: ErrDBUnavailable}
	mgr := newTestManager(store, map[string]*mockBackend{"b1": newMockBackend()})

	err := mgr.AbortMultipartUpload(context.Background(), "upload-1")
	if !errors.Is(err, ErrServiceUnavailable) {
		t.Fatalf("expected ErrServiceUnavailable, got %v", err)
	}
}
