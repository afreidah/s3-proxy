package storage

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"time"
)

// newTestManager creates a BackendManager with mock backends and store for testing.
func newTestManager(store *mockStore, backends map[string]*mockBackend) *BackendManager {
	obs := make(map[string]ObjectBackend, len(backends))
	var order []string
	for name, b := range backends {
		obs[name] = b
		order = append(order, name)
	}
	return NewBackendManager(obs, store, order, 5*time.Second, 30*time.Second)
}

// -------------------------------------------------------------------------
// PutObject
// -------------------------------------------------------------------------

func TestPutObject_Success(t *testing.T) {
	backend := newMockBackend()
	store := &mockStore{getBackendResp: "b1"}
	mgr := newTestManager(store, map[string]*mockBackend{"b1": backend})

	etag, err := mgr.PutObject(context.Background(), "mykey", bytes.NewReader([]byte("hello")), 5, "text/plain")
	if err != nil {
		t.Fatalf("PutObject: %v", err)
	}
	if etag == "" {
		t.Error("expected non-empty etag")
	}
	if !backend.hasObject("mykey") {
		t.Error("object not found on backend")
	}
	if len(store.recordObjectCalls) != 1 {
		t.Fatalf("expected 1 RecordObject call, got %d", len(store.recordObjectCalls))
	}
	call := store.recordObjectCalls[0]
	if call.Key != "mykey" || call.Backend != "b1" || call.Size != 5 {
		t.Errorf("RecordObject called with %+v", call)
	}
}

func TestPutObject_QuotaExhausted(t *testing.T) {
	store := &mockStore{getBackendErr: ErrNoSpaceAvailable}
	mgr := newTestManager(store, map[string]*mockBackend{"b1": newMockBackend()})

	_, err := mgr.PutObject(context.Background(), "key", bytes.NewReader([]byte("x")), 1, "")
	if !errors.Is(err, ErrInsufficientStorage) {
		t.Fatalf("expected ErrInsufficientStorage, got %v", err)
	}
}

func TestPutObject_DBUnavailable(t *testing.T) {
	store := &mockStore{getBackendErr: ErrDBUnavailable}
	mgr := newTestManager(store, map[string]*mockBackend{"b1": newMockBackend()})

	_, err := mgr.PutObject(context.Background(), "key", bytes.NewReader([]byte("x")), 1, "")
	if !errors.Is(err, ErrServiceUnavailable) {
		t.Fatalf("expected ErrServiceUnavailable, got %v", err)
	}
}

func TestPutObject_RecordFailure_CleansUp(t *testing.T) {
	backend := newMockBackend()
	store := &mockStore{
		getBackendResp:  "b1",
		recordObjectErr: errors.New("db write failed"),
	}
	mgr := newTestManager(store, map[string]*mockBackend{"b1": backend})

	_, err := mgr.PutObject(context.Background(), "cleanup-key", bytes.NewReader([]byte("data")), 4, "")
	if err == nil {
		t.Fatal("expected error from RecordObject failure")
	}
	// Backend object should be cleaned up
	if backend.hasObject("cleanup-key") {
		t.Error("orphaned object should have been deleted from backend")
	}
}

// -------------------------------------------------------------------------
// GetObject
// -------------------------------------------------------------------------

func TestGetObject_Success(t *testing.T) {
	backend := newMockBackend()
	_, _ = backend.PutObject(context.Background(), "key", bytes.NewReader([]byte("hello")), 5, "text/plain")

	store := &mockStore{
		getAllLocationsResp: []ObjectLocation{{ObjectKey: "key", BackendName: "b1"}},
	}
	mgr := newTestManager(store, map[string]*mockBackend{"b1": backend})

	result, err := mgr.GetObject(context.Background(), "key", "")
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	defer func() { _ = result.Body.Close() }()
	if result.Size != 5 {
		t.Errorf("size = %d, want 5", result.Size)
	}
	if result.ContentType != "text/plain" {
		t.Errorf("content-type = %q, want %q", result.ContentType, "text/plain")
	}
	got, _ := io.ReadAll(result.Body)
	if string(got) != "hello" {
		t.Errorf("body = %q, want %q", got, "hello")
	}
}

func TestGetObject_NotFound(t *testing.T) {
	store := &mockStore{getAllLocationsErr: ErrObjectNotFound}
	mgr := newTestManager(store, map[string]*mockBackend{"b1": newMockBackend()})

	_, err := mgr.GetObject(context.Background(), "missing", "")
	if !errors.Is(err, ErrObjectNotFound) {
		t.Fatalf("expected ErrObjectNotFound, got %v", err)
	}
}

func TestGetObject_FailoverToReplica(t *testing.T) {
	primary := newMockBackend()
	primary.getErr = errors.New("backend down") // primary fails
	replica := newMockBackend()
	_, _ = replica.PutObject(context.Background(), "key", bytes.NewReader([]byte("data")), 4, "text/plain")

	store := &mockStore{
		getAllLocationsResp: []ObjectLocation{
			{ObjectKey: "key", BackendName: "primary"},
			{ObjectKey: "key", BackendName: "replica"},
		},
	}
	mgr := newTestManager(store, map[string]*mockBackend{"primary": primary, "replica": replica})

	result, err := mgr.GetObject(context.Background(), "key", "")
	if err != nil {
		t.Fatalf("GetObject should failover: %v", err)
	}
	defer func() { _ = result.Body.Close() }()
	got, _ := io.ReadAll(result.Body)
	if string(got) != "data" {
		t.Errorf("body = %q, want %q", got, "data")
	}
}

func TestGetObject_DBUnavailable_BroadcastHit(t *testing.T) {
	backend := newMockBackend()
	_, _ = backend.PutObject(context.Background(), "key", bytes.NewReader([]byte("broadcast")), 9, "text/plain")

	store := &mockStore{getAllLocationsErr: ErrDBUnavailable}
	mgr := newTestManager(store, map[string]*mockBackend{"b1": backend})

	result, err := mgr.GetObject(context.Background(), "key", "")
	if err != nil {
		t.Fatalf("GetObject broadcast should succeed: %v", err)
	}
	defer func() { _ = result.Body.Close() }()
	got, _ := io.ReadAll(result.Body)
	if string(got) != "broadcast" {
		t.Errorf("body = %q, want %q", got, "broadcast")
	}
}

func TestGetObject_DBUnavailable_CacheHit(t *testing.T) {
	b1 := newMockBackend()
	b2 := newMockBackend()
	_, _ = b2.PutObject(context.Background(), "cached-key", bytes.NewReader([]byte("cached")), 6, "text/plain")

	store := &mockStore{getAllLocationsErr: ErrDBUnavailable}
	mgr := newTestManager(store, map[string]*mockBackend{"b1": b1, "b2": b2})

	// First call populates cache via broadcast
	r1, err := mgr.GetObject(context.Background(), "cached-key", "")
	if err != nil {
		t.Fatalf("first GetObject: %v", err)
	}
	_ = r1.Body.Close()

	// Second call should use cache
	r2, err := mgr.GetObject(context.Background(), "cached-key", "")
	if err != nil {
		t.Fatalf("second GetObject (cache hit): %v", err)
	}
	defer func() { _ = r2.Body.Close() }()
	got, _ := io.ReadAll(r2.Body)
	if string(got) != "cached" {
		t.Errorf("body = %q, want %q", got, "cached")
	}
}

func TestGetObject_DBUnavailable_AllFail(t *testing.T) {
	b1 := newMockBackend() // empty — no object
	b2 := newMockBackend() // empty — no object

	store := &mockStore{getAllLocationsErr: ErrDBUnavailable}
	mgr := newTestManager(store, map[string]*mockBackend{"b1": b1, "b2": b2})

	_, err := mgr.GetObject(context.Background(), "nowhere", "")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	// Should NOT be ErrObjectNotFound — real backend errors should propagate
	// so the server maps them to 502 instead of a misleading 404.
	if errors.Is(err, ErrObjectNotFound) {
		t.Fatal("should not mask backend errors as ErrObjectNotFound")
	}
}

// -------------------------------------------------------------------------
// HeadObject
// -------------------------------------------------------------------------

func TestHeadObject_Success(t *testing.T) {
	backend := newMockBackend()
	_, _ = backend.PutObject(context.Background(), "key", bytes.NewReader([]byte("headme")), 6, "application/json")

	store := &mockStore{
		getAllLocationsResp: []ObjectLocation{{ObjectKey: "key", BackendName: "b1"}},
	}
	mgr := newTestManager(store, map[string]*mockBackend{"b1": backend})

	size, ct, etag, err := mgr.HeadObject(context.Background(), "key")
	if err != nil {
		t.Fatalf("HeadObject: %v", err)
	}
	if size != 6 {
		t.Errorf("size = %d, want 6", size)
	}
	if ct != "application/json" {
		t.Errorf("content-type = %q", ct)
	}
	if etag == "" {
		t.Error("expected non-empty etag")
	}
}

func TestHeadObject_DBUnavailable_Broadcast(t *testing.T) {
	backend := newMockBackend()
	_, _ = backend.PutObject(context.Background(), "key", bytes.NewReader([]byte("head")), 4, "text/plain")

	store := &mockStore{getAllLocationsErr: ErrDBUnavailable}
	mgr := newTestManager(store, map[string]*mockBackend{"b1": backend})

	size, _, _, err := mgr.HeadObject(context.Background(), "key")
	if err != nil {
		t.Fatalf("HeadObject broadcast should succeed: %v", err)
	}
	if size != 4 {
		t.Errorf("size = %d, want 4", size)
	}
}

// -------------------------------------------------------------------------
// DeleteObject
// -------------------------------------------------------------------------

func TestDeleteObject_Success(t *testing.T) {
	backend := newMockBackend()
	_, _ = backend.PutObject(context.Background(), "del-key", bytes.NewReader([]byte("rm")), 2, "")

	store := &mockStore{
		deleteObjectResp: []DeletedCopy{{BackendName: "b1", SizeBytes: 2}},
	}
	mgr := newTestManager(store, map[string]*mockBackend{"b1": backend})

	err := mgr.DeleteObject(context.Background(), "del-key")
	if err != nil {
		t.Fatalf("DeleteObject: %v", err)
	}
	if backend.hasObject("del-key") {
		t.Error("object should be deleted from backend")
	}
}

func TestDeleteObject_NotFound_Idempotent(t *testing.T) {
	store := &mockStore{deleteObjectErr: ErrObjectNotFound}
	mgr := newTestManager(store, map[string]*mockBackend{"b1": newMockBackend()})

	err := mgr.DeleteObject(context.Background(), "nonexistent")
	if err != nil {
		t.Fatalf("DeleteObject of nonexistent key should succeed (idempotent): %v", err)
	}
}

func TestDeleteObject_DBUnavailable(t *testing.T) {
	store := &mockStore{deleteObjectErr: ErrDBUnavailable}
	mgr := newTestManager(store, map[string]*mockBackend{"b1": newMockBackend()})

	err := mgr.DeleteObject(context.Background(), "key")
	if !errors.Is(err, ErrServiceUnavailable) {
		t.Fatalf("expected ErrServiceUnavailable, got %v", err)
	}
}

// -------------------------------------------------------------------------
// CopyObject
// -------------------------------------------------------------------------

func TestCopyObject_Success(t *testing.T) {
	backend := newMockBackend()
	_, _ = backend.PutObject(context.Background(), "src", bytes.NewReader([]byte("copy-me")), 7, "text/plain")

	store := &mockStore{
		getAllLocationsResp: []ObjectLocation{{ObjectKey: "src", BackendName: "b1"}},
		getBackendResp:     "b1",
	}
	mgr := newTestManager(store, map[string]*mockBackend{"b1": backend})

	etag, err := mgr.CopyObject(context.Background(), "src", "dst")
	if err != nil {
		t.Fatalf("CopyObject: %v", err)
	}
	if etag == "" {
		t.Error("expected non-empty etag")
	}
	if !backend.hasObject("dst") {
		t.Error("destination object not found")
	}
}

func TestCopyObject_SourceNotFound(t *testing.T) {
	store := &mockStore{getAllLocationsErr: ErrObjectNotFound}
	mgr := newTestManager(store, map[string]*mockBackend{"b1": newMockBackend()})

	_, err := mgr.CopyObject(context.Background(), "missing", "dst")
	if !errors.Is(err, ErrObjectNotFound) {
		t.Fatalf("expected ErrObjectNotFound, got %v", err)
	}
}

func TestCopyObject_DBUnavailable_SourceLookup(t *testing.T) {
	store := &mockStore{getAllLocationsErr: ErrDBUnavailable}
	mgr := newTestManager(store, map[string]*mockBackend{"b1": newMockBackend()})

	_, err := mgr.CopyObject(context.Background(), "src", "dst")
	if !errors.Is(err, ErrServiceUnavailable) {
		t.Fatalf("expected ErrServiceUnavailable, got %v", err)
	}
}

func TestCopyObject_DBUnavailable_DestLookup(t *testing.T) {
	backend := newMockBackend()
	_, _ = backend.PutObject(context.Background(), "src", bytes.NewReader([]byte("data")), 4, "text/plain")

	store := &mockStore{
		getAllLocationsResp: []ObjectLocation{{ObjectKey: "src", BackendName: "b1"}},
		getBackendErr:      ErrDBUnavailable,
	}
	mgr := newTestManager(store, map[string]*mockBackend{"b1": backend})

	_, err := mgr.CopyObject(context.Background(), "src", "dst")
	if !errors.Is(err, ErrServiceUnavailable) {
		t.Fatalf("expected ErrServiceUnavailable, got %v", err)
	}
}

// -------------------------------------------------------------------------
// ListObjects
// -------------------------------------------------------------------------

func TestListObjects_Success(t *testing.T) {
	store := &mockStore{
		listObjectsResp: &ListObjectsResult{
			Objects: []ObjectLocation{
				{ObjectKey: "a/1", BackendName: "b1", SizeBytes: 10},
				{ObjectKey: "a/2", BackendName: "b1", SizeBytes: 20},
			},
		},
	}
	mgr := newTestManager(store, map[string]*mockBackend{"b1": newMockBackend()})

	result, err := mgr.ListObjects(context.Background(), "a/", "", "", 1000)
	if err != nil {
		t.Fatalf("ListObjects: %v", err)
	}
	if len(result.Objects) != 2 {
		t.Errorf("got %d objects, want 2", len(result.Objects))
	}
	if result.KeyCount != 2 {
		t.Errorf("KeyCount = %d, want 2", result.KeyCount)
	}
}

func TestListObjects_WithDelimiter(t *testing.T) {
	store := &mockStore{
		listObjectsResp: &ListObjectsResult{
			Objects: []ObjectLocation{
				{ObjectKey: "photos/2024/a.jpg", BackendName: "b1"},
				{ObjectKey: "photos/2024/b.jpg", BackendName: "b1"},
				{ObjectKey: "photos/2025/c.jpg", BackendName: "b1"},
				{ObjectKey: "photos/top.jpg", BackendName: "b1"},
			},
		},
	}
	mgr := newTestManager(store, map[string]*mockBackend{"b1": newMockBackend()})

	result, err := mgr.ListObjects(context.Background(), "photos/", "/", "", 1000)
	if err != nil {
		t.Fatalf("ListObjects: %v", err)
	}
	// "photos/top.jpg" is a direct child (no more delimiters)
	// "photos/2024/" and "photos/2025/" are common prefixes
	if len(result.Objects) != 1 {
		t.Errorf("got %d objects, want 1", len(result.Objects))
	}
	if len(result.CommonPrefixes) != 2 {
		t.Errorf("got %d common prefixes, want 2", len(result.CommonPrefixes))
	}
	if result.KeyCount != 3 {
		t.Errorf("KeyCount = %d, want 3", result.KeyCount)
	}
}

func TestListObjects_DelimiterPagination(t *testing.T) {
	// Many objects collapse into one common prefix per page. The manager
	// must loop-fetch from the store to fill the requested maxKeys.
	store := &mockStore{
		listObjectsPages: []ListObjectsResult{
			{
				Objects: []ObjectLocation{
					{ObjectKey: "dir/a/1", BackendName: "b1"},
					{ObjectKey: "dir/a/2", BackendName: "b1"},
					{ObjectKey: "dir/a/3", BackendName: "b1"},
				},
				IsTruncated: true,
			},
			{
				Objects: []ObjectLocation{
					{ObjectKey: "dir/b/1", BackendName: "b1"},
					{ObjectKey: "dir/b/2", BackendName: "b1"},
				},
				IsTruncated: true,
			},
			{
				Objects: []ObjectLocation{
					{ObjectKey: "dir/c/1", BackendName: "b1"},
					{ObjectKey: "dir/top.txt", BackendName: "b1"},
				},
				IsTruncated: false,
			},
		},
	}
	mgr := newTestManager(store, map[string]*mockBackend{"b1": newMockBackend()})

	// Request maxKeys=3 with delimiter "/". The first page produces 1 prefix
	// ("dir/a/"), the second produces 1 ("dir/b/"), the third produces
	// 1 prefix ("dir/c/") which fills 3.
	result, err := mgr.ListObjects(context.Background(), "dir/", "/", "", 3)
	if err != nil {
		t.Fatalf("ListObjects: %v", err)
	}
	if result.KeyCount != 3 {
		t.Errorf("KeyCount = %d, want 3 (full page)", result.KeyCount)
	}
	if len(result.CommonPrefixes) != 3 {
		t.Errorf("CommonPrefixes = %v, want 3 entries", result.CommonPrefixes)
	}
	// There are still objects remaining, so IsTruncated should be true
	if !result.IsTruncated {
		t.Error("expected IsTruncated=true since dir/top.txt remains")
	}
}

func TestListObjects_DelimiterDedup(t *testing.T) {
	// Objects in the same virtual directory across pages should not produce
	// duplicate common prefixes.
	store := &mockStore{
		listObjectsPages: []ListObjectsResult{
			{
				Objects: []ObjectLocation{
					{ObjectKey: "p/a/1", BackendName: "b1"},
					{ObjectKey: "p/a/2", BackendName: "b1"},
				},
				IsTruncated: true,
			},
			{
				// Same prefix continues into the next page
				Objects: []ObjectLocation{
					{ObjectKey: "p/a/3", BackendName: "b1"},
					{ObjectKey: "p/b/1", BackendName: "b1"},
				},
				IsTruncated: false,
			},
		},
	}
	mgr := newTestManager(store, map[string]*mockBackend{"b1": newMockBackend()})

	result, err := mgr.ListObjects(context.Background(), "p/", "/", "", 1000)
	if err != nil {
		t.Fatalf("ListObjects: %v", err)
	}
	if len(result.CommonPrefixes) != 2 {
		t.Errorf("CommonPrefixes = %v, want [p/a/ p/b/]", result.CommonPrefixes)
	}
	if result.KeyCount != 2 {
		t.Errorf("KeyCount = %d, want 2", result.KeyCount)
	}
}

func TestListObjects_DBUnavailable(t *testing.T) {
	store := &mockStore{listObjectsErr: ErrDBUnavailable}
	mgr := newTestManager(store, map[string]*mockBackend{"b1": newMockBackend()})

	_, err := mgr.ListObjects(context.Background(), "", "", "", 1000)
	if err == nil {
		t.Fatal("expected error")
	}
	var s3err *S3Error
	if !errors.As(err, &s3err) {
		t.Fatalf("expected S3Error, got %T: %v", err, err)
	}
	if s3err.StatusCode != 503 {
		t.Errorf("StatusCode = %d, want 503", s3err.StatusCode)
	}
}

// -------------------------------------------------------------------------
// Backend Timeout
// -------------------------------------------------------------------------

func TestPutObject_BackendTimeout(t *testing.T) {
	backend := &mockBackend{
		objects: make(map[string]mockObject),
		putErr:  nil,
	}
	// Override PutObject behavior with a slow backend via a wrapper
	slowBackend := &slowMockBackend{mockBackend: backend, delay: 200 * time.Millisecond}

	store := &mockStore{getBackendResp: "b1"}
	obs := map[string]ObjectBackend{"b1": slowBackend}
	mgr := NewBackendManager(obs, store, []string{"b1"}, 5*time.Second, 50*time.Millisecond)

	_, err := mgr.PutObject(context.Background(), "timeout-key", bytes.NewReader([]byte("data")), 4, "text/plain")
	if err == nil {
		t.Fatal("expected timeout error, got nil")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context.DeadlineExceeded, got %v", err)
	}
}

// slowMockBackend wraps a mockBackend and adds a delay to PutObject.
type slowMockBackend struct {
	*mockBackend
	delay time.Duration
}

func (s *slowMockBackend) PutObject(ctx context.Context, key string, body io.Reader, size int64, contentType string) (string, error) {
	select {
	case <-time.After(s.delay):
		return s.mockBackend.PutObject(ctx, key, body, size, contentType)
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

// -------------------------------------------------------------------------
// Location Cache
// -------------------------------------------------------------------------

func TestLocationCache_SetAndGet(t *testing.T) {
	mgr := NewBackendManager(nil, nil, nil, 5*time.Second, 0)
	mgr.cacheSet("key1", "backend-a")

	got, ok := mgr.cacheGet("key1")
	if !ok {
		t.Fatal("expected cache hit")
	}
	if got != "backend-a" {
		t.Errorf("cached backend = %q, want %q", got, "backend-a")
	}
}

func TestLocationCache_Expiry(t *testing.T) {
	mgr := NewBackendManager(nil, nil, nil, 10*time.Millisecond, 0)
	mgr.cacheSet("key1", "backend-a")

	time.Sleep(15 * time.Millisecond)

	_, ok := mgr.cacheGet("key1")
	if ok {
		t.Fatal("expected cache miss after TTL")
	}
}

func TestLocationCache_Overwrite(t *testing.T) {
	mgr := NewBackendManager(nil, nil, nil, 5*time.Second, 0)
	mgr.cacheSet("key1", "old-backend")
	mgr.cacheSet("key1", "new-backend")

	got, ok := mgr.cacheGet("key1")
	if !ok {
		t.Fatal("expected cache hit")
	}
	if got != "new-backend" {
		t.Errorf("cached backend = %q, want %q", got, "new-backend")
	}
}
