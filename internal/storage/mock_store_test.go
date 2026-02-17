package storage

import (
	"context"
	"sync"
	"time"
)

// mockStore is a configurable MetadataStore mock for unit testing manager logic.
// Each method returns its pre-configured response/error. Call tracking fields
// allow assertions on what the manager called.
type mockStore struct {
	mu sync.Mutex

	// --- Configurable responses ---
	getAllLocationsResp []ObjectLocation
	getAllLocationsErr  error

	getBackendResp string
	getBackendErr  error

	recordObjectErr error

	deleteObjectResp []DeletedCopy
	deleteObjectErr  error

	listObjectsResp *ListObjectsResult
	listObjectsErr  error

	// Multipart
	createMultipartErr error
	getMultipartResp   *MultipartUpload
	getMultipartErr    error
	getPartsResp       []MultipartPart
	getPartsErr        error
	deleteMultipartErr error
	recordPartErr      error

	// --- Call tracking ---
	recordObjectCalls []recordObjectCall
	deleteObjectCalls []string
	callCount         int
}

type recordObjectCall struct {
	Key, Backend string
	Size         int64
}

var _ MetadataStore = (*mockStore)(nil)

func (m *mockStore) GetAllObjectLocations(_ context.Context, key string) ([]ObjectLocation, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.callCount++
	if m.getAllLocationsErr != nil {
		return nil, m.getAllLocationsErr
	}
	if m.getAllLocationsResp != nil {
		return m.getAllLocationsResp, nil
	}
	return nil, ErrObjectNotFound
}

func (m *mockStore) GetBackendWithSpace(_ context.Context, size int64, _ []string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.getBackendErr != nil {
		return "", m.getBackendErr
	}
	return m.getBackendResp, nil
}

func (m *mockStore) RecordObject(_ context.Context, key, backend string, size int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.recordObjectCalls = append(m.recordObjectCalls, recordObjectCall{Key: key, Backend: backend, Size: size})
	return m.recordObjectErr
}

func (m *mockStore) DeleteObject(_ context.Context, key string) ([]DeletedCopy, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.deleteObjectCalls = append(m.deleteObjectCalls, key)
	if m.deleteObjectErr != nil {
		return nil, m.deleteObjectErr
	}
	return m.deleteObjectResp, nil
}

func (m *mockStore) ListObjects(_ context.Context, _, _ string, _ int) (*ListObjectsResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.listObjectsErr != nil {
		return nil, m.listObjectsErr
	}
	return m.listObjectsResp, nil
}

func (m *mockStore) CreateMultipartUpload(_ context.Context, _, _, _, _ string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.createMultipartErr
}

func (m *mockStore) GetMultipartUpload(_ context.Context, _ string) (*MultipartUpload, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.getMultipartErr != nil {
		return nil, m.getMultipartErr
	}
	return m.getMultipartResp, nil
}

func (m *mockStore) RecordPart(_ context.Context, _ string, _ int, _ string, _ int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.recordPartErr
}

func (m *mockStore) GetParts(_ context.Context, _ string) ([]MultipartPart, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.getPartsErr != nil {
		return nil, m.getPartsErr
	}
	return m.getPartsResp, nil
}

func (m *mockStore) DeleteMultipartUpload(_ context.Context, _ string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.deleteMultipartErr
}

// --- Background operations (stubs) ---

func (m *mockStore) GetQuotaStats(_ context.Context) (map[string]QuotaStat, error) {
	return nil, nil
}

func (m *mockStore) GetObjectCounts(_ context.Context) (map[string]int64, error) {
	return nil, nil
}

func (m *mockStore) GetActiveMultipartCounts(_ context.Context) (map[string]int64, error) {
	return nil, nil
}

func (m *mockStore) GetStaleMultipartUploads(_ context.Context, _ time.Duration) ([]MultipartUpload, error) {
	return nil, nil
}

func (m *mockStore) ListObjectsByBackend(_ context.Context, _ string, _ int) ([]ObjectLocation, error) {
	return nil, nil
}

func (m *mockStore) MoveObjectLocation(_ context.Context, _, _, _ string) (int64, error) {
	return 0, nil
}

func (m *mockStore) GetUnderReplicatedObjects(_ context.Context, _, _ int) ([]ObjectLocation, error) {
	return nil, nil
}

func (m *mockStore) RecordReplica(_ context.Context, _, _, _ string, _ int64) (bool, error) {
	return false, nil
}
