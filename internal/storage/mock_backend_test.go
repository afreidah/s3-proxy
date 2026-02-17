package storage

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
)

// mockBackend is an in-memory ObjectBackend for unit testing.
type mockBackend struct {
	mu      sync.Mutex
	objects map[string]mockObject
	putErr  error
	getErr  error
	headErr error
	delErr  error
}

type mockObject struct {
	data        []byte
	contentType string
	etag        string
}

func newMockBackend() *mockBackend {
	return &mockBackend{objects: make(map[string]mockObject)}
}

var _ ObjectBackend = (*mockBackend)(nil)

func (m *mockBackend) PutObject(_ context.Context, key string, body io.Reader, _ int64, contentType string) (string, error) {
	m.mu.Lock()
	err := m.putErr
	m.mu.Unlock()
	if err != nil {
		return "", err
	}

	// Read body outside the lock to avoid deadlocking with pipe-based copies
	data, err := io.ReadAll(body)
	if err != nil {
		return "", err
	}

	etag := fmt.Sprintf(`"%x"`, len(data))

	m.mu.Lock()
	m.objects[key] = mockObject{data: data, contentType: contentType, etag: etag}
	m.mu.Unlock()

	return etag, nil
}

func (m *mockBackend) GetObject(_ context.Context, key string, _ string) (*GetObjectResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.getErr != nil {
		return nil, m.getErr
	}
	obj, ok := m.objects[key]
	if !ok {
		return nil, fmt.Errorf("object %q not found", key)
	}
	// Return a copy of the data so the caller can read it after the lock is released
	cp := make([]byte, len(obj.data))
	copy(cp, obj.data)
	return &GetObjectResult{
		Body:        io.NopCloser(bytes.NewReader(cp)),
		Size:        int64(len(cp)),
		ContentType: obj.contentType,
		ETag:        obj.etag,
	}, nil
}

func (m *mockBackend) HeadObject(_ context.Context, key string) (int64, string, string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.headErr != nil {
		return 0, "", "", m.headErr
	}
	obj, ok := m.objects[key]
	if !ok {
		return 0, "", "", fmt.Errorf("object %q not found", key)
	}
	return int64(len(obj.data)), obj.contentType, obj.etag, nil
}

func (m *mockBackend) DeleteObject(_ context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.delErr != nil {
		return m.delErr
	}
	delete(m.objects, key)
	return nil
}

// hasObject returns true if the key exists in the mock backend's store.
func (m *mockBackend) hasObject(key string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.objects[key]
	return ok
}
