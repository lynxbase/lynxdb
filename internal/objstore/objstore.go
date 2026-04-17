package objstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"sync"
)

// ObjectStore is the interface for tiered object storage.
// Implementations include in-memory (for testing) and cloud backends.
type ObjectStore interface {
	// Put writes data to the given key.
	Put(ctx context.Context, key string, data []byte) error
	// Get reads the full object at the given key.
	Get(ctx context.Context, key string) ([]byte, error)
	// GetRange reads a byte range from the object.
	GetRange(ctx context.Context, key string, offset, length int64) ([]byte, error)
	// Delete removes the object at the given key.
	Delete(ctx context.Context, key string) error
	// List returns all keys with the given prefix.
	List(ctx context.Context, prefix string) ([]string, error)
	// Exists returns true if the key exists.
	Exists(ctx context.Context, key string) (bool, error)
	// Copy copies an object from srcKey to dstKey without downloading.
	// S3 implementations use CopyObject; in-memory implementations
	// use Get+Put internally.
	Copy(ctx context.Context, srcKey, dstKey string) error
}

// ErrNotFound indicates that the requested object key does not exist.
var ErrNotFound = errors.New("objstore: key not found")

// IsNotFound reports whether err indicates a missing object key.
func IsNotFound(err error) bool {
	return errors.Is(err, ErrNotFound)
}

func notFoundError(key string) error {
	return fmt.Errorf("%w: %s", ErrNotFound, key)
}

// MemStore is an in-memory ObjectStore implementation for testing.
type MemStore struct {
	mu      sync.RWMutex
	objects map[string][]byte
}

// NewMemStore creates a new in-memory object store.
func NewMemStore() *MemStore {
	return &MemStore{objects: make(map[string][]byte)}
}

func (m *MemStore) Put(_ context.Context, key string, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]byte, len(data))
	copy(cp, data)
	m.objects[key] = cp

	return nil
}

func (m *MemStore) Get(_ context.Context, key string) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	data, ok := m.objects[key]
	if !ok {
		return nil, notFoundError(key)
	}
	cp := make([]byte, len(data))
	copy(cp, data)

	return cp, nil
}

func (m *MemStore) GetRange(_ context.Context, key string, offset, length int64) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	data, ok := m.objects[key]
	if !ok {
		return nil, notFoundError(key)
	}
	if offset < 0 || offset >= int64(len(data)) {
		return nil, fmt.Errorf("objstore: offset %d out of range [0, %d)", offset, len(data))
	}
	end := offset + length
	if end > int64(len(data)) {
		end = int64(len(data))
	}
	cp := make([]byte, end-offset)
	copy(cp, data[offset:end])

	return cp, nil
}

func (m *MemStore) Delete(_ context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.objects, key)

	return nil
}

func (m *MemStore) List(_ context.Context, prefix string) ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var keys []string
	for k := range m.objects {
		if len(k) >= len(prefix) && k[:len(prefix)] == prefix {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)

	return keys, nil
}

func (m *MemStore) Exists(_ context.Context, key string) (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, ok := m.objects[key]

	return ok, nil
}

func (m *MemStore) Copy(_ context.Context, srcKey, dstKey string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	data, ok := m.objects[srcKey]
	if !ok {
		return notFoundError(srcKey)
	}
	cp := make([]byte, len(data))
	copy(cp, data)
	m.objects[dstKey] = cp

	return nil
}

// Size returns the number of objects stored.
func (m *MemStore) Size() int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return len(m.objects)
}

// WriterTo wraps an ObjectStore Put into an io.Writer-compatible interface.
// Provided for future tiering pipeline use; has test coverage in objstore_test.go.
type WriterTo struct {
	store ObjectStore
	key   string
	buf   []byte
}

// NewWriterTo creates a buffering writer that flushes to the ObjectStore on Close.
func NewWriterTo(store ObjectStore, key string) *WriterTo {
	return &WriterTo{store: store, key: key}
}

func (w *WriterTo) Write(p []byte) (int, error) {
	w.buf = append(w.buf, p...)

	return len(p), nil
}

// Close flushes the buffer to the object store.
func (w *WriterTo) Close(ctx context.Context) error {
	return w.store.Put(ctx, w.key, w.buf)
}

type bytesReaderAt struct {
	data []byte
}

func newBytesReaderAt(data []byte) *bytesReaderAt {
	return &bytesReaderAt{data: data}
}

func (r *bytesReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if off >= int64(len(r.data)) {
		return 0, io.EOF
	}
	n := copy(p, r.data[off:])
	if n < len(p) {
		return n, io.EOF
	}

	return n, nil
}
