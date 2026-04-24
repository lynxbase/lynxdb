package simulation

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
)

// S3Fault configures fault injection for SimS3.
type S3Fault struct {
	// PutFailRate is the probability [0.0, 1.0] of a Put operation failing.
	PutFailRate float64
	// GetFailRate is the probability [0.0, 1.0] of a Get operation failing.
	GetFailRate float64
	// LatencyMs is the simulated latency added to each operation.
	LatencyMs int
}

// SimS3 wraps an ObjectStore (typically objstore.MemStore) with fault injection.
// It records operation counts for assertions and supports configurable
// failure rates per operation type.
type SimS3 struct {
	mu      sync.Mutex
	objects map[string][]byte
	fault   S3Fault

	// Counters for assertions.
	PutCount atomic.Int64
	GetCount atomic.Int64
	DelCount atomic.Int64
}

// NewSimS3 creates a new SimS3 with no faults.
func NewSimS3() *SimS3 {
	return &SimS3{
		objects: make(map[string][]byte),
	}
}

// SetFault configures fault injection parameters.
func (s *SimS3) SetFault(f S3Fault) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.fault = f
}

// Put stores an object. Subject to fault injection.
func (s *SimS3) Put(_ context.Context, key string, r io.Reader) error {
	s.PutCount.Add(1)

	s.mu.Lock()
	fault := s.fault
	s.mu.Unlock()

	if fault.PutFailRate > 0 {
		// Deterministic fault based on key hash (reproducible in tests).
		h := simpleHash(key)
		if float64(h%100) < fault.PutFailRate*100 {
			return fmt.Errorf("simS3.Put: injected fault for key %q", key)
		}
	}

	data, err := io.ReadAll(r)
	if err != nil {
		return fmt.Errorf("simS3.Put: %w", err)
	}

	s.mu.Lock()
	s.objects[key] = data
	s.mu.Unlock()

	return nil
}

// Get retrieves an object. Subject to fault injection.
func (s *SimS3) Get(_ context.Context, key string) (io.ReadCloser, error) {
	s.GetCount.Add(1)

	s.mu.Lock()
	fault := s.fault
	data, ok := s.objects[key]
	s.mu.Unlock()

	if fault.GetFailRate > 0 {
		h := simpleHash(key)
		if float64(h%100) < fault.GetFailRate*100 {
			return nil, fmt.Errorf("simS3.Get: injected fault for key %q", key)
		}
	}

	if !ok {
		return nil, fmt.Errorf("simS3.Get: key %q not found", key)
	}

	return io.NopCloser(strings.NewReader(string(data))), nil
}

func (s *SimS3) Delete(_ context.Context, key string) error {
	s.DelCount.Add(1)

	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.objects, key)

	return nil
}

// List returns all keys with the given prefix.
func (s *SimS3) List(_ context.Context, prefix string) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var keys []string
	for k := range s.objects {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}

	return keys, nil
}

// ObjectCount returns the number of stored objects.
func (s *SimS3) ObjectCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return len(s.objects)
}

// simpleHash returns a deterministic hash for fault injection.
func simpleHash(s string) uint32 {
	var h uint32
	for _, c := range s {
		h = h*31 + uint32(c)
	}

	return h
}
