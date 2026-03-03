// Package sources provides a queryable registry of known source names.
// It enables glob pattern matching for multi-source queries (e.g., FROM logs*).
package sources

import (
	"path"
	"sort"
	"sync"
)

// Registry provides fast lookups of known source names.
// It is updated on ingest and segment loading.
// All methods are safe for concurrent use.
type Registry struct {
	mu      sync.RWMutex
	names   []string            // sorted alphabetically
	nameSet map[string]struct{} // for O(1) Contains
}

// NewRegistry creates an empty source registry.
func NewRegistry() *Registry {
	return &Registry{
		nameSet: make(map[string]struct{}),
	}
}

// List returns all known source names, sorted alphabetically.
func (r *Registry) List() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	out := make([]string, len(r.names))
	copy(out, r.names)

	return out
}

// Match returns source names matching the glob pattern.
// Supports '*' (any sequence) and '?' (any single char) via path.Match.
// Pattern "*" returns all sources.
func (r *Registry) Match(pattern string) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if pattern == "*" {
		out := make([]string, len(r.names))
		copy(out, r.names)

		return out
	}

	var matched []string
	for _, name := range r.names {
		ok, _ := path.Match(pattern, name)
		if ok {
			matched = append(matched, name)
		}
	}

	return matched
}

// Contains returns true if the exact source name exists.
func (r *Registry) Contains(name string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()

	_, ok := r.nameSet[name]

	return ok
}

// Register adds a new source name. Idempotent — duplicate registrations
// are no-ops. Called on ingest when a new _source value is seen.
func (r *Registry) Register(name string) {
	if name == "" {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.nameSet[name]; exists {
		return
	}

	r.nameSet[name] = struct{}{}

	// Insert into sorted slice using binary search + copy.
	// O(n) per insert due to slice shifting — acceptable because source count
	// is typically <100 and registration happens infrequently (on new source discovery).
	idx := sort.SearchStrings(r.names, name)
	r.names = append(r.names, "")
	copy(r.names[idx+1:], r.names[idx:])
	r.names[idx] = name
}

// RegisterAll adds multiple source names. This is more efficient than
// calling Register individually when populating from segment metadata.
func (r *Registry) RegisterAll(names []string) {
	if len(names) == 0 {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	changed := false
	for _, name := range names {
		if name == "" {
			continue
		}
		if _, exists := r.nameSet[name]; exists {
			continue
		}
		r.nameSet[name] = struct{}{}
		r.names = append(r.names, name)
		changed = true
	}

	if changed {
		sort.Strings(r.names)
	}
}

// Count returns the number of known sources.
func (r *Registry) Count() int {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return len(r.names)
}
