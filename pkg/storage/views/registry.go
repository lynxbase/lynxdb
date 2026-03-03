package views

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

const viewsFile = "views.json"

// ViewRegistry manages view definitions with persistent storage.
type ViewRegistry struct {
	mu    sync.RWMutex
	views map[string]ViewDefinition
	dir   string
}

// Open loads or creates a view registry in the given directory.
func Open(dir string) (*ViewRegistry, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("views: mkdir %s: %w", dir, err)
	}

	r := &ViewRegistry{
		views: make(map[string]ViewDefinition),
		dir:   dir,
	}

	path := filepath.Join(dir, viewsFile)
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return r, nil
		}

		return nil, fmt.Errorf("views: read %s: %w", path, err)
	}

	var defs []ViewDefinition
	if err := json.Unmarshal(data, &defs); err != nil {
		return nil, fmt.Errorf("views: unmarshal %s: %w", path, err)
	}
	for _, d := range defs {
		r.views[d.Name] = d
	}

	return r, nil
}

// Create adds a new view definition to the registry.
func (r *ViewRegistry) Create(def ViewDefinition) error {
	if err := def.Validate(); err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.views[def.Name]; exists {
		return ErrViewAlreadyExists
	}

	r.views[def.Name] = def

	return r.persistLocked()
}

// Get returns a view definition by name.
func (r *ViewRegistry) Get(name string) (ViewDefinition, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	def, ok := r.views[name]
	if !ok {
		return ViewDefinition{}, ErrViewNotFound
	}

	return def, nil
}

// List returns all view definitions sorted by name.
func (r *ViewRegistry) List() []ViewDefinition {
	r.mu.RLock()
	defer r.mu.RUnlock()

	defs := make([]ViewDefinition, 0, len(r.views))
	for _, d := range r.views {
		defs = append(defs, d)
	}
	sort.Slice(defs, func(i, j int) bool {
		return defs[i].Name < defs[j].Name
	})

	return defs
}

// Update replaces an existing view definition.
func (r *ViewRegistry) Update(def ViewDefinition) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.views[def.Name]; !exists {
		return ErrViewNotFound
	}

	r.views[def.Name] = def

	return r.persistLocked()
}

// Drop removes a view definition and its data directory.
func (r *ViewRegistry) Drop(name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.views[name]; !exists {
		return ErrViewNotFound
	}

	delete(r.views, name)
	if err := r.persistLocked(); err != nil {
		return err
	}

	// Remove view data directory if it exists.
	viewDir := filepath.Join(filepath.Dir(r.dir), name)
	if _, err := os.Stat(viewDir); err == nil {
		if rmErr := os.RemoveAll(viewDir); rmErr != nil {
			slog.Warn("views: failed to remove view data directory", "dir", viewDir, "error", rmErr)
		}
	}

	return nil
}

// Close is a no-op. The registry does not hold open file handles between
// operations — views.json is written atomically via tmp+rename in persistLocked.
func (r *ViewRegistry) Close() error {
	return nil
}

// persistLocked writes the views to disk atomically with full durability.
// Caller must hold r.mu.
func (r *ViewRegistry) persistLocked() error {
	defs := make([]ViewDefinition, 0, len(r.views))
	for _, d := range r.views {
		defs = append(defs, d)
	}
	sort.Slice(defs, func(i, j int) bool {
		return defs[i].Name < defs[j].Name
	})

	data, err := json.MarshalIndent(defs, "", "  ")
	if err != nil {
		return fmt.Errorf("views: marshal: %w", err)
	}

	path := filepath.Join(r.dir, viewsFile)
	tmp := path + ".tmp"

	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("views: create tmp: %w", err)
	}
	if _, err := f.Write(data); err != nil {
		f.Close()

		return fmt.Errorf("views: write tmp: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()

		return fmt.Errorf("views: sync tmp: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("views: close tmp: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("views: rename: %w", err)
	}

	return syncDir(filepath.Dir(path))
}

// syncDir fsyncs a directory to ensure metadata operations (rename, link)
// are persisted to stable storage. Required on Linux where rename durability
// depends on the parent directory being fsynced.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("views: sync dir %s: %w", dir, err)
	}
	if err := d.Sync(); err != nil {
		d.Close()

		return fmt.Errorf("views: sync dir %s: %w", dir, err)
	}

	return d.Close()
}
