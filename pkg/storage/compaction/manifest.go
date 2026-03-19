package compaction

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// Manifest tracks an in-flight compaction for crash recovery.
// Written before merge starts, removed after completion.
type Manifest struct {
	ID          string    `json:"id"`
	Index       string    `json:"index"`
	Partition   string    `json:"partition,omitempty"`
	InputIDs    []string  `json:"input_ids"`
	OutputLevel int       `json:"output_level"`
	TrivialMove bool      `json:"trivial_move,omitempty"`
	StartedAt   time.Time `json:"started_at"`
}

// ManifestStore manages compaction manifests on disk.
type ManifestStore struct {
	dir string // path to compaction/pending/ directory
}

// NewManifestStore creates a manifest store at the given directory.
// Creates the directory if it doesn't exist.
func NewManifestStore(dir string) (*ManifestStore, error) {
	pendingDir := filepath.Join(dir, "compaction", "pending")
	if err := os.MkdirAll(pendingDir, 0o755); err != nil {
		return nil, fmt.Errorf("compaction.NewManifestStore: create dir: %w", err)
	}

	return &ManifestStore{dir: pendingDir}, nil
}

// Write writes a manifest for an in-flight compaction.
// Uses atomic write (tmp + rename) to prevent partial writes on crash.
func (ms *ManifestStore) Write(m *Manifest) error {
	data, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("compaction.ManifestStore.Write: marshal: %w", err)
	}

	path := filepath.Join(ms.dir, m.ID+".json")
	tmpPath := path + ".tmp"

	if err := os.WriteFile(tmpPath, data, 0o644); err != nil {
		return fmt.Errorf("compaction.ManifestStore.Write: write tmp: %w", err)
	}

	if err := os.Rename(tmpPath, path); err != nil {
		os.Remove(tmpPath)

		return fmt.Errorf("compaction.ManifestStore.Write: rename: %w", err)
	}

	return nil
}

// Remove removes the manifest for a completed compaction.
func (ms *ManifestStore) Remove(id string) error {
	path := filepath.Join(ms.dir, id+".json")
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("compaction.ManifestStore.Remove: %w", err)
	}

	return nil
}

// LoadPending returns all pending (interrupted) compaction manifests.
// Call on startup to recover from crashes.
func (ms *ManifestStore) LoadPending() ([]*Manifest, error) {
	entries, err := os.ReadDir(ms.dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}

		return nil, fmt.Errorf("compaction.ManifestStore.LoadPending: read dir: %w", err)
	}

	var manifests []*Manifest

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		// Skip temp files from interrupted writes.
		if filepath.Ext(entry.Name()) != ".json" {
			continue
		}

		data, err := os.ReadFile(filepath.Join(ms.dir, entry.Name()))
		if err != nil {
			continue // skip unreadable files
		}

		var m Manifest
		if err := json.Unmarshal(data, &m); err != nil {
			continue // skip corrupt manifests
		}

		manifests = append(manifests, &m)
	}

	return manifests, nil
}

// CleanupInterrupted handles recovery for interrupted compactions.
// For each pending manifest, it removes the manifest file. The actual segment
// cleanup is handled by the filesystem scan (which ignores tmp_ files) and the
// next compaction cycle (which will re-plan if needed). This is the safe,
// conservative recovery path.
func (ms *ManifestStore) CleanupInterrupted(manifests []*Manifest, existsFn func(id string) bool) []string {
	var cleaned []string

	for _, m := range manifests {
		ms.Remove(m.ID)
		cleaned = append(cleaned, m.ID)
	}

	return cleaned
}
