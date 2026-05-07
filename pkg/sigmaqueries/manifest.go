package sigmaqueries

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
)

// FixtureManifest maps fixture names to their first query metadata entry.
type FixtureManifest map[string]ManifestEntry

// ReadManifestFile reads an rsigma sidecar manifest from path.
func ReadManifestFile(path string) (*Manifest, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	return ReadManifestReader(f)
}

// ReadManifest reads an rsigma sidecar manifest from path.
func ReadManifest(path string) (*Manifest, error) {
	return ReadManifestFile(path)
}

// ReadManifestReader decodes an rsigma sidecar manifest from r.
func ReadManifestReader(r io.Reader) (*Manifest, error) {
	dec := json.NewDecoder(r)

	var manifest Manifest
	if err := dec.Decode(&manifest); err != nil {
		return nil, fmt.Errorf("decode manifest: %w", err)
	}

	return &manifest, nil
}

// WriteManifestFile writes manifest as indented JSON to path.
func WriteManifestFile(path string, manifest *Manifest) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	return WriteManifestWriter(f, manifest)
}

// WriteManifestWriter writes manifest as indented JSON to w.
func WriteManifestWriter(w io.Writer, manifest *Manifest) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(manifest)
}
