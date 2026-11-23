// Package config — projectrc.go handles .lynxdbrc per-project config files.
// These files contain project-scoped CLI defaults (not server settings).
package config

import (
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
)

// ProjectRC holds per-project CLI defaults loaded from a .lynxdbrc file.
// Only a subset of settings relevant to local CLI usage are supported —
// server settings like listen, data_dir, retention are not allowed here.
type ProjectRC struct {
	DefaultSince  string `yaml:"default_since"`  // default --since value
	DefaultFormat string `yaml:"default_format"` // default --format value
	DefaultSource string `yaml:"default_source"` // default source metadata
	Profile       string `yaml:"profile"`        // default --profile name

	// Ignored lists the keys of the file that a .lynxdbrc may not set. A
	// .lynxdbrc is found by walking up from the working directory, so it
	// may not choose the server that the API key is sent to.
	Ignored []string `yaml:"-"`
}

// ignoredRCKeys are the keys a .lynxdbrc may not set.
var ignoredRCKeys = []string{"server"}

// LoadProjectRC walks up from cwd to find the nearest .lynxdbrc file
// and loads it. Returns nil with no error if no .lynxdbrc is found.
func LoadProjectRC() (*ProjectRC, string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return nil, "", err
	}

	for {
		candidate := filepath.Join(dir, ".lynxdbrc")
		if _, err := os.Stat(candidate); err == nil {
			rc, err := loadRCFile(candidate)
			if err != nil {
				return nil, candidate, err
			}

			return rc, candidate, nil
		}

		parent := filepath.Dir(dir)
		if parent == dir {
			break // reached filesystem root
		}

		dir = parent
	}

	return nil, "", nil
}

// loadRCFile reads and parses a single .lynxdbrc file.
func loadRCFile(path string) (*ProjectRC, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var rc ProjectRC
	if err := yaml.Unmarshal(data, &rc); err != nil {
		return nil, err
	}
	var keys map[string]any
	if err := yaml.Unmarshal(data, &keys); err != nil {
		return nil, err
	}
	for _, key := range ignoredRCKeys {
		if _, ok := keys[key]; ok {
			rc.Ignored = append(rc.Ignored, key)
		}
	}

	return &rc, nil
}
