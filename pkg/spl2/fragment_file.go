package spl2

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// FileFragmentResolver resolves fragments from ~/.config/lynxdb/pipelines/.
type FileFragmentResolver struct {
	baseDir  string
	builtins map[string]string
}

// NewFileFragmentResolver creates a resolver that reads fragment files from
// the given directory. If baseDir is empty, ~/.config/lynxdb/pipelines/ is used.
func NewFileFragmentResolver(baseDir string) *FileFragmentResolver {
	if baseDir == "" {
		home, err := os.UserHomeDir()
		if err == nil {
			baseDir = filepath.Join(home, ".config", "lynxdb", "pipelines")
		}
	}

	return &FileFragmentResolver{
		baseDir:  baseDir,
		builtins: BuiltinFragments(),
	}
}

// Resolve resolves a fragment name to a list of commands.
// Built-in @stdlib/* fragments are resolved from hardcoded definitions.
// User fragments are read from <baseDir>/<name>.lf and parsed.
func (r *FileFragmentResolver) Resolve(name string) ([]Command, error) {
	// Built-in fragments.
	if isBuiltinFragment(name) {
		body, ok := r.builtins[name]
		if !ok {
			available := make([]string, 0, len(r.builtins))
			for k := range r.builtins {
				available = append(available, k)
			}
			return nil, fmt.Errorf("unknown built-in fragment %q. Available: %s", name, strings.Join(available, ", "))
		}
		return parseFragmentBody(body)
	}

	// User fragment from file.
	if r.baseDir == "" {
		return nil, fmt.Errorf("no fragment directory configured")
	}

	path := filepath.Join(r.baseDir, name+".lf")
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("fragment %q not found at %s", name, path)
		}
		return nil, fmt.Errorf("reading fragment %q: %w", name, err)
	}

	return parseFragmentBody(string(data))
}

// ListFragments returns the names of all available fragments (built-in + file-based).
func (r *FileFragmentResolver) ListFragments() []string {
	var names []string

	// Built-in fragments.
	for name := range r.builtins {
		names = append(names, name)
	}

	// File-based fragments.
	if r.baseDir != "" {
		entries, err := os.ReadDir(r.baseDir)
		if err == nil {
			for _, e := range entries {
				if !e.IsDir() && strings.HasSuffix(e.Name(), ".lf") {
					name := strings.TrimSuffix(e.Name(), ".lf")
					names = append(names, name)
				}
			}
		}
	}

	return names
}

// ReadFragment returns the raw text of a fragment.
func (r *FileFragmentResolver) ReadFragment(name string) (string, error) {
	if isBuiltinFragment(name) {
		body, ok := r.builtins[name]
		if !ok {
			return "", fmt.Errorf("unknown built-in fragment %q", name)
		}
		return body, nil
	}

	if r.baseDir == "" {
		return "", fmt.Errorf("no fragment directory configured")
	}

	path := filepath.Join(r.baseDir, name+".lf")
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}

	return string(data), nil
}

// parseFragmentBody parses a fragment body as a pipeline (commands only, no FROM).
// The body may optionally start with a pipe character.
func parseFragmentBody(body string) ([]Command, error) {
	body = strings.TrimSpace(body)
	if body == "" {
		return nil, nil
	}

	// Fragments are commands only — wrap in a fake query to parse.
	// "FROM _dummy " + body
	query := "FROM _dummy " + body
	prog, err := ParseProgram(query)
	if err != nil {
		return nil, fmt.Errorf("parsing fragment: %w", err)
	}

	return prog.Main.Commands, nil
}
