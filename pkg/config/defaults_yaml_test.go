package config

import (
	"reflect"
	"regexp"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// TestDefaultsYAMLMatchesCompiledDefaults prevents drift between the embedded
// defaults.yaml template (which is surfaced to users as documentation) and the
// compiled DefaultConfig(). Every uncommented key in the template must unmarshal
// into a value that matches the compiled default; otherwise the template lies.
func TestDefaultsYAMLMatchesCompiledDefaults(t *testing.T) {
	yamlBytes := uncommentYAMLTemplate(DefaultsTemplate)

	var fromYAML Config
	if err := yaml.Unmarshal(yamlBytes, &fromYAML); err != nil {
		t.Fatalf("defaults.yaml is not valid YAML after uncommenting: %v\n\nuncommented:\n%s", err, yamlBytes)
	}

	compiled := DefaultConfig()

	// Compare only the subsections the YAML template documents. The YAML is a
	// curated subset (no profiles block, no cluster block, no TLS/Auth blocks)
	// so we can't deep-equal the whole Config. Compare each documented subtree.
	cases := []struct {
		name         string
		fromYAMLPtr  interface{}
		compiledPtr  interface{}
		skipIfZero   bool // true = skip subtree if YAML didn't populate it
	}{
		{"Storage", fromYAML.Storage, compiled.Storage, true},
		{"Query", fromYAML.Query, compiled.Query, true},
		{"Ingest", fromYAML.Ingest, compiled.Ingest, true},
		{"HTTP", fromYAML.HTTP, compiled.HTTP, true},
		{"Tail", fromYAML.Tail, compiled.Tail, true},
		{"Server", fromYAML.Server, compiled.Server, true},
		{"Views", fromYAML.Views, compiled.Views, true},
		{"BufferManager", fromYAML.BufferManager, compiled.BufferManager, true},
	}

	for _, c := range cases {
		if c.skipIfZero && reflect.DeepEqual(c.fromYAMLPtr, reflect.Zero(reflect.TypeOf(c.fromYAMLPtr)).Interface()) {
			t.Errorf("%s subtree was not populated from YAML — defaults.yaml is missing keys", c.name)

			continue
		}
		if !reflect.DeepEqual(c.fromYAMLPtr, c.compiledPtr) {
			t.Errorf("%s drift between defaults.yaml and DefaultConfig():\n  yaml:     %#v\n  compiled: %#v", c.name, c.fromYAMLPtr, c.compiledPtr)
		}
	}
}

// uncommentYAMLTemplate strips the `# ` prefix from every commented line that
// looks like YAML content (key:value, map header, list item). The hash-prefix
// convention in defaults.yaml uses `# storage:` for section headers and
// `#   compression: lz4` for fields — so we must preserve the indentation of
// the YAML *after* the hash while ignoring any whitespace between the hash
// and the content. Lines that are pure prose comments are left alone (they
// become double-commented and are ignored by the YAML parser).
func uncommentYAMLTemplate(in []byte) []byte {
	// Group 1: indentation *inside* the hash prefix (becomes the YAML indent).
	// Group 2: the YAML payload itself — key:value, map header, or list item.
	keyish := regexp.MustCompile(`^#(\s*)([A-Za-z_][\w-]*:.*|- .*)$`)

	var b strings.Builder
	for _, line := range strings.Split(string(in), "\n") {
		if m := keyish.FindStringSubmatch(line); m != nil {
			// Drop the leading `# ` but preserve the indent that followed it.
			// e.g. "#   compression: lz4" -> "  compression: lz4"
			indent := m[1]
			if strings.HasPrefix(indent, " ") {
				indent = indent[1:] // consume the single space immediately after `#`
			}
			b.WriteString(indent)
			b.WriteString(m[2])
			b.WriteByte('\n')

			continue
		}
		b.WriteString(line)
		b.WriteByte('\n')
	}

	return []byte(b.String())
}
