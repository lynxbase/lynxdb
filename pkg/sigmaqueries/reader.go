package sigmaqueries

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"
)

// Query is one SPL2 query read from an rsigma-produced query file.
type Query struct {
	Line       string
	Source     string
	LineNumber int
}

// Manifest describes the sidecar metadata for rsigma-produced query lines.
type Manifest struct {
	RsigmaVersion string          `json:"rsigma_version"`
	Queries       []ManifestEntry `json:"queries"`
	Fixtures      FixtureManifest `json:"fixtures,omitempty"`
}

// ManifestEntry pairs a query line with Sigma rule metadata.
type ManifestEntry struct {
	Fixture string   `json:"fixture,omitempty"`
	Line    int      `json:"line"`
	RuleID  string   `json:"rule_id"`
	Title   string   `json:"title"`
	Level   string   `json:"level"`
	Tags    []string `json:"tags"`
}

// ReadFile reads SPL2 queries from path.
func ReadFile(path string) ([]Query, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	return ReadReader(f, path)
}

// ReadReader reads one query per non-blank, non-comment line from r.
//
// Lines whose first non-space character is '#' are ignored. Inline comments are
// not stripped because SPL2 has no comment syntax.
func ReadReader(r io.Reader, source string) ([]Query, error) {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)

	var queries []Query
	lineNumber := 0

	for scanner.Scan() {
		lineNumber++
		line := scanner.Text()
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}

		queries = append(queries, Query{
			Line:       line,
			Source:     source,
			LineNumber: lineNumber,
		})
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read %s: %w", source, err)
	}

	return queries, nil
}
