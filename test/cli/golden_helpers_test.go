//go:build clitest

package cli_test

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

var update = flag.Bool("update", false, "update .expected golden files")

// goldenTest represents a parsed .test file.
type goldenTest struct {
	Name           string // base filename without extension
	Path           string // absolute path to .test file
	ExpectedPath   string // absolute path to .expected file
	File           string // log filename relative to testdata/logs/ (file mode only)
	Format         string // output format: json, csv, raw (default: json)
	ExitCode       string // expected exit code: "0", "nonzero", or a specific number (default: "0")
	Skip           string // skip reason (empty = don't skip)
	StderrContains string // substring that must appear in stderr
	Unordered      bool   // sort output lines before comparing (hash-ordered aggregate output)
	Query          string // PPL query (everything after headers)
}

// parseTestFile reads a .test file and returns a goldenTest.
func parseTestFile(t *testing.T, path string) goldenTest {
	t.Helper()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read test file %s: %v", path, err)
	}

	base := filepath.Base(path)
	name := strings.TrimSuffix(base, ".test")
	expectedPath := strings.TrimSuffix(path, ".test") + ".expected"

	tc := goldenTest{
		Name:         name,
		Path:         path,
		ExpectedPath: expectedPath,
		Format:       "json",
		ExitCode:     "0",
	}

	var queryLines []string
	scanner := bufio.NewScanner(strings.NewReader(string(data)))

	for scanner.Scan() {
		line := scanner.Text()

		// Headers: lines starting with "# key: value"
		if strings.HasPrefix(line, "# ") {
			header := strings.TrimPrefix(line, "# ")
			if k, v, ok := strings.Cut(header, ":"); ok {
				k = strings.TrimSpace(k)
				v = strings.TrimSpace(v)

				switch k {
				case "file":
					tc.File = v
				case "format":
					tc.Format = v
				case "exit":
					tc.ExitCode = v
				case "unordered":
					tc.Unordered = v == "true"
				case "skip":
					tc.Skip = v
				case "stderr-contains":
					tc.StderrContains = v
				}

				continue
			}
		}

		queryLines = append(queryLines, line)
	}

	tc.Query = strings.TrimSpace(strings.Join(queryLines, "\n"))

	return tc
}

// discoverTests walks a directory and returns sorted test cases.
func discoverTests(t *testing.T, dir string) []goldenTest {
	t.Helper()

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read test directory %s: %v", dir, err)
	}

	var tests []goldenTest

	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".test") {
			continue
		}

		tc := parseTestFile(t, filepath.Join(dir, e.Name()))
		tests = append(tests, tc)
	}

	sort.Slice(tests, func(i, j int) bool {
		return tests[i].Name < tests[j].Name
	})

	return tests
}

// normalizeNDJSON parses NDJSON lines, re-marshals with sorted keys,
// and normalizes integer-valued floats (e.g., 5200.0 → 5200).
func normalizeNDJSON(raw string) string {
	var lines []string

	for _, line := range strings.Split(raw, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		var m map[string]interface{}
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			// Not valid JSON — keep as-is.
			lines = append(lines, line)
			continue
		}

		// Normalize integer-valued floats.
		normalizeMap(m)

		// json.Marshal sorts keys deterministically.
		b, err := json.Marshal(m)
		if err != nil {
			lines = append(lines, line)
			continue
		}

		lines = append(lines, string(b))
	}

	return strings.Join(lines, "\n") + "\n"
}

// normalizeMap recursively normalizes float64 values:
//   - Whole-number floats (e.g., 5200.0) are converted to int64
//   - Other floats are rounded to 12 significant digits to absorb
//     cross-platform floating-point precision differences
//   - String values containing absolute paths to testdata are stripped to
//     a portable relative form so golden tests work across dev machines/CI
func normalizeMap(m map[string]interface{}) {
	for k, v := range m {
		switch val := v.(type) {
		case float64:
			m[k] = normalizeFloat(val)
		case string:
			m[k] = normalizeTestdataPath(val)
		case map[string]interface{}:
			normalizeMap(val)
		case []interface{}:
			for i, elem := range val {
				switch e := elem.(type) {
				case float64:
					val[i] = normalizeFloat(e)
				case string:
					val[i] = normalizeTestdataPath(e)
				}
			}
		}
	}
}

// normalizeTestdataPath strips machine-specific absolute path prefixes from
// strings referring to files under testdata/, so golden files produced on a
// dev laptop match those produced in CI. Leaves non-matching strings alone.
func normalizeTestdataPath(s string) string {
	const marker = "/testdata/"
	idx := strings.Index(s, marker)
	if idx <= 0 {
		return s
	}
	// Require the prefix to look like an absolute path so we don't rewrite
	// arbitrary strings that happen to contain "/testdata/".
	prefix := s[:idx]
	if !strings.HasPrefix(prefix, "/") {
		return s
	}
	return "testdata/" + s[idx+len(marker):]
}

// normalizeFloat converts integer-valued floats to int64, and rounds
// other floats to 12 significant digits for cross-platform consistency.
func normalizeFloat(f float64) interface{} {
	if math.IsInf(f, 0) || math.IsNaN(f) {
		return f
	}
	if f == math.Trunc(f) {
		return int64(f)
	}
	// Round to 12 significant digits.
	if f == 0 {
		return f
	}
	d := math.Ceil(math.Log10(math.Abs(f)))
	pow := math.Pow(10, 12-d)

	return math.Round(f*pow) / pow
}

// normalizeText trims trailing whitespace per line and trailing empty lines.
func normalizeText(raw string) string {
	var lines []string

	for _, line := range strings.Split(raw, "\n") {
		lines = append(lines, strings.TrimRight(line, " \t\r"))
	}

	// Remove trailing empty lines.
	for len(lines) > 0 && lines[len(lines)-1] == "" {
		lines = lines[:len(lines)-1]
	}

	if len(lines) == 0 {
		return ""
	}

	return strings.Join(lines, "\n") + "\n"
}

// assertGolden compares actual output against the .expected golden file.
// In -update mode, it writes the actual output as the new expected file.
func assertGolden(t *testing.T, tc goldenTest, result Result) {
	t.Helper()

	switch tc.ExitCode {
	case "0":
		if result.ExitCode != 0 {
			t.Errorf("expected exit code 0, got %d\nstderr: %s", result.ExitCode, result.Stderr)
			return
		}
	case "nonzero":
		if result.ExitCode == 0 {
			t.Errorf("expected non-zero exit code, got 0\nstdout: %s", result.Stdout)
			return
		}
	default:
		// Specific exit code (e.g., "4").
		var want int
		if _, err := fmt.Sscanf(tc.ExitCode, "%d", &want); err == nil {
			if result.ExitCode != want {
				t.Errorf("expected exit code %d, got %d\nstderr: %s", want, result.ExitCode, result.Stderr)
				return
			}
		}
	}

	if tc.StderrContains != "" {
		if !strings.Contains(strings.ToLower(result.Stderr), strings.ToLower(tc.StderrContains)) {
			t.Errorf("stderr does not contain %q\nstderr: %s", tc.StderrContains, result.Stderr)
		}
	}

	// For error tests without .expected, we only check exit code + stderr.
	if tc.ExitCode != "0" {
		if _, err := os.Stat(tc.ExpectedPath); os.IsNotExist(err) {
			return
		}
	}

	// Normalize actual output.
	var actual string
	switch tc.Format {
	case "json":
		actual = normalizeNDJSON(result.Stdout)
	default:
		actual = normalizeText(result.Stdout)
	}
	if tc.Unordered {
		actual = sortLines(actual)
	}

	if *update {
		if err := os.WriteFile(tc.ExpectedPath, []byte(actual), 0644); err != nil {
			t.Fatalf("write expected file %s: %v", tc.ExpectedPath, err)
		}

		t.Logf("updated %s", tc.ExpectedPath)

		return
	}

	// Read expected file.
	expectedData, err := os.ReadFile(tc.ExpectedPath)
	if err != nil {
		t.Fatalf("read expected file %s: %v\n(run with -update to create it)", tc.ExpectedPath, err)
	}

	var expected string
	switch tc.Format {
	case "json":
		expected = normalizeNDJSON(string(expectedData))
	default:
		expected = normalizeText(string(expectedData))
	}
	if tc.Unordered {
		expected = sortLines(expected)
	}

	if actual != expected {
		diff := diffStrings(expected, actual)
		t.Errorf("output mismatch for %s\n%s\n(run with -update to regenerate)", tc.Name, diff)
	}
}

// diffStrings produces a line-by-line diff with context.
func diffStrings(expected, actual string) string {
	expectedLines := strings.Split(expected, "\n")
	actualLines := strings.Split(actual, "\n")

	var sb strings.Builder
	sb.WriteString("--- expected\n+++ actual\n")

	maxLen := len(expectedLines)
	if len(actualLines) > maxLen {
		maxLen = len(actualLines)
	}

	contextLines := 3
	lastPrinted := -1

	for i := 0; i < maxLen; i++ {
		var eLine, aLine string

		if i < len(expectedLines) {
			eLine = expectedLines[i]
		}

		if i < len(actualLines) {
			aLine = actualLines[i]
		}

		if eLine != aLine {
			// Print context before the diff.
			start := i - contextLines
			if start < 0 {
				start = 0
			}
			if start <= lastPrinted {
				start = lastPrinted + 1
			}

			if start > lastPrinted+1 && lastPrinted >= 0 {
				fmt.Fprintf(&sb, "@@ line %d @@\n", i+1)
			}

			for j := start; j < i; j++ {
				if j < len(expectedLines) {
					fmt.Fprintf(&sb, " %s\n", expectedLines[j])
				}
			}

			if i < len(expectedLines) {
				fmt.Fprintf(&sb, "-%s\n", eLine)
			} else {
				fmt.Fprint(&sb, "-<missing>\n")
			}

			if i < len(actualLines) {
				fmt.Fprintf(&sb, "+%s\n", aLine)
			} else {
				fmt.Fprint(&sb, "+<missing>\n")
			}

			lastPrinted = i
		}
	}

	if lastPrinted == -1 {
		sb.WriteString("(no differences found — possible whitespace issue)\n")
	}

	return sb.String()
}

// sortLines sorts output lines for transcripts marked `# unordered: true`,
// whose row order is hash-iteration dependent. Ordered transcripts (anything
// with a sort stage) compare verbatim.
func sortLines(s string) string {
	lines := strings.Split(strings.TrimRight(s, "\n"), "\n")
	sort.Strings(lines)
	return strings.Join(lines, "\n") + "\n"
}
