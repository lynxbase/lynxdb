package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestFormatConfigError_Validation(t *testing.T) {
	ve := &ValidationError{
		Section: "storage",
		Field:   "compression",
		Value:   "snappy",
		Message: "must be lz4 or zstd",
	}
	result := FormatConfigError(ve, "")
	if !strings.Contains(result, "Validation error: storage.compression") {
		t.Errorf("expected structured header, got:\n%s", result)
	}
	if !strings.Contains(result, "Value:   snappy") {
		t.Errorf("expected value line, got:\n%s", result)
	}
	if !strings.Contains(result, "Problem: must be lz4 or zstd") {
		t.Errorf("expected problem line, got:\n%s", result)
	}
}

func TestFormatConfigError_TopLevel(t *testing.T) {
	ve := &ValidationError{
		Section: "",
		Field:   "listen",
		Value:   "",
		Message: "must not be empty",
	}
	result := FormatConfigError(ve, "")
	if !strings.Contains(result, "Validation error: listen") {
		t.Errorf("expected top-level header, got:\n%s", result)
	}
	// Empty value should not show Value line.
	if strings.Contains(result, "Value:") {
		t.Errorf("expected no Value line for empty value, got:\n%s", result)
	}
}

func TestFormatConfigError_YAMLParse(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "bad.yaml")
	content := "listen: ok\nbad_indent\nretention: 7d\n"
	os.WriteFile(path, []byte(content), 0o644)

	// Simulate a YAML parse error with line number.
	err := fmt.Errorf("parse config %s: yaml: line 2: could not find expected ':'", path)
	result := FormatConfigError(err, path)

	if !strings.Contains(result, "YAML parse error in") {
		t.Errorf("expected YAML error header, got:\n%s", result)
	}
	if !strings.Contains(result, "> ") {
		t.Errorf("expected line pointer marker, got:\n%s", result)
	}
	if !strings.Contains(result, "bad_indent") {
		t.Errorf("expected offending line content, got:\n%s", result)
	}
}

func TestFormatConfigError_YAMLParse_NoFile(t *testing.T) {
	err := fmt.Errorf("parse config /nonexistent: yaml: line 5: something wrong")
	result := FormatConfigError(err, "/nonexistent")

	// Should still format even if file is unreadable.
	if !strings.Contains(result, "YAML parse error in /nonexistent") {
		t.Errorf("expected YAML error header, got:\n%s", result)
	}
}

func TestFormatConfigError_Nil(t *testing.T) {
	if got := FormatConfigError(nil, ""); got != "" {
		t.Errorf("expected empty string for nil error, got %q", got)
	}
}

func TestFormatConfigError_PlainError(t *testing.T) {
	err := fmt.Errorf("something went wrong")
	result := FormatConfigError(err, "")
	if result != "something went wrong" {
		t.Errorf("expected plain error message, got %q", result)
	}
}
