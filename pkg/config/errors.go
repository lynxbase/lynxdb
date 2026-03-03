package config

import (
	"errors"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
)

// yamlLineRegexp extracts the line number from yaml.v3 parse error messages.
var yamlLineRegexp = regexp.MustCompile(`line (\d+):`)

// FormatConfigError formats a config loading or validation error for display.
// For YAML parse errors, it shows the offending line from the source file.
// For ValidationErrors, it shows the field and a helpful message.
func FormatConfigError(err error, path string) string {
	if err == nil {
		return ""
	}

	// Check for ValidationError first.
	var ve *ValidationError
	if errors.As(err, &ve) {
		return formatValidationError(ve)
	}

	// Check for YAML parse error with line number.
	msg := err.Error()
	if m := yamlLineRegexp.FindStringSubmatch(msg); m != nil {
		lineNum, _ := strconv.Atoi(m[1])
		if lineNum > 0 && path != "" {
			return formatYAMLError(msg, path, lineNum)
		}
	}

	return msg
}

func formatValidationError(ve *ValidationError) string {
	var b strings.Builder
	if ve.Section != "" {
		fmt.Fprintf(&b, "Validation error: %s.%s\n", ve.Section, ve.Field)
	} else {
		fmt.Fprintf(&b, "Validation error: %s\n", ve.Field)
	}
	if ve.Value != "" {
		fmt.Fprintf(&b, "  Value:   %s\n", ve.Value)
	}
	fmt.Fprintf(&b, "  Problem: %s", ve.Message)

	return b.String()
}

func formatYAMLError(msg, path string, lineNum int) string {
	var b strings.Builder
	fmt.Fprintf(&b, "YAML parse error in %s:\n", path)

	data, readErr := os.ReadFile(path)
	if readErr != nil {
		fmt.Fprintf(&b, "  %s", msg)

		return b.String()
	}

	lines := strings.Split(string(data), "\n")
	// Show context: line before, the offending line, line after.
	start := lineNum - 2 // 0-indexed, one line before
	if start < 0 {
		start = 0
	}
	end := lineNum + 1 // one line after (exclusive)
	if end > len(lines) {
		end = len(lines)
	}
	for i := start; i < end; i++ {
		marker := "  "
		if i == lineNum-1 {
			marker = "> "
		}
		fmt.Fprintf(&b, "  %s%4d | %s\n", marker, i+1, lines[i])
	}

	// Extract the specific error from the yaml message (after "line N: ").
	if idx := strings.Index(msg, ": "); idx >= 0 {
		detail := msg[idx+2:]
		// Try to get the last part after "parse config path: "
		if di := strings.LastIndex(detail, ": "); di >= 0 {
			detail = detail[di+2:]
		}
		fmt.Fprintf(&b, "  Error: %s", detail)
	}

	return b.String()
}
