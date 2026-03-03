package config

import (
	"fmt"
	"strconv"
	"strings"
)

// ByteSize represents a size in bytes with human-readable parsing and formatting.
// Supports values like "4mb", "1gb", "256mb", "512kb", or raw integers.
type ByteSize int64

const (
	KB ByteSize = 1024
	MB ByteSize = 1024 * KB
	GB ByteSize = 1024 * MB
	TB ByteSize = 1024 * GB
)

// ParseByteSize parses a human-readable byte size string.
// Supported suffixes (case-insensitive): kb, mb, gb, tb.
// Raw integers are treated as bytes.
func ParseByteSize(s string) (ByteSize, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("empty byte size")
	}

	// Try raw integer first.
	if n, err := strconv.ParseInt(s, 10, 64); err == nil {
		if n < 0 {
			return 0, fmt.Errorf("invalid byte size %q: must not be negative", s)
		}

		return ByteSize(n), nil
	}

	lower := strings.ToLower(s)
	var suffix string
	var multiplier ByteSize
	for _, pair := range []struct {
		suffix string
		mult   ByteSize
	}{
		{"tb", TB},
		{"gb", GB},
		{"mb", MB},
		{"kb", KB},
	} {
		if strings.HasSuffix(lower, pair.suffix) {
			suffix = pair.suffix
			multiplier = pair.mult

			break
		}
	}

	if suffix == "" {
		return 0, fmt.Errorf("invalid byte size %q: unknown suffix", s)
	}

	numStr := strings.TrimSpace(s[:len(s)-len(suffix)])
	n, err := strconv.ParseFloat(numStr, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid byte size %q: %w", s, err)
	}
	if n < 0 {
		return 0, fmt.Errorf("invalid byte size %q: must not be negative", s)
	}

	return ByteSize(n * float64(multiplier)), nil
}

// String returns a human-readable representation.
func (b ByteSize) String() string {
	switch {
	case b == 0:
		return "0"
	case b >= TB && b%TB == 0:
		return fmt.Sprintf("%dtb", b/TB)
	case b >= GB && b%GB == 0:
		return fmt.Sprintf("%dgb", b/GB)
	case b >= MB && b%MB == 0:
		return fmt.Sprintf("%dmb", b/MB)
	case b >= KB && b%KB == 0:
		return fmt.Sprintf("%dkb", b/KB)
	default:
		return strconv.FormatInt(int64(b), 10)
	}
}

// Int64 returns the raw byte count.
func (b ByteSize) Int64() int64 {
	return int64(b)
}

// UnmarshalYAML implements yaml.Unmarshaler.
func (b *ByteSize) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var s string
	if err := unmarshal(&s); err != nil {
		// Try as integer.
		var n int64
		if err2 := unmarshal(&n); err2 != nil {
			return fmt.Errorf("invalid byte size: %w", err)
		}
		*b = ByteSize(n)

		return nil
	}
	parsed, err := ParseByteSize(s)
	if err != nil {
		return err
	}
	*b = parsed

	return nil
}

// MarshalYAML implements yaml.Marshaler.
func (b ByteSize) MarshalYAML() (interface{}, error) {
	return b.String(), nil
}

// UnmarshalJSON implements json.Unmarshaler.
func (b *ByteSize) UnmarshalJSON(data []byte) error {
	s := strings.Trim(string(data), `"`)
	// Try as raw integer.
	if n, err := strconv.ParseInt(s, 10, 64); err == nil {
		*b = ByteSize(n)

		return nil
	}
	parsed, err := ParseByteSize(s)
	if err != nil {
		return err
	}
	*b = parsed

	return nil
}

// MarshalJSON implements json.Marshaler.
func (b ByteSize) MarshalJSON() ([]byte, error) {
	return []byte(`"` + b.String() + `"`), nil
}
