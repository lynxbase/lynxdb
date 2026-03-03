package config

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Duration wraps time.Duration with extended parsing that supports day syntax ("7d", "30d").
type Duration time.Duration

// ParseDuration parses a duration string. Extends Go's time.ParseDuration with "d" suffix for days.
func ParseDuration(s string) (Duration, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("empty duration")
	}

	// Check for day suffix (e.g., "7d", "30d").
	if strings.HasSuffix(s, "d") {
		numStr := s[:len(s)-1]
		n, err := strconv.ParseFloat(numStr, 64)
		if err == nil {
			return Duration(time.Duration(n * float64(24*time.Hour))), nil
		}
	}

	// Fall back to Go's standard parser.
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0, fmt.Errorf("invalid duration %q: %w", s, err)
	}

	return Duration(d), nil
}

// String returns the duration in the most readable form.
// Durations that are exact multiples of 24h are rendered as "Nd".
func (d Duration) String() string {
	dur := time.Duration(d)
	if dur == 0 {
		return "0s"
	}
	hours := dur.Hours()
	if hours >= 24 && dur%(24*time.Hour) == 0 {
		return fmt.Sprintf("%dd", int(hours/24))
	}

	return dur.String()
}

// Duration returns the underlying time.Duration.
func (d Duration) Duration() time.Duration {
	return time.Duration(d)
}

// UnmarshalYAML implements yaml.Unmarshaler.
func (d *Duration) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var s string
	if err := unmarshal(&s); err != nil {
		return fmt.Errorf("invalid duration: %w", err)
	}
	parsed, err := ParseDuration(s)
	if err != nil {
		return err
	}
	*d = parsed

	return nil
}

// MarshalYAML implements yaml.Marshaler.
func (d Duration) MarshalYAML() (interface{}, error) {
	return d.String(), nil
}

// UnmarshalJSON implements json.Unmarshaler.
func (d *Duration) UnmarshalJSON(data []byte) error {
	s := strings.Trim(string(data), `"`)
	parsed, err := ParseDuration(s)
	if err != nil {
		return err
	}
	*d = parsed

	return nil
}

// MarshalJSON implements json.Marshaler.
func (d Duration) MarshalJSON() ([]byte, error) {
	return []byte(`"` + d.String() + `"`), nil
}
