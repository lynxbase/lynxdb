package config

import (
	"encoding/json"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

func TestParseDuration(t *testing.T) {
	tests := []struct {
		input   string
		want    Duration
		wantErr bool
	}{
		{"7d", Duration(7 * 24 * time.Hour), false},
		{"30d", Duration(30 * 24 * time.Hour), false},
		{"90d", Duration(90 * 24 * time.Hour), false},
		{"1d", Duration(24 * time.Hour), false},
		{"100ms", Duration(100 * time.Millisecond), false},
		{"30s", Duration(30 * time.Second), false},
		{"5m", Duration(5 * time.Minute), false},
		{"2h", Duration(2 * time.Hour), false},
		{"0s", Duration(0), false},
		{"0.5d", Duration(12 * time.Hour), false},
		{"", 0, true},
		{"abc", 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := ParseDuration(tt.input)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ParseDuration(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
			}
			if got != tt.want {
				t.Errorf("ParseDuration(%q) = %v, want %v", tt.input, time.Duration(got), time.Duration(tt.want))
			}
		})
	}
}

func TestDurationString(t *testing.T) {
	tests := []struct {
		input Duration
		want  string
	}{
		{Duration(0), "0s"},
		{Duration(7 * 24 * time.Hour), "7d"},
		{Duration(30 * 24 * time.Hour), "30d"},
		{Duration(100 * time.Millisecond), "100ms"},
		{Duration(30 * time.Second), "30s"},
		{Duration(5 * time.Minute), "5m0s"},
		{Duration(2*time.Hour + 30*time.Minute), "2h30m0s"},
	}
	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			if got := tt.input.String(); got != tt.want {
				t.Errorf("Duration(%v).String() = %q, want %q", time.Duration(tt.input), got, tt.want)
			}
		})
	}
}

func TestDurationYAML(t *testing.T) {
	type wrapper struct {
		TTL Duration `yaml:"ttl"`
	}

	var w wrapper
	if err := yaml.Unmarshal([]byte("ttl: 7d"), &w); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if w.TTL != Duration(7*24*time.Hour) {
		t.Errorf("got %v, want 7d", time.Duration(w.TTL))
	}

	// Round-trip.
	data, err := yaml.Marshal(w)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var w2 wrapper
	if err := yaml.Unmarshal(data, &w2); err != nil {
		t.Fatalf("unmarshal round-trip: %v", err)
	}
	if w2.TTL != w.TTL {
		t.Errorf("round-trip: got %v, want %v", time.Duration(w2.TTL), time.Duration(w.TTL))
	}
}

func TestDurationJSON(t *testing.T) {
	type wrapper struct {
		TTL Duration `json:"ttl"`
	}

	var w wrapper
	if err := json.Unmarshal([]byte(`{"ttl":"30d"}`), &w); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if w.TTL != Duration(30*24*time.Hour) {
		t.Errorf("got %v, want 30d", time.Duration(w.TTL))
	}

	// Round-trip.
	data, err := json.Marshal(w)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var w2 wrapper
	if err := json.Unmarshal(data, &w2); err != nil {
		t.Fatalf("unmarshal round-trip: %v", err)
	}
	if w2.TTL != w.TTL {
		t.Errorf("round-trip: got %v, want %v", time.Duration(w2.TTL), time.Duration(w.TTL))
	}
}
