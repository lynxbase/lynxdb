package config

import (
	"encoding/json"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestParseByteSize(t *testing.T) {
	tests := []struct {
		input   string
		want    ByteSize
		wantErr bool
	}{
		{"0", 0, false},
		{"1024", 1024, false},
		{"4mb", 4 * MB, false},
		{"4MB", 4 * MB, false},
		{"4Mb", 4 * MB, false},
		{"256mb", 256 * MB, false},
		{"1gb", 1 * GB, false},
		{"10gb", 10 * GB, false},
		{"512kb", 512 * KB, false},
		{"1tb", 1 * TB, false},
		{"0.5gb", ByteSize(0.5 * float64(GB)), false},
		{"", 0, true},
		{"abc", 0, true},
		{"4xx", 0, true},
		{"-4mb", 0, true},
		{"-1000", 0, true},
		{"-1gb", 0, true},
		{"-0.5kb", 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := ParseByteSize(tt.input)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ParseByteSize(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
			}
			if got != tt.want {
				t.Errorf("ParseByteSize(%q) = %d, want %d", tt.input, got, tt.want)
			}
		})
	}
}

func TestByteSizeString(t *testing.T) {
	tests := []struct {
		input ByteSize
		want  string
	}{
		{0, "0"},
		{512 * KB, "512kb"},
		{4 * MB, "4mb"},
		{256 * MB, "256mb"},
		{1 * GB, "1gb"},
		{10 * GB, "10gb"},
		{1 * TB, "1tb"},
		{1023, "1023"},
	}
	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			if got := tt.input.String(); got != tt.want {
				t.Errorf("ByteSize(%d).String() = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestByteSizeYAML(t *testing.T) {
	type wrapper struct {
		Size ByteSize `yaml:"size"`
	}

	// Unmarshal string.
	var w wrapper
	if err := yaml.Unmarshal([]byte("size: 4mb"), &w); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if w.Size != 4*MB {
		t.Errorf("got %d, want %d", w.Size, 4*MB)
	}

	// Marshal round-trip.
	data, err := yaml.Marshal(w)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var w2 wrapper
	if err := yaml.Unmarshal(data, &w2); err != nil {
		t.Fatalf("unmarshal round-trip: %v", err)
	}
	if w2.Size != 4*MB {
		t.Errorf("round-trip: got %d, want %d", w2.Size, 4*MB)
	}
}

func TestByteSizeJSON(t *testing.T) {
	type wrapper struct {
		Size ByteSize `json:"size"`
	}

	// Unmarshal string.
	var w wrapper
	if err := json.Unmarshal([]byte(`{"size":"4mb"}`), &w); err != nil {
		t.Fatalf("unmarshal string: %v", err)
	}
	if w.Size != 4*MB {
		t.Errorf("got %d, want %d", w.Size, 4*MB)
	}

	// Unmarshal integer.
	var w2 wrapper
	if err := json.Unmarshal([]byte(`{"size":4194304}`), &w2); err != nil {
		t.Fatalf("unmarshal int: %v", err)
	}
	if w2.Size != 4*MB {
		t.Errorf("got %d, want %d", w2.Size, 4*MB)
	}

	// Marshal round-trip.
	data, err := json.Marshal(w)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var w3 wrapper
	if err := json.Unmarshal(data, &w3); err != nil {
		t.Fatalf("unmarshal round-trip: %v", err)
	}
	if w3.Size != 4*MB {
		t.Errorf("round-trip: got %d, want %d", w3.Size, 4*MB)
	}
}
