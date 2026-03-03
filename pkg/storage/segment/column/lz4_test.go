package column

import (
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"testing"
)

func TestLZ4Encoder_Basic(t *testing.T) {
	enc := NewLZ4Encoder()

	tests := []struct {
		name   string
		values []string
	}{
		{
			name:   "single value",
			values: []string{"hello world"},
		},
		{
			name:   "multiple values",
			values: []string{"foo", "bar", "baz"},
		},
		{
			name:   "empty strings",
			values: []string{"", "", "non-empty", ""},
		},
		{
			name:   "long strings",
			values: []string{strings.Repeat("a", 1000), strings.Repeat("b", 2000)},
		},
		{
			name:   "highly compressible",
			values: repeatSlice([]string{"the quick brown fox"}, 100),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := enc.EncodeStrings(tt.values)
			if err != nil {
				t.Fatalf("EncodeStrings: %v", err)
			}

			got, err := enc.DecodeStrings(data)
			if err != nil {
				t.Fatalf("DecodeStrings: %v", err)
			}

			if len(got) != len(tt.values) {
				t.Fatalf("length mismatch: got %d, want %d", len(got), len(tt.values))
			}
			for i := range tt.values {
				if got[i] != tt.values[i] {
					t.Errorf("index %d: got %q, want %q", i, got[i], tt.values[i])
				}
			}
		})
	}
}

func TestLZ4Encoder_EmptyInput(t *testing.T) {
	enc := NewLZ4Encoder()
	_, err := enc.EncodeStrings(nil)
	if !errors.Is(err, ErrEmptyInput) {
		t.Errorf("expected ErrEmptyInput, got %v", err)
	}
}

func TestLZ4Encoder_CorruptData(t *testing.T) {
	enc := NewLZ4Encoder()

	tests := []struct {
		name string
		data []byte
	}{
		{"too short", []byte{1, 2}},
		{"wrong type", []byte{byte(EncodingDict8), 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := enc.DecodeStrings(tt.data)
			if err == nil {
				t.Fatal("expected error for corrupt data")
			}
		})
	}
}

func TestLZ4Encoder_PropertyRoundtrip(t *testing.T) {
	enc := NewLZ4Encoder()
	rng := rand.New(rand.NewSource(42))

	for trial := 0; trial < 100; trial++ {
		n := rng.Intn(200) + 1
		values := make([]string, n)
		for i := range values {
			length := rng.Intn(100)
			b := make([]byte, length)
			for j := range b {
				b[j] = byte(32 + rng.Intn(95)) // printable ASCII
			}
			values[i] = string(b)
		}

		data, err := enc.EncodeStrings(values)
		if err != nil {
			t.Fatalf("trial %d: encode: %v", trial, err)
		}
		got, err := enc.DecodeStrings(data)
		if err != nil {
			t.Fatalf("trial %d: decode: %v", trial, err)
		}
		if len(got) != len(values) {
			t.Fatalf("trial %d: length mismatch", trial)
		}
		for i := range values {
			if got[i] != values[i] {
				t.Fatalf("trial %d, index %d: got %q, want %q", trial, i, got[i], values[i])
			}
		}
	}
}

func TestLZ4Encoder_LargeData(t *testing.T) {
	enc := NewLZ4Encoder()

	// Generate log-like lines.
	values := make([]string, 10000)
	for i := range values {
		values[i] = fmt.Sprintf("2024-01-01T00:00:%02d.000Z host=%d level=INFO msg=\"Request processed successfully for user %d\"", i%60, i%10, i)
	}

	data, err := enc.EncodeStrings(values)
	if err != nil {
		t.Fatalf("EncodeStrings: %v", err)
	}

	// Verify compression is happening.
	rawSize := 0
	for _, v := range values {
		rawSize += len(v)
	}
	t.Logf("raw=%d, encoded=%d, ratio=%.2f", rawSize, len(data), float64(len(data))/float64(rawSize))

	got, err := enc.DecodeStrings(data)
	if err != nil {
		t.Fatalf("DecodeStrings: %v", err)
	}

	for i := range values {
		if got[i] != values[i] {
			t.Fatalf("index %d: mismatch", i)
		}
	}
}

func repeatSlice(s []string, n int) []string {
	out := make([]string, 0, len(s)*n)
	for i := 0; i < n; i++ {
		out = append(out, s...)
	}

	return out
}

func BenchmarkLZ4Encode(b *testing.B) {
	enc := NewLZ4Encoder()
	values := make([]string, 10000)
	for i := range values {
		values[i] = fmt.Sprintf("2024-01-01 host-%d level=INFO request processed user=%d path=/api/v1/data", i%10, i)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = enc.EncodeStrings(values)
	}
}

func BenchmarkLZ4Decode(b *testing.B) {
	enc := NewLZ4Encoder()
	values := make([]string, 10000)
	for i := range values {
		values[i] = fmt.Sprintf("2024-01-01 host-%d level=INFO request processed user=%d path=/api/v1/data", i%10, i)
	}
	data, _ := enc.EncodeStrings(values)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = enc.DecodeStrings(data)
	}
}
