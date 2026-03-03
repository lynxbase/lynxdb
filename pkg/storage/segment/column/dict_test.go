package column

import (
	"errors"
	"fmt"
	"math/rand"
	"testing"
)

func TestDictEncoder_Basic(t *testing.T) {
	enc := NewDictEncoder()

	tests := []struct {
		name   string
		values []string
	}{
		{
			name:   "single value",
			values: []string{"hello"},
		},
		{
			name:   "repeated values",
			values: []string{"a", "b", "a", "b", "a"},
		},
		{
			name:   "all same",
			values: []string{"x", "x", "x", "x"},
		},
		{
			name:   "empty strings",
			values: []string{"", "", "a", ""},
		},
		{
			name:   "many unique (under 256)",
			values: generateUniqueStrings(200),
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

func TestDictEncoder_8bitAnd16bit(t *testing.T) {
	enc := NewDictEncoder()

	t.Run("8-bit (<=256 unique)", func(t *testing.T) {
		values := make([]string, 1000)
		for i := range values {
			values[i] = fmt.Sprintf("val_%d", i%256)
		}
		data, err := enc.EncodeStrings(values)
		if err != nil {
			t.Fatalf("EncodeStrings: %v", err)
		}
		if EncodingType(data[0]) != EncodingDict8 {
			t.Errorf("expected Dict8, got %v", EncodingType(data[0]))
		}
		got, err := enc.DecodeStrings(data)
		if err != nil {
			t.Fatalf("DecodeStrings: %v", err)
		}
		for i := range values {
			if got[i] != values[i] {
				t.Errorf("index %d: got %q, want %q", i, got[i], values[i])
			}
		}
	})

	t.Run("16-bit (>256 unique)", func(t *testing.T) {
		values := make([]string, 2000)
		for i := range values {
			values[i] = fmt.Sprintf("val_%d", i%500)
		}
		data, err := enc.EncodeStrings(values)
		if err != nil {
			t.Fatalf("EncodeStrings: %v", err)
		}
		if EncodingType(data[0]) != EncodingDict16 {
			t.Errorf("expected Dict16, got %v", EncodingType(data[0]))
		}
		got, err := enc.DecodeStrings(data)
		if err != nil {
			t.Fatalf("DecodeStrings: %v", err)
		}
		for i := range values {
			if got[i] != values[i] {
				t.Errorf("index %d: got %q, want %q", i, got[i], values[i])
			}
		}
	})
}

func TestDictEncoder_TooManyUnique(t *testing.T) {
	enc := NewDictEncoder()
	values := generateUniqueStrings(65537)
	_, err := enc.EncodeStrings(values)
	if err == nil {
		t.Fatal("expected error for >65536 unique values")
	}
}

func TestDictEncoder_EmptyInput(t *testing.T) {
	enc := NewDictEncoder()
	_, err := enc.EncodeStrings(nil)
	if !errors.Is(err, ErrEmptyInput) {
		t.Errorf("expected ErrEmptyInput, got %v", err)
	}
}

func TestDictEncoder_CorruptData(t *testing.T) {
	enc := NewDictEncoder()

	tests := []struct {
		name string
		data []byte
	}{
		{"too short", []byte{1, 2}},
		{"truncated", []byte{byte(EncodingDict8), 5, 0, 0, 0, 2, 0, 0, 0}},
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

func TestDictEncoder_PropertyRoundtrip(t *testing.T) {
	enc := NewDictEncoder()
	rng := rand.New(rand.NewSource(42))

	for trial := 0; trial < 100; trial++ {
		n := rng.Intn(500) + 1
		cardinality := rng.Intn(min(n, 256)) + 1
		palette := generateUniqueStringsRng(rng, cardinality)
		values := make([]string, n)
		for i := range values {
			values[i] = palette[rng.Intn(len(palette))]
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

func generateUniqueStrings(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("unique_%d", i)
	}

	return out
}

func generateUniqueStringsRng(rng *rand.Rand, n int) []string {
	out := make([]string, n)
	for i := range out {
		length := rng.Intn(20) + 1
		b := make([]byte, length)
		for j := range b {
			b[j] = byte('a' + rng.Intn(26))
		}
		out[i] = string(b)
	}

	return out
}

func BenchmarkDictEncode8bit(b *testing.B) {
	enc := NewDictEncoder()
	values := make([]string, 10000)
	for i := range values {
		values[i] = fmt.Sprintf("host-%d", i%50)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = enc.EncodeStrings(values)
	}
}

func BenchmarkDictDecode8bit(b *testing.B) {
	enc := NewDictEncoder()
	values := make([]string, 10000)
	for i := range values {
		values[i] = fmt.Sprintf("host-%d", i%50)
	}
	data, _ := enc.EncodeStrings(values)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = enc.DecodeStrings(data)
	}
}

func BenchmarkDictEncode16bit(b *testing.B) {
	enc := NewDictEncoder()
	values := make([]string, 10000)
	for i := range values {
		values[i] = fmt.Sprintf("host-%d", i%500)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = enc.EncodeStrings(values)
	}
}

func BenchmarkDictDecode16bit(b *testing.B) {
	enc := NewDictEncoder()
	values := make([]string, 10000)
	for i := range values {
		values[i] = fmt.Sprintf("host-%d", i%500)
	}
	data, _ := enc.EncodeStrings(values)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = enc.DecodeStrings(data)
	}
}
