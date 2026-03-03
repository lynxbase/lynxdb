package column

import (
	"errors"
	"math"
	"math/rand"
	"testing"
	"time"
)

func TestDeltaEncoder_Basic(t *testing.T) {
	enc := NewDeltaEncoder()

	tests := []struct {
		name   string
		values []int64
	}{
		{
			name:   "single value",
			values: []int64{42},
		},
		{
			name:   "monotonic increasing",
			values: []int64{100, 200, 300, 400, 500},
		},
		{
			name:   "monotonic decreasing",
			values: []int64{500, 400, 300, 200, 100},
		},
		{
			name:   "all same",
			values: []int64{7, 7, 7, 7, 7},
		},
		{
			name:   "with negatives",
			values: []int64{-100, -50, 0, 50, 100},
		},
		{
			name:   "zeros",
			values: []int64{0, 0, 0, 0},
		},
		{
			name:   "large values",
			values: []int64{math.MaxInt64, math.MaxInt64 - 1, math.MaxInt64 - 2},
		},
		{
			name:   "min values",
			values: []int64{math.MinInt64, math.MinInt64 + 1, math.MinInt64 + 2},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := enc.EncodeInt64s(tt.values)
			if err != nil {
				t.Fatalf("EncodeInt64s: %v", err)
			}

			got, err := enc.DecodeInt64s(data)
			if err != nil {
				t.Fatalf("DecodeInt64s: %v", err)
			}

			if len(got) != len(tt.values) {
				t.Fatalf("length mismatch: got %d, want %d", len(got), len(tt.values))
			}
			for i := range tt.values {
				if got[i] != tt.values[i] {
					t.Errorf("index %d: got %d, want %d", i, got[i], tt.values[i])
				}
			}
		})
	}
}

func TestDeltaEncoder_Timestamps(t *testing.T) {
	enc := NewDeltaEncoder()

	// Simulate realistic timestamps: events every ~100ms with jitter.
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano()
	rng := rand.New(rand.NewSource(42))
	values := make([]int64, 10000)
	ts := base
	for i := range values {
		values[i] = ts
		ts += int64(90+rng.Intn(20)) * int64(time.Millisecond) // 90-110ms
	}

	data, err := enc.EncodeInt64s(values)
	if err != nil {
		t.Fatalf("EncodeInt64s: %v", err)
	}

	// Check compression ratio.
	rawSize := len(values) * 8
	t.Logf("raw=%d, encoded=%d, ratio=%.2f", rawSize, len(data), float64(len(data))/float64(rawSize))

	got, err := enc.DecodeInt64s(data)
	if err != nil {
		t.Fatalf("DecodeInt64s: %v", err)
	}

	for i := range values {
		if got[i] != values[i] {
			t.Fatalf("index %d: got %d, want %d", i, got[i], values[i])
		}
	}
}

func TestDeltaEncoder_EmptyInput(t *testing.T) {
	enc := NewDeltaEncoder()
	_, err := enc.EncodeInt64s(nil)
	if !errors.Is(err, ErrEmptyInput) {
		t.Errorf("expected ErrEmptyInput, got %v", err)
	}
}

func TestDeltaEncoder_CorruptData(t *testing.T) {
	enc := NewDeltaEncoder()

	tests := []struct {
		name string
		data []byte
	}{
		{"too short", []byte{1, 2}},
		{"wrong type", []byte{byte(EncodingLZ4), 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := enc.DecodeInt64s(tt.data)
			if err == nil {
				t.Fatal("expected error for corrupt data")
			}
		})
	}
}

func TestDeltaEncoder_PropertyRoundtrip(t *testing.T) {
	enc := NewDeltaEncoder()
	rng := rand.New(rand.NewSource(42))

	for trial := 0; trial < 100; trial++ {
		n := rng.Intn(500) + 1
		values := make([]int64, n)
		// Mostly monotonic with some jitter.
		v := rng.Int63n(1e15)
		for i := range values {
			values[i] = v
			v += rng.Int63n(1000) - 100 // mostly increasing
		}

		data, err := enc.EncodeInt64s(values)
		if err != nil {
			t.Fatalf("trial %d: encode: %v", trial, err)
		}
		got, err := enc.DecodeInt64s(data)
		if err != nil {
			t.Fatalf("trial %d: decode: %v", trial, err)
		}
		if len(got) != len(values) {
			t.Fatalf("trial %d: length mismatch", trial)
		}
		for i := range values {
			if got[i] != values[i] {
				t.Fatalf("trial %d, index %d: got %d, want %d", trial, i, got[i], values[i])
			}
		}
	}
}

func TestZigzag(t *testing.T) {
	tests := []struct {
		input    int64
		expected uint64
	}{
		{0, 0},
		{-1, 1},
		{1, 2},
		{-2, 3},
		{2, 4},
		{math.MaxInt64, math.MaxUint64 - 1},
		{math.MinInt64, math.MaxUint64},
	}

	for _, tt := range tests {
		// Inline zigzag encode: (v << 1) ^ (v >> 63)
		encoded := uint64((tt.input << 1) ^ (tt.input >> 63))
		if encoded != tt.expected {
			t.Errorf("zigzagEncode(%d): got %d, want %d", tt.input, encoded, tt.expected)
		}
		// Inline zigzag decode: (v >> 1) ^ -(v & 1)
		decoded := int64(encoded>>1) ^ -int64(encoded&1)
		if decoded != tt.input {
			t.Errorf("zigzagDecode(%d): got %d, want %d", tt.expected, decoded, tt.input)
		}
	}
}

func BenchmarkDeltaEncode(b *testing.B) {
	enc := NewDeltaEncoder()
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano()
	values := make([]int64, 10000)
	for i := range values {
		values[i] = base + int64(i)*int64(100*time.Millisecond)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = enc.EncodeInt64s(values)
	}
}

func BenchmarkDeltaDecode(b *testing.B) {
	enc := NewDeltaEncoder()
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano()
	values := make([]int64, 10000)
	for i := range values {
		values[i] = base + int64(i)*int64(100*time.Millisecond)
	}
	data, _ := enc.EncodeInt64s(values)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = enc.DecodeInt64s(data)
	}
}
