package column

import (
	"errors"
	"math"
	"math/rand"
	"testing"
)

func TestGorillaEncoder_Basic(t *testing.T) {
	enc := NewGorillaEncoder()

	tests := []struct {
		name   string
		values []float64
	}{
		{
			name:   "single value",
			values: []float64{3.14},
		},
		{
			name:   "all same",
			values: []float64{1.0, 1.0, 1.0, 1.0},
		},
		{
			name:   "increasing",
			values: []float64{1.0, 2.0, 3.0, 4.0, 5.0},
		},
		{
			name:   "small deltas",
			values: []float64{100.0, 100.1, 100.2, 100.3, 100.4},
		},
		{
			name:   "with zeros",
			values: []float64{0.0, 0.0, 1.0, 0.0},
		},
		{
			name:   "negative values",
			values: []float64{-1.5, -2.5, -0.5, 0.5, 1.5},
		},
		{
			name:   "special: very small",
			values: []float64{1e-300, 2e-300, 3e-300},
		},
		{
			name:   "special: very large",
			values: []float64{1e300, 2e300, 3e300},
		},
		{
			name:   "mixed magnitude",
			values: []float64{0.001, 1000.0, 0.001, 1000.0},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := enc.EncodeFloat64s(tt.values)
			if err != nil {
				t.Fatalf("EncodeFloat64s: %v", err)
			}

			got, err := enc.DecodeFloat64s(data)
			if err != nil {
				t.Fatalf("DecodeFloat64s: %v", err)
			}

			if len(got) != len(tt.values) {
				t.Fatalf("length mismatch: got %d, want %d", len(got), len(tt.values))
			}
			for i := range tt.values {
				if math.Float64bits(got[i]) != math.Float64bits(tt.values[i]) {
					t.Errorf("index %d: got %v (bits=%016x), want %v (bits=%016x)",
						i, got[i], math.Float64bits(got[i]), tt.values[i], math.Float64bits(tt.values[i]))
				}
			}
		})
	}
}

func TestGorillaEncoder_SpecialValues(t *testing.T) {
	enc := NewGorillaEncoder()

	values := []float64{
		0.0,
		math.Inf(1),
		math.Inf(-1),
		math.MaxFloat64,
		math.SmallestNonzeroFloat64,
		-math.MaxFloat64,
	}

	data, err := enc.EncodeFloat64s(values)
	if err != nil {
		t.Fatalf("EncodeFloat64s: %v", err)
	}

	got, err := enc.DecodeFloat64s(data)
	if err != nil {
		t.Fatalf("DecodeFloat64s: %v", err)
	}

	for i := range values {
		if math.Float64bits(got[i]) != math.Float64bits(values[i]) {
			t.Errorf("index %d: got %v, want %v", i, got[i], values[i])
		}
	}
}

func TestGorillaEncoder_NaN(t *testing.T) {
	enc := NewGorillaEncoder()
	values := []float64{math.NaN(), math.NaN(), 1.0}

	data, err := enc.EncodeFloat64s(values)
	if err != nil {
		t.Fatalf("EncodeFloat64s: %v", err)
	}

	got, err := enc.DecodeFloat64s(data)
	if err != nil {
		t.Fatalf("DecodeFloat64s: %v", err)
	}

	if !math.IsNaN(got[0]) {
		t.Errorf("index 0: expected NaN, got %v", got[0])
	}
	if !math.IsNaN(got[1]) {
		t.Errorf("index 1: expected NaN, got %v", got[1])
	}
	if got[2] != 1.0 {
		t.Errorf("index 2: expected 1.0, got %v", got[2])
	}
}

func TestGorillaEncoder_EmptyInput(t *testing.T) {
	enc := NewGorillaEncoder()
	_, err := enc.EncodeFloat64s(nil)
	if !errors.Is(err, ErrEmptyInput) {
		t.Errorf("expected ErrEmptyInput, got %v", err)
	}
}

func TestGorillaEncoder_CorruptData(t *testing.T) {
	enc := NewGorillaEncoder()

	tests := []struct {
		name string
		data []byte
	}{
		{"too short", []byte{1, 2}},
		{"wrong type", []byte{byte(EncodingDelta), 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := enc.DecodeFloat64s(tt.data)
			if err == nil {
				t.Fatal("expected error for corrupt data")
			}
		})
	}
}

func TestGorillaEncoder_PropertyRoundtrip(t *testing.T) {
	enc := NewGorillaEncoder()
	rng := rand.New(rand.NewSource(42))

	for trial := 0; trial < 100; trial++ {
		n := rng.Intn(500) + 1
		values := make([]float64, n)
		base := rng.Float64() * 1000
		for i := range values {
			values[i] = base + rng.NormFloat64()*10
		}

		data, err := enc.EncodeFloat64s(values)
		if err != nil {
			t.Fatalf("trial %d: encode: %v", trial, err)
		}
		got, err := enc.DecodeFloat64s(data)
		if err != nil {
			t.Fatalf("trial %d: decode: %v", trial, err)
		}
		if len(got) != len(values) {
			t.Fatalf("trial %d: length mismatch", trial)
		}
		for i := range values {
			if math.Float64bits(got[i]) != math.Float64bits(values[i]) {
				t.Fatalf("trial %d, index %d: got %v, want %v", trial, i, got[i], values[i])
			}
		}
	}
}

func TestGorillaEncoder_Compression(t *testing.T) {
	enc := NewGorillaEncoder()

	// Similar values should compress well.
	values := make([]float64, 10000)
	for i := range values {
		values[i] = 100.0 + float64(i)*0.001
	}

	data, err := enc.EncodeFloat64s(values)
	if err != nil {
		t.Fatalf("EncodeFloat64s: %v", err)
	}

	rawSize := len(values) * 8
	t.Logf("raw=%d, encoded=%d, ratio=%.2f", rawSize, len(data), float64(len(data))/float64(rawSize))

	got, err := enc.DecodeFloat64s(data)
	if err != nil {
		t.Fatalf("DecodeFloat64s: %v", err)
	}

	for i := range values {
		if math.Float64bits(got[i]) != math.Float64bits(values[i]) {
			t.Fatalf("index %d: got %v, want %v", i, got[i], values[i])
		}
	}
}

func BenchmarkGorillaEncode(b *testing.B) {
	enc := NewGorillaEncoder()
	values := make([]float64, 10000)
	for i := range values {
		values[i] = 100.0 + float64(i)*0.01
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = enc.EncodeFloat64s(values)
	}
}

func BenchmarkGorillaDecode(b *testing.B) {
	enc := NewGorillaEncoder()
	values := make([]float64, 10000)
	for i := range values {
		values[i] = 100.0 + float64(i)*0.01
	}
	data, _ := enc.EncodeFloat64s(values)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = enc.DecodeFloat64s(data)
	}
}
