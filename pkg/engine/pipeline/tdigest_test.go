package pipeline

import (
	"math"
	"math/rand"
	"sort"
	"testing"
)

// exactQuantile computes the empirical quantile of sorted data via the same
// rank convention the digest targets (rank q*N interpolated on midpoints).
func exactQuantile(sorted []float64, q float64) float64 {
	n := len(sorted)
	if n == 0 {
		return 0
	}
	pos := q*float64(n) - 0.5
	if pos <= 0 {
		return sorted[0]
	}
	if pos >= float64(n-1) {
		return sorted[n-1]
	}
	lo := int(pos)
	frac := pos - float64(lo)

	return sorted[lo] + frac*(sorted[lo+1]-sorted[lo])
}

func checkQuantiles(t *testing.T, td *TDigest, sorted []float64, relTol float64) {
	t.Helper()
	span := sorted[len(sorted)-1] - sorted[0]
	for _, q := range []float64{0.01, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99, 0.999} {
		got := td.Quantile(q)
		want := exactQuantile(sorted, q)
		if relErr := math.Abs(got-want) / span; relErr > relTol {
			t.Errorf("q=%.3f: got %.4f, want %.4f (rel err %.4f > %.4f)", q, got, want, relErr, relTol)
		}
	}
}

func TestTDigestSmallExact(t *testing.T) {
	td := NewTDigest(100)
	data := make([]float64, 100)
	for i := range data {
		data[i] = float64(i + 1)
		td.Add(data[i])
	}
	// With N=count <= compression, centroids stay near-singleton; quantiles
	// should be within one value of exact.
	if got := td.Quantile(0.5); math.Abs(got-50.5) > 1 {
		t.Errorf("p50: got %.2f, want 50.5 +/- 1", got)
	}
	if got := td.Quantile(0.99); math.Abs(got-99.5) > 1.5 {
		t.Errorf("p99: got %.2f, want ~99.5", got)
	}
	if got := td.Quantile(0); got != 1 {
		t.Errorf("q=0: got %.2f, want 1 (min)", got)
	}
	if got := td.Quantile(1); got != 100 {
		t.Errorf("q=1: got %.2f, want 100 (max)", got)
	}
}

func TestTDigestWeighted(t *testing.T) {
	td := NewTDigest(100)
	td.AddWeighted(10, 100)
	td.AddWeighted(100, 1)
	td.AddWeighted(1_000, 0)

	got := td.Quantile(0.5)
	if got < 10 || got > 20 {
		t.Fatalf("weighted p50 = %f, want near heavily weighted value", got)
	}
	if got := td.Count(); got != 101 {
		t.Fatalf("weighted count = %f, want 101", got)
	}
}

func TestTDigestUniformAccuracy(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	td := NewTDigest(100)
	data := make([]float64, 100_000)
	for i := range data {
		data[i] = rng.Float64() * 1000
		td.Add(data[i])
	}
	sort.Float64s(data)
	checkQuantiles(t, td, data, 0.01)
}

func TestTDigestSkewedAccuracy(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	td := NewTDigest(100)
	data := make([]float64, 100_000)
	for i := range data {
		data[i] = math.Exp(rng.NormFloat64()) // lognormal
		td.Add(data[i])
	}
	sort.Float64s(data)
	checkQuantiles(t, td, data, 0.01)
}

func TestTDigestDeterministicWithDuplicates(t *testing.T) {
	build := func() *TDigest {
		td := NewTDigest(50)
		for i := 0; i < 10_000; i++ {
			td.Add(float64(i % 100)) // heavy duplication → equal-mean centroids
		}
		return td
	}
	a, b := build(), build()
	for _, q := range []float64{0.1, 0.5, 0.9, 0.99} {
		if av, bv := a.Quantile(q), b.Quantile(q); av != bv {
			t.Errorf("q=%.2f: non-deterministic estimates %.6f vs %.6f", q, av, bv)
		}
	}
}
