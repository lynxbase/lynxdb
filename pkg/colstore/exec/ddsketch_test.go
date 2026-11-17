package exec

import (
	"math"
	"math/rand"
	"sort"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/mem"
)

// sketchOf is a percentile sketch outside a group, for tests: the words are
// the zeros and the refs of the positive and the negative store.
type sketchOf struct {
	h *stateHeap
	w [3]int64
}

var sketchAt = sketchWords{zeros: 0, pos: 1, neg: 2}

func (s *sketchOf) add(x float64) {
	if x == 0 {
		s.w[0]++
	} else {
		sketchAt.add(s.h, s.w[:], x, 1)
	}
}

func (s *sketchOf) merge(o *sketchOf) {
	s.w[0] += o.w[0]
	for i, positive := range []bool{true, false} {
		if src := o.h.get(stateRef(o.w[1+i])); len(src) != 0 {
			sketchAt.merge(s.h, s.w[:], src, positive)
		}
	}
}

func (s *sketchOf) quantile(q float64) float64 {
	v, _ := ddQuantile(s.h.get(stateRef(s.w[2])), s.h.get(stateRef(s.w[1])), uint64(s.w[0]), q)

	return v
}

func (s *sketchOf) size() int { return len(s.h.get(stateRef(s.w[1]))) + len(s.h.get(stateRef(s.w[2]))) }

var sketchQuantiles = []float64{0, 0.001, 0.01, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99, 0.999, 1}

// heavyTailed returns latencies that span six orders of magnitude: most
// around a millisecond, a Pareto tail up to minutes, and when signed is set
// some zeros and negative values.
func heavyTailed(n int, seed int64, signed bool) []float64 {
	rng := rand.New(rand.NewSource(seed))
	out := make([]float64, n)
	for i := range out {
		x := 1e-3 * math.Exp(rng.NormFloat64())
		if rng.Intn(10) == 0 {
			x = math.Min(1e-3/math.Pow(rng.Float64(), 1.2), 1e3)
		}
		switch r := rng.Intn(20); {
		case signed && r == 0:
			x = 0
		case signed && r < 4:
			x = -x
		}
		out[i] = x
	}

	return out
}

func TestSketchIsWithinOnePercentOfExactQuantiles(t *testing.T) {
	a := mem.NewArena()
	defer a.Free()
	var h stateHeap
	h.init(a)
	for _, signed := range []bool{false, true} {
		for _, n := range []int{1, 2, 10, 1000, 300_000} {
			values := heavyTailed(n, int64(n), signed)
			s := sketchOf{h: &h}
			for _, x := range values {
				s.add(x)
			}
			sort.Float64s(values)
			if span := values[n-1] / values[0]; n == 300_000 && !signed && span < 1e6 {
				t.Fatalf("the sample spans a factor of %.3g, want six orders of magnitude", span)
			}
			for _, q := range sketchQuantiles {
				exact := values[min(int(q*float64(n)), n-1)]
				got := s.quantile(q)
				if n <= ddExactMax && got != exact {
					t.Fatalf("%d values, signed %v, quantile %g: got %g, want exactly %g", n, signed, q, got, exact)
				}
				if math.Abs(got-exact) > ddAlpha*math.Abs(exact)*(1+1e-9) {
					t.Fatalf("%d values, signed %v, quantile %g: got %g, exact %g, off by %.3f%%", n, signed, q, got, exact, 100*(got-exact)/exact)
				}
			}
			if size := s.size(); size > 8<<10 {
				t.Fatalf("%d values take %d bytes", n, size)
			}
		}
	}
}

func TestSketchesMergeToTheSketchOfAllValues(t *testing.T) {
	a := mem.NewArena()
	defer a.Free()
	var h stateHeap
	h.init(a)
	values := heavyTailed(50_000, 3, true)
	whole := sketchOf{h: &h}
	pieces := make([]sketchOf, 9)
	for i := range pieces {
		pieces[i].h = &h
	}
	for i, x := range values {
		whole.add(x)
		// Pieces of very different sizes, so that counts of every width meet.
		pieces[int(math.Sqrt(float64(i%81)))].add(x)
	}
	for _, order := range [][]int{{0, 1, 2, 3, 4, 5, 6, 7, 8}, {8, 7, 6, 5, 4, 3, 2, 1, 0}, {4, 0, 8, 2, 6, 1, 7, 3, 5}} {
		merged := sketchOf{h: &h}
		for _, p := range order {
			merged.merge(&pieces[p])
		}
		for _, q := range sketchQuantiles {
			if got, want := merged.quantile(q), whole.quantile(q); got != want {
				t.Fatalf("order %v, quantile %g: merged to %g, want %g", order, q, got, want)
			}
		}
	}
}

func TestSketchHasAHardSizeCap(t *testing.T) {
	a := mem.NewArena()
	defer a.Free()
	var h stateHeap
	h.init(a)
	rng := rand.New(rand.NewSource(5))
	var values []float64
	for i := 0; i < 200_000; i++ {
		values = append(values, math.Pow(10, 600*rng.Float64()-300))
	}
	sorted := append([]float64(nil), values...)
	sort.Float64s(sorted)
	forward, backward := sketchOf{h: &h}, sketchOf{h: &h}
	for i, x := range values {
		forward.add(x)
		backward.add(values[len(values)-1-i])
	}
	for _, s := range []*sketchOf{&forward, &backward} {
		if size := s.size(); size > ddHeader+8*ddMaxBuckets {
			t.Fatalf("the store takes %d bytes", size)
		}
		// The buckets kept span a factor of gamma^2048 below the greatest
		// value; quantiles that fall there keep the promise.
		floor := sorted[len(sorted)-1] / math.Pow(ddGamma, ddMaxBuckets-1)
		for _, q := range sketchQuantiles {
			exact := sorted[min(int(q*float64(len(sorted))), len(sorted)-1)]
			got := s.quantile(q)
			if exact > floor && math.Abs(got-exact) > ddAlpha*exact*(1+1e-9) {
				t.Fatalf("quantile %g: got %g, exact %g", q, got, exact)
			}
			if want := forward.quantile(q); got != want {
				t.Fatalf("quantile %g depends on the order of the values: %g and %g", q, got, want)
			}
		}
	}
	// A count that outgrows every width.
	var ref stateRef
	for _, count := range []uint64{200, 100, 70_000, 5_000_000_000, 1} {
		ref = ddAdd(&h, ref, ddKey(42), count)
	}
	ref = ddAdd(&h, ref, ddKey(1e6), 3)
	if got, want := ddTotal(h.get(ref)), uint64(5_000_070_304); got != want {
		t.Fatalf("counted %d values, want %d", got, want)
	}
	if v, _ := ddQuantile(nil, h.get(ref), 0, 0.5); math.Abs(v-42) > ddAlpha*42 {
		t.Fatalf("median %g, want 42", v)
	}
}

// TestSketchOfFewDistinctNumbersIsExact covers the numbers of a small
// domain, such as response statuses, however many there are of each and
// however they are split and merged.
func TestSketchOfFewDistinctNumbersIsExact(t *testing.T) {
	a := mem.NewArena()
	defer a.Free()
	var h stateHeap
	h.init(a)
	rng := rand.New(rand.NewSource(9))
	domain := []float64{200, 200, 200, 200, 201, 204, 301, 304, 400, 404, 404, 500, 503, 0, -1, 0.5}
	for _, distinct := range []int{1, len(domain), ddExactMax, ddExactMax + 1, 200} {
		var values []float64
		whole := sketchOf{h: &h}
		pieces := []sketchOf{{h: &h}, {h: &h}, {h: &h}}
		for i := 0; i < 20_000; i++ {
			x := domain[rng.Intn(min(distinct, len(domain)))]
			if distinct > len(domain) && rng.Intn(2) == 0 {
				x = float64(1000 + rng.Intn(distinct-11))
			}
			values = append(values, x)
			whole.add(x)
			pieces[min(rng.Intn(40), 2)].add(x)
		}
		sort.Float64s(values)
		merged := sketchOf{h: &h}
		for p := range pieces {
			merged.merge(&pieces[p])
		}
		for _, q := range sketchQuantiles {
			exact := values[min(int(q*float64(len(values))), len(values)-1)]
			got := whole.quantile(q)
			if distinct <= ddExactMax && got != exact {
				t.Fatalf("%d distinct numbers, quantile %g: got %g, want exactly %g", distinct, q, got, exact)
			}
			if math.Abs(got-exact) > ddAlpha*math.Abs(exact) {
				t.Fatalf("%d distinct numbers, quantile %g: got %g, exact %g", distinct, q, got, exact)
			}
			if m := merged.quantile(q); m != got {
				t.Fatalf("%d distinct numbers, quantile %g: merged to %g, want %g", distinct, q, m, got)
			}
		}
	}
}
