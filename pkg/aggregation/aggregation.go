package aggregation

import (
	"fmt"
	"math"
	"sort"
	"strconv"
)

// Aggregator defines the interface for aggregation functions.
// Pattern: Init → Update (per value) → Merge (combine states) → Finalize (produce result).
type Aggregator interface {
	Init() State
	Update(state State, value interface{})
	Merge(a, b State) State
	Finalize(state State) interface{}
	Name() string
}

// State holds aggregation state. Concrete type depends on the aggregator.
type State interface{}

// Registry maps aggregation function names to factory functions.
var Registry = map[string]func() Aggregator{
	"count":      func() Aggregator { return &CountAgg{} },
	"sum":        func() Aggregator { return &SumAgg{} },
	"avg":        func() Aggregator { return &AvgAgg{} },
	"min":        func() Aggregator { return &MinAgg{} },
	"max":        func() Aggregator { return &MaxAgg{} },
	"dc":         func() Aggregator { return &DCCountAgg{} },
	"values":     func() Aggregator { return &ValuesAgg{} },
	"first":      func() Aggregator { return &FirstAgg{} },
	"last":       func() Aggregator { return &LastAgg{} },
	"percentile": func() Aggregator { return &PercentileAgg{Pct: 50} },
	"stdev":      func() Aggregator { return &StdevAgg{} },
	"perc50":     func() Aggregator { return &PercentileAgg{Pct: 50} },
	"perc75":     func() Aggregator { return &PercentileAgg{Pct: 75} },
	"perc90":     func() Aggregator { return &PercentileAgg{Pct: 90} },
	"perc95":     func() Aggregator { return &PercentileAgg{Pct: 95} },
	"perc99":     func() Aggregator { return &PercentileAgg{Pct: 99} },
	"earliest":   func() Aggregator { return &FirstAgg{} },
	"latest":     func() Aggregator { return &LastAgg{} },
}

// NewAggregator creates an aggregator by name.
func NewAggregator(name string) (Aggregator, error) {
	factory, ok := Registry[name]
	if !ok {
		return nil, fmt.Errorf("aggregation: unknown function %q", name)
	}

	return factory(), nil
}

// Count

type countState struct {
	count int64
}

type CountAgg struct{}

func (a *CountAgg) Name() string { return "count" }
func (a *CountAgg) Init() State  { return &countState{} }
func (a *CountAgg) Update(state State, value interface{}) {
	state.(*countState).count++
}
func (a *CountAgg) Merge(sa, sb State) State {
	return &countState{count: sa.(*countState).count + sb.(*countState).count}
}
func (a *CountAgg) Finalize(state State) interface{} {
	return state.(*countState).count
}

// Sum

type sumState struct {
	sum float64
}

type SumAgg struct{}

func (a *SumAgg) Name() string { return "sum" }
func (a *SumAgg) Init() State  { return &sumState{} }
func (a *SumAgg) Update(state State, value interface{}) {
	state.(*sumState).sum += toFloat64(value)
}
func (a *SumAgg) Merge(sa, sb State) State {
	return &sumState{sum: sa.(*sumState).sum + sb.(*sumState).sum}
}
func (a *SumAgg) Finalize(state State) interface{} {
	return state.(*sumState).sum
}

// Avg

type avgState struct {
	sum   float64
	count int64
}

type AvgAgg struct{}

func (a *AvgAgg) Name() string { return "avg" }
func (a *AvgAgg) Init() State  { return &avgState{} }
func (a *AvgAgg) Update(state State, value interface{}) {
	s := state.(*avgState)
	s.sum += toFloat64(value)
	s.count++
}
func (a *AvgAgg) Merge(sa, sb State) State {
	a2, b2 := sa.(*avgState), sb.(*avgState)

	return &avgState{sum: a2.sum + b2.sum, count: a2.count + b2.count}
}
func (a *AvgAgg) Finalize(state State) interface{} {
	s := state.(*avgState)
	if s.count == 0 {
		return 0.0
	}

	return s.sum / float64(s.count)
}

// Min

type minState struct {
	min float64
	set bool
}

type MinAgg struct{}

func (a *MinAgg) Name() string { return "min" }
func (a *MinAgg) Init() State  { return &minState{min: math.MaxFloat64} }
func (a *MinAgg) Update(state State, value interface{}) {
	s := state.(*minState)
	v := toFloat64(value)
	if !s.set || v < s.min {
		s.min = v
		s.set = true
	}
}
func (a *MinAgg) Merge(sa, sb State) State {
	a2, b2 := sa.(*minState), sb.(*minState)
	if !a2.set {
		return b2
	}
	if !b2.set {
		return a2
	}
	if a2.min < b2.min {
		return a2
	}

	return b2
}
func (a *MinAgg) Finalize(state State) interface{} {
	s := state.(*minState)
	if !s.set {
		return 0.0
	}

	return s.min
}

// Max

type maxState struct {
	max float64
	set bool
}

type MaxAgg struct{}

func (a *MaxAgg) Name() string { return "max" }
func (a *MaxAgg) Init() State  { return &maxState{max: -math.MaxFloat64} }
func (a *MaxAgg) Update(state State, value interface{}) {
	s := state.(*maxState)
	v := toFloat64(value)
	if !s.set || v > s.max {
		s.max = v
		s.set = true
	}
}
func (a *MaxAgg) Merge(sa, sb State) State {
	a2, b2 := sa.(*maxState), sb.(*maxState)
	if !a2.set {
		return b2
	}
	if !b2.set {
		return a2
	}
	if a2.max > b2.max {
		return a2
	}

	return b2
}
func (a *MaxAgg) Finalize(state State) interface{} {
	s := state.(*maxState)
	if !s.set {
		return 0.0
	}

	return s.max
}

// Distinct Count (dc)

type dcState struct {
	values map[string]struct{}
}

type DCCountAgg struct{}

func (a *DCCountAgg) Name() string { return "dc" }
func (a *DCCountAgg) Init() State  { return &dcState{values: make(map[string]struct{})} }
func (a *DCCountAgg) Update(state State, value interface{}) {
	state.(*dcState).values[fmt.Sprint(value)] = struct{}{}
}
func (a *DCCountAgg) Merge(sa, sb State) State {
	a2, b2 := sa.(*dcState), sb.(*dcState)
	merged := &dcState{values: make(map[string]struct{}, len(a2.values)+len(b2.values))}
	for k := range a2.values {
		merged.values[k] = struct{}{}
	}
	for k := range b2.values {
		merged.values[k] = struct{}{}
	}

	return merged
}
func (a *DCCountAgg) Finalize(state State) interface{} {
	return int64(len(state.(*dcState).values))
}

// Values

type valuesState struct {
	values []interface{}
}

type ValuesAgg struct{}

func (a *ValuesAgg) Name() string { return "values" }
func (a *ValuesAgg) Init() State  { return &valuesState{} }
func (a *ValuesAgg) Update(state State, value interface{}) {
	state.(*valuesState).values = append(state.(*valuesState).values, value)
}
func (a *ValuesAgg) Merge(sa, sb State) State {
	a2, b2 := sa.(*valuesState), sb.(*valuesState)

	return &valuesState{values: append(a2.values, b2.values...)}
}
func (a *ValuesAgg) Finalize(state State) interface{} {
	return state.(*valuesState).values
}

// First

type firstState struct {
	value interface{}
	set   bool
}

type FirstAgg struct{}

func (a *FirstAgg) Name() string { return "first" }
func (a *FirstAgg) Init() State  { return &firstState{} }
func (a *FirstAgg) Update(state State, value interface{}) {
	s := state.(*firstState)
	if !s.set {
		s.value = value
		s.set = true
	}
}
func (a *FirstAgg) Merge(sa, sb State) State {
	a2 := sa.(*firstState)
	if a2.set {
		return a2
	}

	return sb
}
func (a *FirstAgg) Finalize(state State) interface{} {
	return state.(*firstState).value
}

// Last

type lastState struct {
	value interface{}
}

type LastAgg struct{}

func (a *LastAgg) Name() string { return "last" }
func (a *LastAgg) Init() State  { return &lastState{} }
func (a *LastAgg) Update(state State, value interface{}) {
	state.(*lastState).value = value
}
func (a *LastAgg) Merge(sa, sb State) State {
	b2 := sb.(*lastState)
	if b2.value != nil {
		return b2
	}

	return sa
}
func (a *LastAgg) Finalize(state State) interface{} {
	return state.(*lastState).value
}

// Percentile (t-digest based for O(1) memory per stream)

type percentileState struct {
	centroids   []pctCentroid
	count       float64
	compression float64
	maxSize     int
}

type pctCentroid struct {
	mean  float64
	count float64
}

func newPercentileState() *percentileState {
	const compression = 100

	return &percentileState{
		compression: compression,
		maxSize:     int(compression) * 5,
	}
}

func (s *percentileState) add(value float64) {
	s.centroids = append(s.centroids, pctCentroid{mean: value, count: 1})
	s.count++
	if len(s.centroids) > s.maxSize {
		s.compress()
	}
}

func (s *percentileState) compress() {
	if len(s.centroids) <= 1 {
		return
	}
	sort.Slice(s.centroids, func(i, j int) bool {
		return s.centroids[i].mean < s.centroids[j].mean
	})
	// When the number of centroids fits within the compression budget,
	// keep them all for exact precision on small datasets.
	if len(s.centroids) <= int(s.compression) {
		return
	}
	merged := make([]pctCentroid, 0, len(s.centroids)/2+1)
	merged = append(merged, s.centroids[0])
	for i := 1; i < len(s.centroids); i++ {
		last := &merged[len(merged)-1]
		q := (last.count/2 + s.centroids[i].count/2) / s.count
		maxSz := 4 * s.compression * q * (1 - q)
		if maxSz < 1 {
			maxSz = 1
		}
		if last.count+s.centroids[i].count <= maxSz {
			totalCount := last.count + s.centroids[i].count
			last.mean = (last.mean*last.count + s.centroids[i].mean*s.centroids[i].count) / totalCount
			last.count = totalCount
		} else {
			merged = append(merged, s.centroids[i])
		}
	}
	s.centroids = merged
}

func (s *percentileState) quantile(q float64) float64 {
	if len(s.centroids) == 0 {
		return 0
	}
	s.compress()
	if q <= 0 {
		return s.centroids[0].mean
	}
	if q >= 1 {
		return s.centroids[len(s.centroids)-1].mean
	}
	target := q * s.count
	cumulative := 0.0
	for i, c := range s.centroids {
		cumulative += c.count
		if cumulative >= target {
			if i == 0 {
				return c.mean
			}
			prev := s.centroids[i-1]
			prevCum := cumulative - c.count
			frac := (target - prevCum) / c.count

			return prev.mean + frac*(c.mean-prev.mean)
		}
	}

	return s.centroids[len(s.centroids)-1].mean
}

type PercentileAgg struct {
	Pct float64 // 0-100
}

func (a *PercentileAgg) Name() string { return "percentile" }
func (a *PercentileAgg) Init() State  { return newPercentileState() }
func (a *PercentileAgg) Update(state State, value interface{}) {
	state.(*percentileState).add(toFloat64(value))
}
func (a *PercentileAgg) Merge(sa, sb State) State {
	a2, b2 := sa.(*percentileState), sb.(*percentileState)
	merged := newPercentileState()
	merged.centroids = append(merged.centroids, a2.centroids...)
	merged.centroids = append(merged.centroids, b2.centroids...)
	merged.count = a2.count + b2.count
	if len(merged.centroids) > merged.maxSize {
		merged.compress()
	}

	return merged
}
func (a *PercentileAgg) Finalize(state State) interface{} {
	return state.(*percentileState).quantile(a.Pct / 100.0)
}

// Stdev (Welford's online algorithm — single-pass, numerically stable)

type stdevState struct {
	count int64
	mean  float64
	m2    float64 // sum of squared differences from the current mean
}

// StdevAgg computes sample standard deviation using Welford's online algorithm.
type StdevAgg struct{}

func (a *StdevAgg) Name() string { return "stdev" }
func (a *StdevAgg) Init() State  { return &stdevState{} }
func (a *StdevAgg) Update(state State, value interface{}) {
	s := state.(*stdevState)
	v := toFloat64(value)
	s.count++
	delta := v - s.mean
	s.mean += delta / float64(s.count)
	delta2 := v - s.mean
	s.m2 += delta * delta2
}

func (a *StdevAgg) Merge(sa, sb State) State {
	a2, b2 := sa.(*stdevState), sb.(*stdevState)
	if a2.count == 0 {
		return b2
	}
	if b2.count == 0 {
		return a2
	}
	// Chan et al. parallel algorithm for combining Welford states.
	combined := &stdevState{}
	combined.count = a2.count + b2.count
	delta := b2.mean - a2.mean
	combined.mean = (a2.mean*float64(a2.count) + b2.mean*float64(b2.count)) / float64(combined.count)
	combined.m2 = a2.m2 + b2.m2 + delta*delta*float64(a2.count)*float64(b2.count)/float64(combined.count)

	return combined
}

func (a *StdevAgg) Finalize(state State) interface{} {
	s := state.(*stdevState)
	if s.count < 2 {
		return 0.0
	}

	return math.Sqrt(s.m2 / float64(s.count-1))
}

// toFloat64 converts a value to float64.
func toFloat64(v interface{}) float64 {
	switch val := v.(type) {
	case float64:
		return val
	case float32:
		return float64(val)
	case int64:
		return float64(val)
	case int:
		return float64(val)
	case int32:
		return float64(val)
	case string:
		f, _ := strconv.ParseFloat(val, 64)

		return f
	default:
		return 0
	}
}
