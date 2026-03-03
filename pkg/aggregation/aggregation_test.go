package aggregation

import (
	"math"
	"testing"
)

func TestCountAgg(t *testing.T) {
	agg := &CountAgg{}
	s := agg.Init()
	for i := 0; i < 100; i++ {
		agg.Update(s, i)
	}
	result := agg.Finalize(s).(int64)
	if result != 100 {
		t.Errorf("count: got %d, want 100", result)
	}
}

func TestCountAgg_Merge(t *testing.T) {
	agg := &CountAgg{}
	s1 := agg.Init()
	s2 := agg.Init()
	for i := 0; i < 50; i++ {
		agg.Update(s1, i)
	}
	for i := 0; i < 30; i++ {
		agg.Update(s2, i)
	}
	merged := agg.Merge(s1, s2)
	result := agg.Finalize(merged).(int64)
	if result != 80 {
		t.Errorf("count merge: got %d, want 80", result)
	}
}

func TestSumAgg(t *testing.T) {
	agg := &SumAgg{}
	s := agg.Init()
	agg.Update(s, 10.0)
	agg.Update(s, 20.0)
	agg.Update(s, 30.0)
	result := agg.Finalize(s).(float64)
	if result != 60.0 {
		t.Errorf("sum: got %f, want 60.0", result)
	}
}

func TestAvgAgg(t *testing.T) {
	agg := &AvgAgg{}
	s := agg.Init()
	agg.Update(s, 10.0)
	agg.Update(s, 20.0)
	agg.Update(s, 30.0)
	result := agg.Finalize(s).(float64)
	if result != 20.0 {
		t.Errorf("avg: got %f, want 20.0", result)
	}
}

func TestAvgAgg_Empty(t *testing.T) {
	agg := &AvgAgg{}
	s := agg.Init()
	result := agg.Finalize(s).(float64)
	if result != 0.0 {
		t.Errorf("avg empty: got %f, want 0.0", result)
	}
}

func TestMinAgg(t *testing.T) {
	agg := &MinAgg{}
	s := agg.Init()
	agg.Update(s, 30.0)
	agg.Update(s, 10.0)
	agg.Update(s, 20.0)
	result := agg.Finalize(s).(float64)
	if result != 10.0 {
		t.Errorf("min: got %f, want 10.0", result)
	}
}

func TestMaxAgg(t *testing.T) {
	agg := &MaxAgg{}
	s := agg.Init()
	agg.Update(s, 10.0)
	agg.Update(s, 30.0)
	agg.Update(s, 20.0)
	result := agg.Finalize(s).(float64)
	if result != 30.0 {
		t.Errorf("max: got %f, want 30.0", result)
	}
}

func TestDCCountAgg(t *testing.T) {
	agg := &DCCountAgg{}
	s := agg.Init()
	agg.Update(s, "a")
	agg.Update(s, "b")
	agg.Update(s, "a")
	agg.Update(s, "c")
	agg.Update(s, "b")
	result := agg.Finalize(s).(int64)
	if result != 3 {
		t.Errorf("dc: got %d, want 3", result)
	}
}

func TestValuesAgg(t *testing.T) {
	agg := &ValuesAgg{}
	s := agg.Init()
	agg.Update(s, "a")
	agg.Update(s, "b")
	agg.Update(s, "c")
	result := agg.Finalize(s).([]interface{})
	if len(result) != 3 {
		t.Errorf("values: got %d items, want 3", len(result))
	}
}

func TestFirstAgg(t *testing.T) {
	agg := &FirstAgg{}
	s := agg.Init()
	agg.Update(s, "first")
	agg.Update(s, "second")
	agg.Update(s, "third")
	result := agg.Finalize(s).(string)
	if result != "first" {
		t.Errorf("first: got %q, want first", result)
	}
}

func TestLastAgg(t *testing.T) {
	agg := &LastAgg{}
	s := agg.Init()
	agg.Update(s, "first")
	agg.Update(s, "second")
	agg.Update(s, "third")
	result := agg.Finalize(s).(string)
	if result != "third" {
		t.Errorf("last: got %q, want third", result)
	}
}

func TestPercentileAgg(t *testing.T) {
	agg := &PercentileAgg{Pct: 50}
	s := agg.Init()
	for i := 1; i <= 100; i++ {
		agg.Update(s, float64(i))
	}
	result := agg.Finalize(s).(float64)
	// t-digest is approximate; allow ±2 tolerance for 100-element streams.
	if result < 48.0 || result > 52.0 {
		t.Errorf("p50: got %f, want ~50.0", result)
	}

	agg95 := &PercentileAgg{Pct: 95}
	s95 := agg95.Init()
	for i := 1; i <= 100; i++ {
		agg95.Update(s95, float64(i))
	}
	result95 := agg95.Finalize(s95).(float64)
	if result95 < 93.0 || result95 > 97.0 {
		t.Errorf("p95: got %f, want ~95.0", result95)
	}
}

func TestToFloat64(t *testing.T) {
	tests := []struct {
		input    interface{}
		expected float64
	}{
		{42.0, 42.0},
		{int64(10), 10.0},
		{int(5), 5.0},
		{"3.14", 3.14},
		{"notanumber", 0.0},
		{nil, 0.0},
	}
	for _, tt := range tests {
		result := toFloat64(tt.input)
		if math.Abs(result-tt.expected) > 1e-10 {
			t.Errorf("toFloat64(%v): got %f, want %f", tt.input, result, tt.expected)
		}
	}
}

func TestStdevAgg(t *testing.T) {
	agg := &StdevAgg{}
	s := agg.Init()
	// sample stdev of [2, 4, 4, 4, 5, 5, 7, 9] ≈ 2.13809
	for _, v := range []float64{2, 4, 4, 4, 5, 5, 7, 9} {
		agg.Update(s, v)
	}
	result := agg.Finalize(s).(float64)
	expected := math.Sqrt(32.0 / 7.0) // sample variance = 32/7
	if math.Abs(result-expected) > 1e-10 {
		t.Errorf("stdev: got %f, want %f", result, expected)
	}
}

func TestStdevAgg_SingleValue(t *testing.T) {
	agg := &StdevAgg{}
	s := agg.Init()
	agg.Update(s, 42.0)
	result := agg.Finalize(s).(float64)
	if result != 0.0 {
		t.Errorf("stdev single: got %f, want 0.0", result)
	}
}

func TestStdevAgg_Empty(t *testing.T) {
	agg := &StdevAgg{}
	s := agg.Init()
	result := agg.Finalize(s).(float64)
	if result != 0.0 {
		t.Errorf("stdev empty: got %f, want 0.0", result)
	}
}

func TestStdevAgg_Merge(t *testing.T) {
	agg := &StdevAgg{}
	s1 := agg.Init()
	s2 := agg.Init()
	data := []float64{2, 4, 4, 4, 5, 5, 7, 9}
	for _, v := range data[:4] {
		agg.Update(s1, v)
	}
	for _, v := range data[4:] {
		agg.Update(s2, v)
	}
	merged := agg.Merge(s1, s2)
	result := agg.Finalize(merged).(float64)
	expected := math.Sqrt(32.0 / 7.0)
	if math.Abs(result-expected) > 1e-10 {
		t.Errorf("stdev merge: got %f, want %f", result, expected)
	}
}

func TestPercentileAliases(t *testing.T) {
	tests := []struct {
		name string
		pct  float64
	}{
		{"perc50", 50},
		{"perc75", 75},
		{"perc90", 90},
		{"perc95", 95},
		{"perc99", 99},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			agg, err := NewAggregator(tt.name)
			if err != nil {
				t.Fatalf("NewAggregator(%q): %v", tt.name, err)
			}
			pa, ok := agg.(*PercentileAgg)
			if !ok {
				t.Fatalf("expected *PercentileAgg, got %T", agg)
			}
			if pa.Pct != tt.pct {
				t.Errorf("pct: got %f, want %f", pa.Pct, tt.pct)
			}
		})
	}
}

func TestEarliestLatestAliases(t *testing.T) {
	// earliest = first
	agg, err := NewAggregator("earliest")
	if err != nil {
		t.Fatalf("NewAggregator(earliest): %v", err)
	}
	s := agg.Init()
	agg.Update(s, "a")
	agg.Update(s, "b")
	if agg.Finalize(s).(string) != "a" {
		t.Error("earliest: expected first value")
	}

	// latest = last
	agg2, err := NewAggregator("latest")
	if err != nil {
		t.Fatalf("NewAggregator(latest): %v", err)
	}
	s2 := agg2.Init()
	agg2.Update(s2, "a")
	agg2.Update(s2, "b")
	if agg2.Finalize(s2).(string) != "b" {
		t.Error("latest: expected last value")
	}
}

func TestNewAggregator(t *testing.T) {
	names := []string{"count", "sum", "avg", "min", "max", "dc", "values", "first", "last",
		"percentile", "stdev", "perc50", "perc75", "perc90", "perc95", "perc99", "earliest", "latest"}
	for _, name := range names {
		agg, err := NewAggregator(name)
		if err != nil {
			t.Errorf("NewAggregator(%q): %v", name, err)
		}
		if agg == nil {
			t.Errorf("NewAggregator(%q): returned nil", name)
		}
	}

	_, err := NewAggregator("nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent aggregator")
	}
}
