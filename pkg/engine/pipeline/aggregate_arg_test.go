package pipeline

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/event"
)

func TestAggregateArgFunctions(t *testing.T) {
	const groups = 100
	const perGroup = 50

	rows := make([]map[string]event.Value, 0, groups*perGroup)
	for score := 0; score < perGroup; score++ {
		for group := 0; group < groups; group++ {
			groupName := fmt.Sprintf("g%d", group)
			rows = append(rows, map[string]event.Value{
				"group":  event.StringValue(groupName),
				"value":  event.StringValue(fmt.Sprintf("%s-%02d", groupName, score)),
				"score":  event.IntValue(int64(score)),
				"weight": event.IntValue(int64(score%5 + 1)),
				"route":  event.StringValue(fmt.Sprintf("route-%d", score%3)),
				"team":   event.StringValue(fmt.Sprintf("team-%d", group)),
				"metrics": event.ObjectValue(map[string]event.Value{
					"requests": event.IntValue(1),
					"errors":   event.IntValue(int64(score % 2)),
					"ignored":  event.StringValue("non-numeric"),
				}),
			})
		}
	}

	child := NewRowScanIterator(rows, 128)
	aggs := []AggFunc{
		{Name: "arg_max", Field: "value", OrderField: "score", Alias: "max_value"},
		{Name: "arg_min", Field: "value", OrderField: "score", Alias: "min_value"},
		{Name: "any_value", Field: "team", Alias: "some_team"},
		{Name: "top_k", Field: "route", Limit: 2, Alias: "top_routes"},
		{Name: "top_k_weighted", Field: "route", WeightField: "weight", Limit: 2, Alias: "top_weighted_routes"},
		{Name: "value_counts", Field: "route", Alias: "route_counts"},
		{Name: "avg_weighted", Field: "score", WeightField: "weight", Alias: "weighted_score"},
		{Name: "entropy", Field: "route", Alias: "route_entropy"},
		{Name: "max_n", Field: "score", Limit: 3, Alias: "max_scores"},
		{Name: "min_n", Field: "score", Limit: 3, Alias: "min_scores"},
		{Name: "corr", Field: "score", WeightField: "weight", Alias: "score_weight_corr"},
		{Name: "covar", Field: "score", WeightField: "weight", Alias: "score_weight_covar"},
		{Name: "linear_fit", Field: "score", WeightField: "weight", Alias: "score_weight_fit"},
		{Name: "sum_object", Field: "metrics", Alias: "metric_totals"},
		{Name: "skewness", Field: "score", Alias: "score_skew"},
		{Name: "kurtosis", Field: "score", Alias: "score_kurt"},
		{Name: "perc", Field: "score", Quantile: 1, Alias: "score_p100"},
		{Name: "mad", Field: "score", Alias: "score_mad"},
		{Name: "perc_weighted", Field: "score", WeightField: "weight", Quantile: 0.5, Alias: "weighted_p50"},
	}
	iter := NewAggregateIterator(child, aggs, []string{"group"}, nil)

	ctx := context.Background()
	if err := iter.Init(ctx); err != nil {
		t.Fatal(err)
	}
	result, err := CollectAll(ctx, iter)
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != groups {
		t.Fatalf("expected %d groups, got %d", groups, len(result))
	}

	byGroup := make(map[string]map[string]event.Value, len(result))
	for _, row := range result {
		byGroup[row["group"].AsString()] = row
	}

	for _, group := range []string{"g0", "g42", "g99"} {
		row := byGroup[group]
		if row == nil {
			t.Fatalf("missing group %s", group)
		}
		assertEventString(t, row["max_value"], fmt.Sprintf("%s-49", group), group+" max")
		assertEventString(t, row["min_value"], fmt.Sprintf("%s-00", group), group+" min")
		if row["some_team"].IsNull() {
			t.Fatalf("%s any_value should be non-null", group)
		}
		assertEventTopKEntry(t, row["top_routes"], 0, "route-0", 17, group+" top 0")
		assertEventTopKEntry(t, row["top_routes"], 1, "route-1", 17, group+" top 1")
		assertEventWeightedTopKEntry(t, row["top_weighted_routes"], 0, "route-1", 52, group+" weighted top 0")
		assertEventWeightedTopKEntry(t, row["top_weighted_routes"], 1, "route-0", 50, group+" weighted top 1")
		assertEventTopKEntry(t, row["route_counts"], 0, "route-0", 17, group+" counts 0")
		assertEventTopKEntry(t, row["route_counts"], 1, "route-1", 17, group+" counts 1")
		assertEventTopKEntry(t, row["route_counts"], 2, "route-2", 16, group+" counts 2")
		assertEventFloat(t, row["weighted_score"], weightedScoreWant(), group+" weighted")
		assertEventFloat(t, row["route_entropy"], routeEntropyWant(), group+" entropy")
		assertEventIntArray(t, row["max_scores"], []int64{49, 48, 47}, group+" max_n")
		assertEventIntArray(t, row["min_scores"], []int64{0, 1, 2}, group+" min_n")
		assertEventFloat(t, row["score_weight_corr"], scoreWeightCorrWant(), group+" corr")
		assertEventFloat(t, row["score_weight_covar"], scoreWeightCovarWant(), group+" covar")
		assertEventObjectFloat(t, row["score_weight_fit"], "slope", scoreWeightSlopeWant(), group+" fit slope")
		assertEventObjectFloat(t, row["score_weight_fit"], "intercept", scoreWeightInterceptWant(), group+" fit intercept")
		assertEventObjectFloat(t, row["score_weight_fit"], "r2", scoreWeightR2Want(), group+" fit r2")
		assertEventObjectFloat(t, row["metric_totals"], "requests", 50, group+" object requests")
		assertEventObjectFloat(t, row["metric_totals"], "errors", 25, group+" object errors")
		assertEventFloat(t, row["score_skew"], scoreSkewWant(), group+" skew")
		assertEventFloat(t, row["score_kurt"], scoreKurtWant(), group+" kurt")
		assertEventFloat(t, row["score_p100"], 49, group+" p100")
		assertEventFloat(t, row["score_mad"], 12.5, group+" mad")
		assertEventFloat(t, row["weighted_p50"], weightedP50Want(), group+" weighted p50")
	}
}

func weightedScoreWant() float64 {
	var sum, weightSum float64
	for score := 0; score < 50; score++ {
		weight := float64(score%5 + 1)
		sum += float64(score) * weight
		weightSum += weight
	}
	return sum / weightSum
}

func weightedP50Want() float64 {
	td := NewTDigest(defaultTDigestCompression)
	for score := 0; score < 50; score++ {
		td.AddWeighted(float64(score), float64(score%5+1))
	}

	return td.Quantile(0.5)
}

func routeEntropyWant() float64 {
	counts := []float64{17, 17, 16}
	var entropy float64
	for _, count := range counts {
		p := count / 50
		entropy -= p * math.Log2(p)
	}
	return entropy
}

func scoreWeightCorrWant() float64 {
	stats := scoreWeightStats()
	n := float64(stats.count)
	num := n*stats.sumXY - stats.sumX*stats.sumY
	xDen := n*stats.sumX2 - stats.sumX*stats.sumX
	yDen := n*stats.sumY2 - stats.sumY*stats.sumY
	return num / math.Sqrt(xDen*yDen)
}

func scoreWeightCovarWant() float64 {
	stats := scoreWeightStats()
	n := float64(stats.count)
	return (stats.sumXY - stats.sumX*stats.sumY/n) / (n - 1)
}

func scoreWeightSlopeWant() float64 {
	stats := scoreWeightStats()
	n := float64(stats.count)
	return (n*stats.sumXY - stats.sumX*stats.sumY) / (n*stats.sumX2 - stats.sumX*stats.sumX)
}

func scoreWeightInterceptWant() float64 {
	stats := scoreWeightStats()
	return (stats.sumY - scoreWeightSlopeWant()*stats.sumX) / float64(stats.count)
}

func scoreWeightR2Want() float64 {
	corr := scoreWeightCorrWant()
	return corr * corr
}

type scoreWeightSummary struct {
	count int
	sumX  float64
	sumY  float64
	sumX2 float64
	sumY2 float64
	sumXY float64
}

func scoreWeightStats() scoreWeightSummary {
	var stats scoreWeightSummary
	for score := 0; score < 50; score++ {
		x := float64(score)
		y := float64(score%5 + 1)
		stats.count++
		stats.sumX += x
		stats.sumY += y
		stats.sumX2 += x * x
		stats.sumY2 += y * y
		stats.sumXY += x * y
	}
	return stats
}

func scoreSkewWant() float64 {
	_, _, _, skew, _ := scoreMoments()
	return skew
}

func scoreKurtWant() float64 {
	_, _, _, _, kurt := scoreMoments()
	return kurt
}

func scoreMoments() (float64, float64, float64, float64, float64) {
	const count = 50
	var sum float64
	for score := 0; score < count; score++ {
		sum += float64(score)
	}
	mean := sum / count
	var m2, m3, m4 float64
	for score := 0; score < count; score++ {
		diff := float64(score) - mean
		diff2 := diff * diff
		m2 += diff2
		m3 += diff2 * diff
		m4 += diff2 * diff2
	}
	n := float64(count)
	m2 /= n
	m3 /= n
	m4 /= n
	g1 := m3 / math.Pow(m2, 1.5)
	skew := math.Sqrt(n*(n-1)) / (n - 2) * g1
	g2 := m4/(m2*m2) - 3
	kurt := (n - 1) / ((n - 2) * (n - 3)) * ((n+1)*g2 + 6)
	return m2, m3, m4, skew, kurt
}

func assertEventString(t *testing.T, v event.Value, expected string, label string) {
	t.Helper()
	if v.Type() != event.FieldTypeString {
		t.Fatalf("%s: expected string, got %s", label, v.Type())
	}
	if v.AsString() != expected {
		t.Fatalf("%s: got %q, want %q", label, v.AsString(), expected)
	}
}

func assertEventTopKEntry(t *testing.T, v event.Value, idx int, expected string, count int64, label string) {
	t.Helper()
	if v.Type() != event.FieldTypeArray {
		t.Fatalf("%s: expected array, got %s", label, v.Type())
	}
	arr := v.AsArray()
	if len(arr) <= idx {
		t.Fatalf("%s: expected entry %d, got len %d", label, idx, len(arr))
	}
	obj, ok := arr[idx].TryAsObject()
	if !ok {
		t.Fatalf("%s: expected object, got %s", label, arr[idx].Type())
	}
	assertEventString(t, obj["value"], expected, label+" value")
	got, ok := obj["count"].TryAsInt()
	if !ok || got != count {
		t.Fatalf("%s count: got %v, want %d", label, obj["count"], count)
	}
}

func assertEventWeightedTopKEntry(t *testing.T, v event.Value, idx int, expected string, weight float64, label string) {
	t.Helper()
	if v.Type() != event.FieldTypeArray {
		t.Fatalf("%s: expected array, got %s", label, v.Type())
	}
	arr := v.AsArray()
	if len(arr) <= idx {
		t.Fatalf("%s: expected entry %d, got len %d", label, idx, len(arr))
	}
	obj, ok := arr[idx].TryAsObject()
	if !ok {
		t.Fatalf("%s: expected object, got %s", label, arr[idx].Type())
	}
	assertEventString(t, obj["value"], expected, label+" value")
	got, ok := obj["weight"].TryAsFloat()
	if !ok || math.Abs(got-weight) > 1e-9 {
		t.Fatalf("%s weight: got %v, want %f", label, obj["weight"], weight)
	}
}

func assertEventFloat(t *testing.T, v event.Value, expected float64, label string) {
	t.Helper()
	got, ok := v.TryAsFloat()
	if !ok {
		t.Fatalf("%s: expected float, got %s", label, v.Type())
	}
	if math.Abs(got-expected) > 1e-9 {
		t.Fatalf("%s: got %f, want %f", label, got, expected)
	}
}

func assertEventObjectFloat(t *testing.T, v event.Value, key string, expected float64, label string) {
	t.Helper()
	if v.Type() != event.FieldTypeObject {
		t.Fatalf("%s: expected object, got %s", label, v.Type())
	}
	obj, ok := v.TryAsObject()
	if !ok {
		t.Fatalf("%s: expected object, got %s", label, v.Type())
	}
	assertEventFloat(t, obj[key], expected, label+" "+key)
}

func assertEventIntArray(t *testing.T, v event.Value, expected []int64, label string) {
	t.Helper()
	if v.Type() != event.FieldTypeArray {
		t.Fatalf("%s: expected array, got %s", label, v.Type())
	}
	arr := v.AsArray()
	if len(arr) != len(expected) {
		t.Fatalf("%s: got len %d, want %d", label, len(arr), len(expected))
	}
	for i, want := range expected {
		got, ok := arr[i].TryAsInt()
		if !ok || got != want {
			t.Fatalf("%s[%d]: got %v, want %d", label, i, arr[i], want)
		}
	}
}
