package rest

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	dto "github.com/prometheus/client_model/go"

	"github.com/OrlovEvgeny/Lynxdb/pkg/server"
)

func TestPrometheusMetrics_RecordQuery(t *testing.T) {
	pm := NewPrometheusMetrics()

	ss := &server.SearchStats{
		ElapsedMS:           350,
		ScanMS:              280,
		PipelineMS:          70,
		RowsScanned:         50_000,
		PeakMemoryBytes:     8 * 1024 * 1024,
		SegmentsSkippedBF:   2,
		SegmentsSkippedStat: 1,
		SegmentsSkippedTime: 3,
		SegmentsScanned:     5,
	}

	pm.RecordQuery(ss)

	// Gather and verify metrics are present.
	families := gatherMetrics(t, pm)

	assertHistogramCount(t, families, "lynxdb_query_duration_seconds", 1)
	assertHistogramCount(t, families, "lynxdb_query_scan_duration_seconds", 1)
	assertHistogramCount(t, families, "lynxdb_query_pipeline_duration_seconds", 1)
	assertHistogramCount(t, families, "lynxdb_query_peak_memory_bytes", 1)
	assertHistogramCount(t, families, "lynxdb_query_rows_scanned", 1)

	assertCounterValue(t, families, "lynxdb_segments_skipped_bloom_total", 2)
	assertCounterValue(t, families, "lynxdb_segments_skipped_column_stats_total", 1)
	assertCounterValue(t, families, "lynxdb_segments_skipped_time_total", 3)
	assertCounterValue(t, families, "lynxdb_segments_scanned_total", 5)
}

func TestPrometheusMetrics_MultipleQueries(t *testing.T) {
	pm := NewPrometheusMetrics()

	for i := 0; i < 5; i++ {
		pm.RecordQuery(&server.SearchStats{
			ElapsedMS:         float64(100 * (i + 1)),
			RowsScanned:       int64(1000 * (i + 1)),
			SegmentsSkippedBF: 1,
			SegmentsScanned:   2,
		})
	}

	families := gatherMetrics(t, pm)

	assertHistogramCount(t, families, "lynxdb_query_duration_seconds", 5)
	assertCounterValue(t, families, "lynxdb_segments_skipped_bloom_total", 5)
	assertCounterValue(t, families, "lynxdb_segments_scanned_total", 10)
}

func TestPrometheusMetrics_NilStats(t *testing.T) {
	pm := NewPrometheusMetrics()

	// Should not panic.
	pm.RecordQuery(nil)

	families := gatherMetrics(t, pm)
	assertHistogramCount(t, families, "lynxdb_query_duration_seconds", 0)
}

func TestPrometheusMetrics_Handler(t *testing.T) {
	pm := NewPrometheusMetrics()
	pm.RecordQuery(&server.SearchStats{ElapsedMS: 42, RowsScanned: 100, SegmentsScanned: 1})

	// Serve the /metrics endpoint.
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/metrics", http.NoBody)
	pm.Handler().ServeHTTP(w, r)

	resp := w.Result()
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	body, _ := io.ReadAll(resp.Body)
	bodyStr := string(body)

	// Verify key metric names appear in the exposition format.
	// Note: lynxdb_query_errors_total is a CounterVec — Prometheus does not
	// emit it until at least one label combination has been observed.
	for _, name := range []string{
		"lynxdb_query_duration_seconds",
		"lynxdb_query_scan_duration_seconds",
		"lynxdb_query_rows_scanned",
		"lynxdb_segments_scanned_total",
		"lynxdb_segments_skipped_bloom_total",
		"lynxdb_segments_skipped_column_stats_total",
		"lynxdb_segments_skipped_time_total",
		"lynxdb_query_slow_total",
	} {
		if !strings.Contains(bodyStr, name) {
			t.Errorf("metric %q not found in /metrics output", name)
		}
	}
}

func TestPrometheusMetrics_ZeroSkipsNotRecorded(t *testing.T) {
	pm := NewPrometheusMetrics()

	// Query with zero segment skips — counters should remain at 0.
	pm.RecordQuery(&server.SearchStats{
		ElapsedMS:           10,
		RowsScanned:         100,
		SegmentsSkippedBF:   0,
		SegmentsSkippedStat: 0,
		SegmentsSkippedTime: 0,
		SegmentsScanned:     0,
	})

	families := gatherMetrics(t, pm)

	assertCounterValue(t, families, "lynxdb_segments_skipped_bloom_total", 0)
	assertCounterValue(t, families, "lynxdb_segments_skipped_column_stats_total", 0)
	assertCounterValue(t, families, "lynxdb_segments_scanned_total", 0)
}

func TestPrometheusMetrics_SlowQueryCounter(t *testing.T) {
	pm := NewPrometheusMetrics()

	// Non-slow query: SlowQuery=false — counter should stay at 0.
	pm.RecordQuery(&server.SearchStats{
		ElapsedMS:       500,
		RowsScanned:     1000,
		ResultTypeLabel: "events",
		SlowQuery:       false,
	})

	families := gatherMetrics(t, pm)
	assertCounterValue(t, families, "lynxdb_query_slow_total", 0)

	// Slow query: SlowQuery=true — counter should increment.
	pm.RecordQuery(&server.SearchStats{
		ElapsedMS:       2000,
		RowsScanned:     50_000,
		ResultTypeLabel: "events",
		SlowQuery:       true,
	})

	families = gatherMetrics(t, pm)
	assertCounterValue(t, families, "lynxdb_query_slow_total", 1)

	// Another slow query — counter should be 2.
	pm.RecordQuery(&server.SearchStats{
		ElapsedMS:       5000,
		RowsScanned:     100_000,
		ResultTypeLabel: "aggregate",
		SlowQuery:       true,
	})

	families = gatherMetrics(t, pm)
	assertCounterValue(t, families, "lynxdb_query_slow_total", 2)
}

func TestPrometheusMetrics_ErrorCounter(t *testing.T) {
	pm := NewPrometheusMetrics()

	// Successful query — no error counter increment.
	pm.RecordQuery(&server.SearchStats{
		ElapsedMS:       100,
		ResultTypeLabel: "events",
	})

	families := gatherMetrics(t, pm)
	assertCounterValue(t, families, "lynxdb_query_errors_total", 0)

	// Timeout error.
	pm.RecordQuery(&server.SearchStats{
		ElapsedMS:       60_000,
		ResultTypeLabel: "events",
		ErrorType:       "timeout",
	})

	families = gatherMetrics(t, pm)
	assertCounterVecValue(t, families, "lynxdb_query_errors_total", "timeout", 1)

	// Memory error.
	pm.RecordQuery(&server.SearchStats{
		ElapsedMS:       500,
		ResultTypeLabel: "aggregate",
		ErrorType:       "memory",
	})

	families = gatherMetrics(t, pm)
	assertCounterVecValue(t, families, "lynxdb_query_errors_total", "timeout", 1)
	assertCounterVecValue(t, families, "lynxdb_query_errors_total", "memory", 1)

	// Execution error.
	pm.RecordQuery(&server.SearchStats{
		ElapsedMS:       200,
		ResultTypeLabel: "events",
		ErrorType:       "execution",
	})

	families = gatherMetrics(t, pm)
	assertCounterVecValue(t, families, "lynxdb_query_errors_total", "execution", 1)
	// Total across all labels should be 3.
	assertCounterValue(t, families, "lynxdb_query_errors_total", 3)
}

func TestPrometheusMetrics_ResultTypeLabel(t *testing.T) {
	pm := NewPrometheusMetrics()

	pm.RecordQuery(&server.SearchStats{
		ElapsedMS:       100,
		ResultTypeLabel: "events",
	})
	pm.RecordQuery(&server.SearchStats{
		ElapsedMS:       200,
		ResultTypeLabel: "aggregate",
	})
	pm.RecordQuery(&server.SearchStats{
		ElapsedMS:       300,
		ResultTypeLabel: "timechart",
	})

	families := gatherMetrics(t, pm)
	mf, ok := families["lynxdb_query_duration_seconds"]
	if !ok {
		t.Fatal("lynxdb_query_duration_seconds not found")
	}

	// We should have 3 label combinations: events, aggregate, timechart.
	labels := make(map[string]uint64)
	for _, m := range mf.GetMetric() {
		for _, lp := range m.GetLabel() {
			if lp.GetName() == "result_type" {
				if h := m.GetHistogram(); h != nil {
					labels[lp.GetValue()] += h.GetSampleCount()
				}
			}
		}
	}

	for _, rt := range []string{"events", "aggregate", "timechart"} {
		if labels[rt] != 1 {
			t.Errorf("result_type=%q: expected count 1, got %d", rt, labels[rt])
		}
	}
}

func TestPrometheusMetrics_ResultTypeFallback(t *testing.T) {
	pm := NewPrometheusMetrics()

	// Empty ResultTypeLabel should fall back to "query".
	pm.RecordQuery(&server.SearchStats{
		ElapsedMS: 42,
	})

	families := gatherMetrics(t, pm)
	mf, ok := families["lynxdb_query_duration_seconds"]
	if !ok {
		t.Fatal("lynxdb_query_duration_seconds not found")
	}

	for _, m := range mf.GetMetric() {
		for _, lp := range m.GetLabel() {
			if lp.GetName() == "result_type" && lp.GetValue() != "query" {
				t.Errorf("expected fallback label 'query', got %q", lp.GetValue())
			}
		}
	}
}

func TestPrometheusMetrics_ErrorsAppearInHandler(t *testing.T) {
	pm := NewPrometheusMetrics()

	// Record an error so the CounterVec emits a label.
	pm.RecordQuery(&server.SearchStats{
		ElapsedMS: 100,
		ErrorType: "timeout",
	})

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/metrics", http.NoBody)
	pm.Handler().ServeHTTP(w, r)

	resp := w.Result()
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	bodyStr := string(body)

	if !strings.Contains(bodyStr, "lynxdb_query_errors_total") {
		t.Error("lynxdb_query_errors_total not found in /metrics output after recording error")
	}
	if !strings.Contains(bodyStr, `type="timeout"`) {
		t.Error(`type="timeout" label not found in /metrics output`)
	}
}

// Helpers

// gatherMetrics uses the registry's Gather() method to collect all registered
// metric families without going through text parsing (avoids expfmt validation
// scheme issues).
func gatherMetrics(t *testing.T, pm *PrometheusMetrics) map[string]*dto.MetricFamily {
	t.Helper()

	gathered, err := pm.Registry().Gather()
	if err != nil {
		t.Fatalf("failed to gather metrics: %v", err)
	}

	families := make(map[string]*dto.MetricFamily, len(gathered))
	for _, mf := range gathered {
		families[mf.GetName()] = mf
	}

	return families
}

func assertHistogramCount(t *testing.T, families map[string]*dto.MetricFamily, name string, expectedCount uint64) {
	t.Helper()

	mf, ok := families[name]
	if !ok {
		if expectedCount == 0 {
			return // metric not present yet — expected for 0 observations
		}
		t.Errorf("metric %q not found", name)

		return
	}

	var total uint64
	for _, m := range mf.GetMetric() {
		if h := m.GetHistogram(); h != nil {
			total += h.GetSampleCount()
		}
	}

	if total != expectedCount {
		t.Errorf("metric %q: expected count %d, got %d", name, expectedCount, total)
	}
}

func assertCounterValue(t *testing.T, families map[string]*dto.MetricFamily, name string, expectedValue float64) {
	t.Helper()

	mf, ok := families[name]
	if !ok {
		if expectedValue == 0 {
			return
		}
		t.Errorf("metric %q not found", name)

		return
	}

	var total float64
	for _, m := range mf.GetMetric() {
		if c := m.GetCounter(); c != nil {
			total += c.GetValue()
		}
	}

	if total != expectedValue {
		t.Errorf("metric %q: expected value %f, got %f", name, expectedValue, total)
	}
}

// assertCounterVecValue checks a single label combination within a CounterVec
// metric family. It finds the metric whose "type" label matches labelValue and
// asserts that its counter value equals expectedValue.
func assertCounterVecValue(t *testing.T, families map[string]*dto.MetricFamily, name, labelValue string, expectedValue float64) {
	t.Helper()

	mf, ok := families[name]
	if !ok {
		if expectedValue == 0 {
			return // metric not present yet — expected for 0 observations
		}
		t.Errorf("metric %q not found", name)

		return
	}

	for _, m := range mf.GetMetric() {
		for _, lp := range m.GetLabel() {
			if lp.GetName() == "type" && lp.GetValue() == labelValue {
				got := m.GetCounter().GetValue()
				if got != expectedValue {
					t.Errorf("metric %q{type=%q}: expected %f, got %f", name, labelValue, expectedValue, got)
				}

				return
			}
		}
	}

	if expectedValue == 0 {
		return // label not found and expected 0 — OK
	}
	t.Errorf("metric %q: label type=%q not found", name, labelValue)
}
