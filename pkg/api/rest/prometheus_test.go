package rest

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
)

func TestPrometheusMetrics_RecordQuery(t *testing.T) {
	pm := NewPrometheusMetrics()

	pm.RecordQuery("events", 350*time.Millisecond, "")

	families := gatherMetrics(t, pm)

	assertHistogramCount(t, families, "lynxdb_query_duration_seconds", 1)
	assertCounterValue(t, families, "lynxdb_query_errors_total", 0)

	var sum float64
	for _, m := range families["lynxdb_query_duration_seconds"].GetMetric() {
		sum += m.GetHistogram().GetSampleSum()
	}
	if sum != 0.35 {
		t.Errorf("duration sum = %v, want 0.35", sum)
	}
}

func TestPrometheusMetrics_MultipleQueries(t *testing.T) {
	pm := NewPrometheusMetrics()

	for i := 0; i < 5; i++ {
		pm.RecordQuery("events", time.Duration(100*(i+1))*time.Millisecond, "")
	}

	families := gatherMetrics(t, pm)

	assertHistogramCount(t, families, "lynxdb_query_duration_seconds", 5)
}

func TestPrometheusMetrics_Handler(t *testing.T) {
	pm := NewPrometheusMetrics()
	pm.RecordQuery("events", 42*time.Millisecond, "")
	pm.RecordIngest(3, 120)

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

	// lynxdb_query_errors_total is a CounterVec — Prometheus does not
	// emit it until at least one label combination has been observed.
	for _, name := range []string{
		"lynxdb_query_duration_seconds",
		"lynxdb_ingest_events_total",
		"lynxdb_ingest_batches_total",
		"lynxdb_ingest_bytes_total",
		"lynxdb_ingest_errors_total",
		"lynxdb_ingest_es_bulk_duration_seconds",
		"lynxdb_ingest_otlp_request_bytes",
	} {
		if !strings.Contains(bodyStr, name) {
			t.Errorf("metric %q not found in /metrics output", name)
		}
	}
}

func TestPrometheusMetrics_RecordIngest(t *testing.T) {
	pm := NewPrometheusMetrics()

	pm.RecordIngest(3, 120)
	pm.RecordIngest(2, 80)
	pm.RecordIngestError()

	families := gatherMetrics(t, pm)
	assertCounterValue(t, families, "lynxdb_ingest_events_total", 5)
	assertCounterValue(t, families, "lynxdb_ingest_batches_total", 2)
	assertCounterValue(t, families, "lynxdb_ingest_bytes_total", 200)
	assertCounterValue(t, families, "lynxdb_ingest_errors_total", 1)
}

func TestPrometheusMetrics_DecompressionRejectCounter(t *testing.T) {
	pm := NewPrometheusMetrics()

	pm.OnReject("decompressed", "gzip")

	families := gatherMetrics(t, pm)
	assertCounterVecLabelValue(t, families, "lynxdb_ingest_decompression_rejected_total", "stage", "decompressed", 1)
	assertCounterVecLabelValue(t, families, "lynxdb_ingest_decompression_rejected_total", "encoding", "gzip", 1)
}

func TestPrometheusMetrics_ESHandshakeCounter(t *testing.T) {
	pm := NewPrometheusMetrics()

	pm.RecordESHandshake("license")
	pm.RecordESHandshake("license")
	pm.RecordESHandshake("template")

	families := gatherMetrics(t, pm)
	assertCounterVecLabelValue(t, families, "lynxdb_ingest_es_handshake_total", "kind", "license", 2)
	assertCounterVecLabelValue(t, families, "lynxdb_ingest_es_handshake_total", "kind", "template", 1)
}

func TestPrometheusMetrics_IngestCompatibilityMetrics(t *testing.T) {
	pm := NewPrometheusMetrics()

	pm.RecordESBulkRequest("success", 0.02)
	pm.RecordESBulkItem("index", "ok")
	pm.RecordOTLPRequest("logs", "json", "success", 2048)
	pm.RecordOTLPRecords("logs", "accepted", 3)
	pm.RecordDroppedRecords("otlp", "empty_body", 1)
	pm.SetListenerUp("es", true)

	families := gatherMetrics(t, pm)
	assertCounterVecLabelValue(t, families, "lynxdb_ingest_es_bulk_requests_total", "result", "success", 1)
	assertCounterVecLabelValue(t, families, "lynxdb_ingest_es_bulk_items_total", "action", "index", 1)
	assertHistogramCount(t, families, "lynxdb_ingest_es_bulk_duration_seconds", 1)
	assertCounterVecLabelValue(t, families, "lynxdb_ingest_otlp_requests_total", "signal", "logs", 1)
	assertCounterVecLabelValue(t, families, "lynxdb_ingest_otlp_records_total", "result", "accepted", 3)
	assertCounterVecLabelValue(t, families, "lynxdb_ingest_dropped_records_total", "source", "otlp", 1)
	assertHistogramCount(t, families, "lynxdb_ingest_otlp_request_bytes", 1)
	assertGaugeVecValue(t, families, "lynxdb_ingest_listener_up", "listener", "es", 1)
}

func TestPrometheusMetrics_ErrorCounter(t *testing.T) {
	pm := NewPrometheusMetrics()

	pm.RecordQuery("events", 100*time.Millisecond, "")

	families := gatherMetrics(t, pm)
	assertCounterValue(t, families, "lynxdb_query_errors_total", 0)

	pm.RecordQuery("events", time.Minute, string(ErrCodeExecutionError))

	families = gatherMetrics(t, pm)
	assertCounterVecValue(t, families, "lynxdb_query_errors_total", string(ErrCodeExecutionError), 1)

	pm.RecordQuery("aggregate", 500*time.Millisecond, string(ErrCodeQueryMemoryExceeded))

	families = gatherMetrics(t, pm)
	assertCounterVecValue(t, families, "lynxdb_query_errors_total", string(ErrCodeExecutionError), 1)
	assertCounterVecValue(t, families, "lynxdb_query_errors_total", string(ErrCodeQueryMemoryExceeded), 1)

	pm.RecordQuery("events", 200*time.Millisecond, string(ErrCodeExecutionError))

	families = gatherMetrics(t, pm)
	assertCounterVecValue(t, families, "lynxdb_query_errors_total", string(ErrCodeExecutionError), 2)
	assertCounterValue(t, families, "lynxdb_query_errors_total", 3)
	// Failed queries count in the duration histogram too.
	assertHistogramCount(t, families, "lynxdb_query_duration_seconds", 4)
}

func TestPrometheusMetrics_ResultTypeLabel(t *testing.T) {
	pm := NewPrometheusMetrics()

	pm.RecordQuery("events", 100*time.Millisecond, "")
	pm.RecordQuery("aggregate", 200*time.Millisecond, "")
	pm.RecordQuery("aggregate", 300*time.Millisecond, "")

	families := gatherMetrics(t, pm)
	mf, ok := families["lynxdb_query_duration_seconds"]
	if !ok {
		t.Fatal("lynxdb_query_duration_seconds not found")
	}

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

	if labels["events"] != 1 || labels["aggregate"] != 2 || len(labels) != 2 {
		t.Errorf("result_type counts = %v, want events 1 and aggregate 2", labels)
	}
}

func TestPrometheusMetrics_ErrorsAppearInHandler(t *testing.T) {
	pm := NewPrometheusMetrics()

	pm.RecordQuery("events", 100*time.Millisecond, string(ErrCodeExecutionError))

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
	if want := fmt.Sprintf(`type=%q`, ErrCodeExecutionError); !strings.Contains(bodyStr, want) {
		t.Errorf("%s label not found in /metrics output", want)
	}
}

func TestPrometheusMetrics_ServerRecordsQueriesAndIngest(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	ingestTestEvents(t, srv.Addr(), 10, 2)
	postQueryData(t, srv.Addr(), map[string]interface{}{"q": "source=main | stats count() by host"})

	// The duration of a query job is recorded once the job has finished.
	deadline := time.Now().Add(5 * time.Second)
	for {
		families := gatherMetrics(t, srv.promMetrics)
		var aggregates uint64
		for _, m := range families["lynxdb_query_duration_seconds"].GetMetric() {
			for _, lp := range m.GetLabel() {
				if lp.GetName() == "result_type" && lp.GetValue() == "aggregate" {
					aggregates += m.GetHistogram().GetSampleCount()
				}
			}
		}
		if aggregates == 1 {
			assertCounterValue(t, families, "lynxdb_ingest_events_total", 10)
			assertCounterValue(t, families, "lynxdb_ingest_batches_total", 1)
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("aggregate query durations recorded = %d, want 1", aggregates)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

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

	assertCounterVecLabelValue(t, families, name, "type", labelValue, expectedValue)
}

func assertCounterVecLabelValue(t *testing.T, families map[string]*dto.MetricFamily, name, labelName, labelValue string, expectedValue float64) {
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
			if lp.GetName() == labelName && lp.GetValue() == labelValue {
				got := m.GetCounter().GetValue()
				if got != expectedValue {
					t.Errorf("metric %q{%s=%q}: expected %f, got %f", name, labelName, labelValue, expectedValue, got)
				}

				return
			}
		}
	}

	if expectedValue == 0 {
		return // label not found and expected 0 — OK
	}
	t.Errorf("metric %q: label %s=%q not found", name, labelName, labelValue)
}

func assertGaugeVecValue(t *testing.T, families map[string]*dto.MetricFamily, name, labelName, labelValue string, expectedValue float64) {
	t.Helper()

	mf, ok := families[name]
	if !ok {
		t.Errorf("metric %q not found", name)

		return
	}

	for _, m := range mf.GetMetric() {
		for _, lp := range m.GetLabel() {
			if lp.GetName() == labelName && lp.GetValue() == labelValue {
				got := m.GetGauge().GetValue()
				if got != expectedValue {
					t.Errorf("metric %q{%s=%q}: expected %f, got %f", name, labelName, labelValue, expectedValue, got)
				}

				return
			}
		}
	}

	t.Errorf("metric %q: label %s=%q not found", name, labelName, labelValue)
}
