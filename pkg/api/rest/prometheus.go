package rest

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// PrometheusMetrics holds the Prometheus collectors of the server: query
// durations and errors, ingest counters and the counters of the shipper
// endpoints. Metrics are registered explicitly (no init()) for testability.
type PrometheusMetrics struct {
	registry *prometheus.Registry

	queryDuration    *prometheus.HistogramVec
	queryErrorsTotal *prometheus.CounterVec

	ingestEventsTotal          prometheus.Counter
	ingestBatchesTotal         prometheus.Counter
	ingestBytesTotal           prometheus.Counter
	ingestErrorsTotal          prometheus.Counter
	ingestESBulkRequestsTotal  *prometheus.CounterVec
	ingestESBulkItemsTotal     *prometheus.CounterVec
	ingestESHandshakeTotal     *prometheus.CounterVec
	ingestESBulkDuration       prometheus.Histogram
	ingestOTLPRequestsTotal    *prometheus.CounterVec
	ingestOTLPRecordsTotal     *prometheus.CounterVec
	ingestDroppedRecordsTotal  *prometheus.CounterVec
	ingestOTLPRequestBytes     prometheus.Histogram
	ingestListenerUp           *prometheus.GaugeVec
	decompressionRejectedTotal *prometheus.CounterVec
}

// NewPrometheusMetrics creates and registers all LynxDB Prometheus metrics.
func NewPrometheusMetrics() *PrometheusMetrics {
	reg := prometheus.NewRegistry()

	// Include Go runtime and process metrics alongside LynxDB metrics.
	reg.MustRegister(collectors.NewGoCollector())
	reg.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	pm := &PrometheusMetrics{
		registry: reg,
		queryDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "lynxdb_query_duration_seconds",
			Help:    "Query execution duration in seconds.",
			Buckets: prometheus.ExponentialBuckets(0.001, 2, 15), // 1ms to 32s
		}, []string{"result_type"}),
		queryErrorsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "lynxdb_query_errors_total",
			Help: "Total query errors by type.",
		}, []string{"type"}),
		ingestEventsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "lynxdb_ingest_events_total",
			Help: "Total events ingested.",
		}),
		ingestBatchesTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "lynxdb_ingest_batches_total",
			Help: "Total ingest batches processed.",
		}),
		ingestBytesTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "lynxdb_ingest_bytes_total",
			Help: "Total NDJSON bytes ingested.",
		}),
		ingestErrorsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "lynxdb_ingest_errors_total",
			Help: "Total ingest errors.",
		}),
		ingestESBulkRequestsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "lynxdb_ingest_es_bulk_requests_total",
			Help: "Total Elasticsearch bulk requests by result.",
		}, []string{"result"}),
		ingestESBulkItemsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "lynxdb_ingest_es_bulk_items_total",
			Help: "Total Elasticsearch bulk action items by action and result.",
		}, []string{"action", "result"}),
		ingestESHandshakeTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "lynxdb_ingest_es_handshake_total",
			Help: "Total Elasticsearch compatibility handshake and management probe requests.",
		}, []string{"kind"}),
		ingestESBulkDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "lynxdb_ingest_es_bulk_duration_seconds",
			Help:    "Wall time per Elasticsearch bulk request.",
			Buckets: prometheus.DefBuckets,
		}),
		ingestOTLPRequestsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "lynxdb_ingest_otlp_requests_total",
			Help: "Total OTLP HTTP requests by signal, encoding, and result.",
		}, []string{"signal", "encoding", "result"}),
		ingestOTLPRecordsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "lynxdb_ingest_otlp_records_total",
			Help: "Total OTLP records by signal and result.",
		}, []string{"signal", "result"}),
		ingestDroppedRecordsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "lynxdb_ingest_dropped_records_total",
			Help: "Total ingest records dropped by source and reason.",
		}, []string{"source", "reason"}),
		ingestOTLPRequestBytes: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "lynxdb_ingest_otlp_request_bytes",
			Help:    "Decompressed OTLP HTTP request body size in bytes.",
			Buckets: prometheus.ExponentialBuckets(1024, 2, 19),
		}),
		ingestListenerUp: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "lynxdb_ingest_listener_up",
			Help: "Whether an ingest listener is bound.",
		}, []string{"listener"}),
		decompressionRejectedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "lynxdb_ingest_decompression_rejected_total",
			Help: "Total shipper ingest requests rejected by compressed or decompressed body limits.",
		}, []string{"stage", "encoding"}),
	}

	reg.MustRegister(
		pm.queryDuration,
		pm.queryErrorsTotal,
		pm.ingestEventsTotal,
		pm.ingestBatchesTotal,
		pm.ingestBytesTotal,
		pm.ingestErrorsTotal,
		pm.ingestESBulkRequestsTotal,
		pm.ingestESBulkItemsTotal,
		pm.ingestESHandshakeTotal,
		pm.ingestESBulkDuration,
		pm.ingestOTLPRequestsTotal,
		pm.ingestOTLPRecordsTotal,
		pm.ingestDroppedRecordsTotal,
		pm.ingestOTLPRequestBytes,
		pm.ingestListenerUp,
		pm.decompressionRejectedTotal,
	)

	return pm
}

// RecordQuery records the duration of a finished query and, when it failed,
// the error code it failed with.
func (pm *PrometheusMetrics) RecordQuery(resultType string, d time.Duration, errType string) {
	pm.queryDuration.WithLabelValues(resultType).Observe(d.Seconds())
	if errType != "" {
		pm.queryErrorsTotal.WithLabelValues(errType).Inc()
	}
}

// RecordIngest counts one ingested batch of events and its bytes.
func (pm *PrometheusMetrics) RecordIngest(events, bytes int) {
	pm.ingestBatchesTotal.Inc()
	pm.ingestEventsTotal.Add(float64(events))
	pm.ingestBytesTotal.Add(float64(bytes))
}

// RecordIngestError counts a batch the database refused.
func (pm *PrometheusMetrics) RecordIngestError() {
	pm.ingestErrorsTotal.Inc()
}

func (pm *PrometheusMetrics) RecordESHandshake(kind string) {
	if kind == "" {
		kind = "unknown"
	}
	pm.ingestESHandshakeTotal.WithLabelValues(kind).Inc()
}

func (pm *PrometheusMetrics) RecordESBulkRequest(result string, durationSeconds float64) {
	pm.ingestESBulkRequestsTotal.WithLabelValues(result).Inc()
	pm.ingestESBulkDuration.Observe(durationSeconds)
}

func (pm *PrometheusMetrics) RecordESBulkItem(action, result string) {
	pm.ingestESBulkItemsTotal.WithLabelValues(action, result).Inc()
}

func (pm *PrometheusMetrics) RecordOTLPRequest(signal, encoding, result string, bytes int) {
	pm.ingestOTLPRequestsTotal.WithLabelValues(signal, encoding, result).Inc()
	pm.ingestOTLPRequestBytes.Observe(float64(bytes))
}

func (pm *PrometheusMetrics) RecordOTLPRecords(signal, result string, count int) {
	if count <= 0 {
		return
	}
	pm.ingestOTLPRecordsTotal.WithLabelValues(signal, result).Add(float64(count))
}

func (pm *PrometheusMetrics) RecordDroppedRecords(source, reason string, count int) {
	if count <= 0 {
		return
	}
	pm.ingestDroppedRecordsTotal.WithLabelValues(source, reason).Add(float64(count))
}

func (pm *PrometheusMetrics) SetListenerUp(listener string, up bool) {
	value := 0.0
	if up {
		value = 1
	}
	pm.ingestListenerUp.WithLabelValues(listener).Set(value)
}

func (pm *PrometheusMetrics) OnReject(stage, encoding string) {
	if encoding == "" {
		encoding = "identity"
	}
	pm.decompressionRejectedTotal.WithLabelValues(stage, encoding).Inc()
}

// Handler returns the HTTP handler that serves the Prometheus metrics endpoint.
func (pm *PrometheusMetrics) Handler() http.Handler {
	return promhttp.HandlerFor(pm.registry, promhttp.HandlerOpts{})
}

// Registry returns the underlying Prometheus registry for testing.
func (pm *PrometheusMetrics) Registry() *prometheus.Registry {
	return pm.registry
}
