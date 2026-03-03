package rest

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/OrlovEvgeny/Lynxdb/pkg/server"
)

// PrometheusMetrics holds Prometheus metric collectors for query observability.
// Metrics are registered explicitly (no init()) for testability and clean lifecycle.
type PrometheusMetrics struct {
	registry *prometheus.Registry

	queryDuration         *prometheus.HistogramVec
	queryScanDuration     *prometheus.Histogram
	queryPipelineDuration *prometheus.Histogram
	queryPeakMemory       *prometheus.Histogram
	queryRowsScanned      *prometheus.Histogram

	segmentsSkippedBloom    prometheus.Counter
	segmentsSkippedColStats prometheus.Counter
	segmentsSkippedTime     prometheus.Counter
	segmentsScannedTotal    prometheus.Counter
	querySlowTotal          prometheus.Counter
	queryErrorsTotal        *prometheus.CounterVec
}

// NewPrometheusMetrics creates and registers all LynxDB Prometheus metrics.
// The returned handler serves the /metrics endpoint. Metrics are recorded
// via the OnQueryComplete callback set on the engine.
func NewPrometheusMetrics() *PrometheusMetrics {
	reg := prometheus.NewRegistry()

	// Include Go runtime and process metrics alongside LynxDB metrics.
	reg.MustRegister(collectors.NewGoCollector())
	reg.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	queryDuration := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "lynxdb_query_duration_seconds",
			Help:    "Query execution duration in seconds.",
			Buckets: prometheus.ExponentialBuckets(0.001, 2, 15), // 1ms to 32s
		},
		[]string{"result_type"},
	)

	scanDur := prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "lynxdb_query_scan_duration_seconds",
		Help:    "Time spent in segment scan phase.",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 15),
	})

	pipelineDur := prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "lynxdb_query_pipeline_duration_seconds",
		Help:    "Time spent in pipeline execution phase.",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 15),
	})

	peakMem := prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "lynxdb_query_peak_memory_bytes",
		Help:    "Peak memory allocated per query.",
		Buckets: prometheus.ExponentialBuckets(1024, 4, 10), // 1KB to 1GB
	})

	rowsScanned := prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "lynxdb_query_rows_scanned",
		Help:    "Number of rows scanned per query.",
		Buckets: prometheus.ExponentialBuckets(100, 4, 10), // 100 to ~100M
	})

	skippedBloom := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "lynxdb_segments_skipped_bloom_total",
		Help: "Total segments skipped by bloom filter.",
	})

	skippedColStats := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "lynxdb_segments_skipped_column_stats_total",
		Help: "Total segments skipped by column min/max stats.",
	})

	skippedTime := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "lynxdb_segments_skipped_time_total",
		Help: "Total segments skipped by time range pruning.",
	})

	scannedTotal := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "lynxdb_segments_scanned_total",
		Help: "Total segments scanned across all queries.",
	})

	slowTotal := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "lynxdb_query_slow_total",
		Help: "Total queries exceeding slow query threshold.",
	})

	errorsTotal := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "lynxdb_query_errors_total",
		Help: "Total query errors by type.",
	}, []string{"type"})

	reg.MustRegister(
		queryDuration,
		scanDur,
		pipelineDur,
		peakMem,
		rowsScanned,
		skippedBloom,
		skippedColStats,
		skippedTime,
		scannedTotal,
		slowTotal,
		errorsTotal,
	)

	return &PrometheusMetrics{
		registry:                reg,
		queryDuration:           queryDuration,
		queryScanDuration:       &scanDur,
		queryPipelineDuration:   &pipelineDur,
		queryPeakMemory:         &peakMem,
		queryRowsScanned:        &rowsScanned,
		segmentsSkippedBloom:    skippedBloom,
		segmentsSkippedColStats: skippedColStats,
		segmentsSkippedTime:     skippedTime,
		segmentsScannedTotal:    scannedTotal,
		querySlowTotal:          slowTotal,
		queryErrorsTotal:        errorsTotal,
	}
}

// Handler returns the HTTP handler that serves the Prometheus metrics endpoint.
func (pm *PrometheusMetrics) Handler() http.Handler {
	return promhttp.HandlerFor(pm.registry, promhttp.HandlerOpts{})
}

// RecordQuery records Prometheus observations from a completed query's stats.
// Called via Engine.OnQueryComplete callback — executed once per query, after
// the job transitions to done (or error/timeout).
func (pm *PrometheusMetrics) RecordQuery(ss *server.SearchStats) {
	if ss == nil {
		return
	}

	// Query duration histogram, labeled by result type.
	// ElapsedMS is always populated; convert to seconds for Prometheus convention.
	resultType := ss.ResultTypeLabel
	if resultType == "" {
		resultType = "query" // fallback for legacy callers
	}
	pm.queryDuration.WithLabelValues(resultType).Observe(ss.ElapsedMS / 1000)
	(*pm.queryScanDuration).Observe(ss.ScanMS / 1000)
	(*pm.queryPipelineDuration).Observe(ss.PipelineMS / 1000)

	if ss.PeakMemoryBytes > 0 {
		(*pm.queryPeakMemory).Observe(float64(ss.PeakMemoryBytes))
	}

	if ss.RowsScanned > 0 {
		(*pm.queryRowsScanned).Observe(float64(ss.RowsScanned))
	}

	// Segment skip counters (monotonically increasing across queries).
	if ss.SegmentsSkippedBF > 0 {
		pm.segmentsSkippedBloom.Add(float64(ss.SegmentsSkippedBF))
	}

	if ss.SegmentsSkippedStat > 0 {
		pm.segmentsSkippedColStats.Add(float64(ss.SegmentsSkippedStat))
	}

	if ss.SegmentsSkippedTime > 0 {
		pm.segmentsSkippedTime.Add(float64(ss.SegmentsSkippedTime))
	}

	if ss.SegmentsScanned > 0 {
		pm.segmentsScannedTotal.Add(float64(ss.SegmentsScanned))
	}

	// Slow query counter: incremented when the engine flags the query as slow
	// (exceeded SlowQueryThresholdMs). The threshold check is done in the engine
	// so the Prometheus layer doesn't need to know the config value.
	if ss.SlowQuery {
		pm.querySlowTotal.Inc()
	}

	// Error counter: labeled by error type (parse, execution, timeout, memory).
	// Only incremented when the query failed — ErrorType is empty for success.
	if ss.ErrorType != "" {
		pm.queryErrorsTotal.WithLabelValues(ss.ErrorType).Inc()
	}
}

// Registry returns the underlying Prometheus registry for testing.
func (pm *PrometheusMetrics) Registry() *prometheus.Registry {
	return pm.registry
}
