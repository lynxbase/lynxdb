package rest

import (
	"net/http"
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/lynxbase/lynxdb/pkg/memgov"
	"github.com/lynxbase/lynxdb/pkg/server"
	"github.com/lynxbase/lynxdb/pkg/storage"
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
	querySpilledTotal       *prometheus.CounterVec
	spillBytesTotal         prometheus.Counter

	// Ingestion metrics.
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
	stagingFlushesTotal        *prometheus.CounterVec
	stagingOverflowsTotal      prometheus.Counter
	stagingDroppedTotal        *prometheus.CounterVec
	stagingBytes               prometheus.Gauge
	stagingEvents              prometheus.Gauge
	stagingAgeSeconds          prometheus.Gauge
	stagingFlushSizeBytes      prometheus.Histogram

	// Compaction metrics.
	compactionRunsTotal     prometheus.Counter
	compactionDurationTotal prometheus.Counter
	compactionInputBytes    prometheus.Counter
	compactionOutputBytes   prometheus.Counter
	compactionErrorsTotal   prometheus.Counter
	compactionQueueDepth    prometheus.Gauge

	// Tiering metrics.
	tieringUploadsTotal       prometheus.Counter
	tieringUploadBytesTotal   prometheus.Counter
	tieringDownloadsTotal     prometheus.Counter
	tieringDownloadBytesTotal prometheus.Counter

	// Cache metrics.
	cacheHitsTotal      prometheus.Counter
	cacheMissesTotal    prometheus.Counter
	cacheEvictionsTotal prometheus.Counter
	cacheSizeBytes      prometheus.Gauge

	// Memory-governance metrics.
	memgovClassBytes        *prometheus.GaugeVec
	memgovClassPeakBytes    *prometheus.GaugeVec
	memgovPressureEvents    prometheus.Counter
	memgovRevocationFreed   *prometheus.CounterVec
	spillFilesActive        prometheus.Gauge
	spillBytesActive        prometheus.Gauge
	lastMemgovPressureEvent atomic.Int64
	lastRevocationFreed     atomic.Int64
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
	querySpilled := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "lynxdb_query_spilled_total",
		Help: "Total completed queries that spilled to disk by operator.",
	}, []string{"operator"})
	spillBytes := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "lynxdb_spill_bytes_total",
		Help: "Total bytes written to query spill files by completed queries.",
	})

	// Ingestion metrics.
	ingestEvents := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "lynxdb_ingest_events_total",
		Help: "Total events ingested.",
	})
	ingestBatches := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "lynxdb_ingest_batches_total",
		Help: "Total ingest batches processed.",
	})
	ingestBytes := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "lynxdb_ingest_bytes_total",
		Help: "Total raw bytes ingested.",
	})
	ingestErrors := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "lynxdb_ingest_errors_total",
		Help: "Total ingest errors.",
	})
	ingestESBulkRequests := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "lynxdb_ingest_es_bulk_requests_total",
		Help: "Total Elasticsearch bulk requests by result.",
	}, []string{"result"})
	ingestESBulkItems := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "lynxdb_ingest_es_bulk_items_total",
		Help: "Total Elasticsearch bulk action items by action and result.",
	}, []string{"action", "result"})
	ingestESHandshake := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "lynxdb_ingest_es_handshake_total",
		Help: "Total Elasticsearch compatibility handshake and management probe requests.",
	}, []string{"kind"})
	ingestESBulkDuration := prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "lynxdb_ingest_es_bulk_duration_seconds",
		Help:    "Wall time per Elasticsearch bulk request.",
		Buckets: prometheus.DefBuckets,
	})
	ingestOTLPRequests := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "lynxdb_ingest_otlp_requests_total",
		Help: "Total OTLP HTTP requests by signal, encoding, and result.",
	}, []string{"signal", "encoding", "result"})
	ingestOTLPRecords := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "lynxdb_ingest_otlp_records_total",
		Help: "Total OTLP records by signal and result.",
	}, []string{"signal", "result"})
	ingestDroppedRecords := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "lynxdb_ingest_dropped_records_total",
		Help: "Total ingest records dropped by source and reason.",
	}, []string{"source", "reason"})
	ingestOTLPRequestBytes := prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "lynxdb_ingest_otlp_request_bytes",
		Help:    "Decompressed OTLP HTTP request body size in bytes.",
		Buckets: prometheus.ExponentialBuckets(1024, 2, 19),
	})
	ingestListenerUp := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "lynxdb_ingest_listener_up",
		Help: "Whether an ingest listener is bound.",
	}, []string{"listener"})
	decompressionRejected := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "lynxdb_ingest_decompression_rejected_total",
		Help: "Total shipper ingest requests rejected by compressed or decompressed body limits.",
	}, []string{"stage", "encoding"})
	stagingFlushes := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "lynxdb_ingest_staging_flushes_total",
		Help: "Total staging-buffer flushes by trigger.",
	}, []string{"trigger"})
	stagingOverflows := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "lynxdb_ingest_staging_overflows_total",
		Help: "Total staging-buffer overflows.",
	})
	stagingDropped := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "lynxdb_ingest_staging_dropped_total",
		Help: "Total staged events dropped after retry exhaustion.",
	}, []string{"reason"})
	stagingBytes := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "lynxdb_ingest_staging_bytes",
		Help: "Current pending bytes in the shipper staging buffer.",
	})
	stagingEvents := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "lynxdb_ingest_staging_events",
		Help: "Current pending event count in the shipper staging buffer.",
	})
	stagingAge := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "lynxdb_ingest_staging_age_seconds",
		Help: "Age of the oldest pending staged event in seconds.",
	})
	stagingFlushSize := prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "lynxdb_ingest_staging_flush_size_bytes",
		Help:    "Bytes flushed from the shipper staging buffer.",
		Buckets: prometheus.ExponentialBuckets(1024, 2, 19),
	})

	// Compaction metrics.
	compactionRuns := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "lynxdb_compaction_runs_total",
		Help: "Total compaction runs.",
	})
	compactionDuration := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "lynxdb_compaction_duration_seconds_total",
		Help: "Cumulative compaction duration in seconds.",
	})
	compactionInput := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "lynxdb_compaction_input_bytes_total",
		Help: "Total bytes read by compaction.",
	})
	compactionOutput := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "lynxdb_compaction_output_bytes_total",
		Help: "Total bytes written by compaction.",
	})
	compactionErrors := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "lynxdb_compaction_errors_total",
		Help: "Total compaction errors.",
	})
	compactionQueue := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "lynxdb_compaction_queue_depth",
		Help: "Current number of pending compaction jobs.",
	})

	// Tiering metrics.
	tieringUploads := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "lynxdb_tiering_uploads_total",
		Help: "Total segment uploads to warm/cold tier.",
	})
	tieringUploadBytes := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "lynxdb_tiering_upload_bytes_total",
		Help: "Total bytes uploaded to warm/cold tier.",
	})
	tieringDownloads := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "lynxdb_tiering_downloads_total",
		Help: "Total segment downloads from warm/cold tier.",
	})
	tieringDownloadBytes := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "lynxdb_tiering_download_bytes_total",
		Help: "Total bytes downloaded from warm/cold tier.",
	})

	// Cache metrics.
	cacheHits := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "lynxdb_cache_hits_total",
		Help: "Total query cache hits.",
	})
	cacheMisses := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "lynxdb_cache_misses_total",
		Help: "Total query cache misses.",
	})
	cacheEvictions := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "lynxdb_cache_evictions_total",
		Help: "Total query cache evictions.",
	})
	cacheSize := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "lynxdb_cache_size_bytes",
		Help: "Current query cache size in bytes.",
	})

	memgovClassBytes := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "lynxdb_memgov_class_bytes",
		Help: "Current bytes allocated by memory-governance class.",
	}, []string{"class"})
	memgovClassPeakBytes := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "lynxdb_memgov_class_peak_bytes",
		Help: "Peak bytes allocated by memory-governance class.",
	}, []string{"class"})
	memgovPressureEvents := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "lynxdb_memgov_pressure_events_total",
		Help: "Total memory-governance pressure events.",
	})
	memgovRevocationFreed := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "lynxdb_memgov_revocation_freed_bytes_total",
		Help: "Total bytes freed by memory-governance revocation callbacks.",
	}, []string{"class"})
	spillFilesActive := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "lynxdb_spill_files_active",
		Help: "Current number of tracked query spill files.",
	})
	spillBytesActive := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "lynxdb_spill_bytes_active",
		Help: "Current bytes held in tracked query spill files.",
	})

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
		querySpilled,
		spillBytes,
		ingestEvents,
		ingestBatches,
		ingestBytes,
		ingestErrors,
		ingestESBulkRequests,
		ingestESBulkItems,
		ingestESHandshake,
		ingestESBulkDuration,
		ingestOTLPRequests,
		ingestOTLPRecords,
		ingestDroppedRecords,
		ingestOTLPRequestBytes,
		ingestListenerUp,
		decompressionRejected,
		stagingFlushes,
		stagingOverflows,
		stagingDropped,
		stagingBytes,
		stagingEvents,
		stagingAge,
		stagingFlushSize,
		compactionRuns,
		compactionDuration,
		compactionInput,
		compactionOutput,
		compactionErrors,
		compactionQueue,
		tieringUploads,
		tieringUploadBytes,
		tieringDownloads,
		tieringDownloadBytes,
		cacheHits,
		cacheMisses,
		cacheEvictions,
		cacheSize,
		memgovClassBytes,
		memgovClassPeakBytes,
		memgovPressureEvents,
		memgovRevocationFreed,
		spillFilesActive,
		spillBytesActive,
	)

	return &PrometheusMetrics{
		registry:                   reg,
		queryDuration:              queryDuration,
		queryScanDuration:          &scanDur,
		queryPipelineDuration:      &pipelineDur,
		queryPeakMemory:            &peakMem,
		queryRowsScanned:           &rowsScanned,
		segmentsSkippedBloom:       skippedBloom,
		segmentsSkippedColStats:    skippedColStats,
		segmentsSkippedTime:        skippedTime,
		segmentsScannedTotal:       scannedTotal,
		querySlowTotal:             slowTotal,
		queryErrorsTotal:           errorsTotal,
		querySpilledTotal:          querySpilled,
		spillBytesTotal:            spillBytes,
		ingestEventsTotal:          ingestEvents,
		ingestBatchesTotal:         ingestBatches,
		ingestBytesTotal:           ingestBytes,
		ingestErrorsTotal:          ingestErrors,
		ingestESBulkRequestsTotal:  ingestESBulkRequests,
		ingestESBulkItemsTotal:     ingestESBulkItems,
		ingestESHandshakeTotal:     ingestESHandshake,
		ingestESBulkDuration:       ingestESBulkDuration,
		ingestOTLPRequestsTotal:    ingestOTLPRequests,
		ingestOTLPRecordsTotal:     ingestOTLPRecords,
		ingestDroppedRecordsTotal:  ingestDroppedRecords,
		ingestOTLPRequestBytes:     ingestOTLPRequestBytes,
		ingestListenerUp:           ingestListenerUp,
		decompressionRejectedTotal: decompressionRejected,
		stagingFlushesTotal:        stagingFlushes,
		stagingOverflowsTotal:      stagingOverflows,
		stagingDroppedTotal:        stagingDropped,
		stagingBytes:               stagingBytes,
		stagingEvents:              stagingEvents,
		stagingAgeSeconds:          stagingAge,
		stagingFlushSizeBytes:      stagingFlushSize,
		compactionRunsTotal:        compactionRuns,
		compactionDurationTotal:    compactionDuration,
		compactionInputBytes:       compactionInput,
		compactionOutputBytes:      compactionOutput,
		compactionErrorsTotal:      compactionErrors,
		compactionQueueDepth:       compactionQueue,
		tieringUploadsTotal:        tieringUploads,
		tieringUploadBytesTotal:    tieringUploadBytes,
		tieringDownloadsTotal:      tieringDownloads,
		tieringDownloadBytesTotal:  tieringDownloadBytes,
		cacheHitsTotal:             cacheHits,
		cacheMissesTotal:           cacheMisses,
		cacheEvictionsTotal:        cacheEvictions,
		cacheSizeBytes:             cacheSize,
		memgovClassBytes:           memgovClassBytes,
		memgovClassPeakBytes:       memgovClassPeakBytes,
		memgovPressureEvents:       memgovPressureEvents,
		memgovRevocationFreed:      memgovRevocationFreed,
		spillFilesActive:           spillFilesActive,
		spillBytesActive:           spillBytesActive,
	}
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

func (pm *PrometheusMetrics) SetState(bytes int64, events int, ageSeconds float64) {
	pm.stagingBytes.Set(float64(bytes))
	pm.stagingEvents.Set(float64(events))
	pm.stagingAgeSeconds.Set(ageSeconds)
}

func (pm *PrometheusMetrics) RecordFlush(trigger string, bytes int64) {
	pm.stagingFlushesTotal.WithLabelValues(trigger).Inc()
	pm.stagingFlushSizeBytes.Observe(float64(bytes))
}

func (pm *PrometheusMetrics) RecordOverflow() {
	pm.stagingOverflowsTotal.Inc()
}

func (pm *PrometheusMetrics) RecordDropped(reason string, events int) {
	pm.stagingDroppedTotal.WithLabelValues(reason).Add(float64(events))
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

	if ss.SpillBytes > 0 {
		pm.spillBytesTotal.Add(float64(ss.SpillBytes))
	}
	if ss.SpilledToDisk {
		recorded := false
		for _, stage := range ss.PipelineStages {
			if stage.SpilledRows > 0 || stage.SpillBytes > 0 {
				pm.querySpilledTotal.WithLabelValues(stage.Name).Inc()
				recorded = true
			}
		}
		if !recorded {
			pm.querySpilledTotal.WithLabelValues("unknown").Inc()
		}
	}
}

// Registry returns the underlying Prometheus registry for testing.
func (pm *PrometheusMetrics) Registry() *prometheus.Registry {
	return pm.registry
}

// RecordGovernorStats updates memory-governance metrics from the latest engine
// snapshot. Gauges are set directly; pressure events are emitted as a monotonic
// counter by adding only the snapshot delta since the previous scrape.
func (pm *PrometheusMetrics) RecordGovernorStats(stats *memgov.TotalStats) {
	if stats == nil {
		return
	}

	for i := range stats.ByClass {
		class := memgov.MemoryClass(i).String()
		cs := stats.ByClass[i]
		pm.memgovClassBytes.WithLabelValues(class).Set(float64(cs.Allocated))
		pm.memgovClassPeakBytes.WithLabelValues(class).Set(float64(cs.Peak))
	}

	previous := pm.lastMemgovPressureEvent.Swap(stats.PressureEvents)
	if delta := stats.PressureEvents - previous; delta > 0 {
		pm.memgovPressureEvents.Add(float64(delta))
	}
}

// RecordSpillStats updates scrape-time spill-manager gauges.
func (pm *PrometheusMetrics) RecordSpillStats(fileCount int, totalBytes int64) {
	pm.spillFilesActive.Set(float64(fileCount))
	pm.spillBytesActive.Set(float64(totalBytes))
}

// RecordRevocationStats updates cumulative revocation metrics from the latest
// engine snapshot.
func (pm *PrometheusMetrics) RecordRevocationStats(spillableFreedBytes int64) {
	previous := pm.lastRevocationFreed.Swap(spillableFreedBytes)
	if delta := spillableFreedBytes - previous; delta > 0 {
		pm.memgovRevocationFreed.WithLabelValues(memgov.ClassSpillable.String()).Add(float64(delta))
	}
}

// RecordStorageMetrics reads from the storage engine metrics and updates
// Prometheus gauges/counters for ingestion, compaction, tiering, and cache.
// Called periodically or on-demand (e.g., from /metrics scrape handler).
func (pm *PrometheusMetrics) RecordStorageMetrics(sm *storage.Metrics) {
	if sm == nil {
		return
	}

	// Ingestion.
	if v := sm.IngestEvents.Load(); v > 0 {
		pm.ingestEventsTotal.Add(float64(v))
	}
	if v := sm.IngestBatches.Load(); v > 0 {
		pm.ingestBatchesTotal.Add(float64(v))
	}
	if v := sm.IngestBytes.Load(); v > 0 {
		pm.ingestBytesTotal.Add(float64(v))
	}
	if v := sm.IngestErrors.Load(); v > 0 {
		pm.ingestErrorsTotal.Add(float64(v))
	}

	// Compaction.
	if v := sm.CompactionRuns.Load(); v > 0 {
		pm.compactionRunsTotal.Add(float64(v))
	}
	if v := sm.CompactionDurationNs.Load(); v > 0 {
		pm.compactionDurationTotal.Add(float64(v) / 1e9)
	}
	if v := sm.CompactionInputBytes.Load(); v > 0 {
		pm.compactionInputBytes.Add(float64(v))
	}
	if v := sm.CompactionOutputBytes.Load(); v > 0 {
		pm.compactionOutputBytes.Add(float64(v))
	}
	if v := sm.CompactionErrors.Load(); v > 0 {
		pm.compactionErrorsTotal.Add(float64(v))
	}
	pm.compactionQueueDepth.Set(float64(sm.CompactionQueueDepth.Load()))

	// Tiering.
	if v := sm.TieringUploads.Load(); v > 0 {
		pm.tieringUploadsTotal.Add(float64(v))
	}
	if v := sm.TieringUploadBytes.Load(); v > 0 {
		pm.tieringUploadBytesTotal.Add(float64(v))
	}
	if v := sm.TieringDownloads.Load(); v > 0 {
		pm.tieringDownloadsTotal.Add(float64(v))
	}
	if v := sm.TieringDownloadBytes.Load(); v > 0 {
		pm.tieringDownloadBytesTotal.Add(float64(v))
	}

	// Cache.
	if v := sm.CacheHits.Load(); v > 0 {
		pm.cacheHitsTotal.Add(float64(v))
	}
	if v := sm.CacheMisses.Load(); v > 0 {
		pm.cacheMissesTotal.Add(float64(v))
	}
	if v := sm.CacheEvictions.Load(); v > 0 {
		pm.cacheEvictionsTotal.Add(float64(v))
	}
	pm.cacheSizeBytes.Set(float64(sm.CacheSizeBytes.Load()))
}
