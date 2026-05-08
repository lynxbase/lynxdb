package config

// Environment variable names for LynxDB configuration.
// Defined here so they can be referenced in envBindings, Entries, and elsewhere.
const (
	// LYNXDB_AUTH_ENABLED is the enable or disable API key authentication.
	// YAML key: auth.enabled. Default: false.
	LYNXDB_AUTH_ENABLED = "LYNXDB_AUTH_ENABLED"
	// LYNXDB_BUFFER_MANAGER_BATCHER_TARGET_PERCENT is the advisory % of pool for batcher.
	// YAML key: buffer_manager.batcher_target_percent. Default: 10.
	LYNXDB_BUFFER_MANAGER_BATCHER_TARGET_PERCENT = "LYNXDB_BUFFER_MANAGER_BATCHER_TARGET_PERCENT"
	// LYNXDB_BUFFER_MANAGER_CACHE_TARGET_PERCENT is the advisory % of pool for segment cache.
	// YAML key: buffer_manager.cache_target_percent. Default: 60.
	LYNXDB_BUFFER_MANAGER_CACHE_TARGET_PERCENT = "LYNXDB_BUFFER_MANAGER_CACHE_TARGET_PERCENT"
	// LYNXDB_BUFFER_MANAGER_ENABLED is the activate unified buffer manager.
	// YAML key: buffer_manager.enabled. Default: false.
	LYNXDB_BUFFER_MANAGER_ENABLED = "LYNXDB_BUFFER_MANAGER_ENABLED"
	// LYNXDB_BUFFER_MANAGER_ENABLE_OFF_HEAP is the use mmap for page data.
	// YAML key: buffer_manager.enable_off_heap. Default: true.
	LYNXDB_BUFFER_MANAGER_ENABLE_OFF_HEAP = "LYNXDB_BUFFER_MANAGER_ENABLE_OFF_HEAP"
	// LYNXDB_BUFFER_MANAGER_MAX_MEMORY_BYTES is the total buffer pool memory (0 = auto: 80% RAM).
	// YAML key: buffer_manager.max_memory_bytes. Default: 0.
	LYNXDB_BUFFER_MANAGER_MAX_MEMORY_BYTES = "LYNXDB_BUFFER_MANAGER_MAX_MEMORY_BYTES"
	// LYNXDB_BUFFER_MANAGER_MAX_PINNED_PAGES_PER_QUERY is the safety limit for pinned pages (0 = no limit).
	// YAML key: buffer_manager.max_pinned_pages_per_query. Default: 1024.
	LYNXDB_BUFFER_MANAGER_MAX_PINNED_PAGES_PER_QUERY = "LYNXDB_BUFFER_MANAGER_MAX_PINNED_PAGES_PER_QUERY"
	// LYNXDB_BUFFER_MANAGER_PAGE_SIZE is the page size in bytes (must be power of 2, >= 4096).
	// YAML key: buffer_manager.page_size. Default: 65536.
	LYNXDB_BUFFER_MANAGER_PAGE_SIZE = "LYNXDB_BUFFER_MANAGER_PAGE_SIZE"
	// LYNXDB_BUFFER_MANAGER_QUERY_TARGET_PERCENT is the advisory % of pool for query operators.
	// YAML key: buffer_manager.query_target_percent. Default: 30.
	LYNXDB_BUFFER_MANAGER_QUERY_TARGET_PERCENT = "LYNXDB_BUFFER_MANAGER_QUERY_TARGET_PERCENT"
	// LYNXDB_CLUSTER_ACK_LEVEL is the cluster acknowledgement level.
	// YAML key: cluster.ack_level. Default: "".
	LYNXDB_CLUSTER_ACK_LEVEL = "LYNXDB_CLUSTER_ACK_LEVEL"
	// LYNXDB_CLUSTER_DC_HLL_THRESHOLD is the DC HLL threshold for cluster.
	// YAML key: cluster.dc_hll_threshold. Default: "".
	LYNXDB_CLUSTER_DC_HLL_THRESHOLD = "LYNXDB_CLUSTER_DC_HLL_THRESHOLD"
	// LYNXDB_CLUSTER_ENABLED is the enable clustering.
	// YAML key: cluster.enabled. Default: false.
	LYNXDB_CLUSTER_ENABLED = "LYNXDB_CLUSTER_ENABLED"
	// LYNXDB_CLUSTER_GRPC_PORT is the cluster gRPC port.
	// YAML key: cluster.grpc_port. Default: "".
	LYNXDB_CLUSTER_GRPC_PORT = "LYNXDB_CLUSTER_GRPC_PORT"
	// LYNXDB_CLUSTER_HEARTBEAT_INTERVAL is the heartbeat interval between cluster nodes.
	// YAML key: cluster.heartbeat_interval. Default: "".
	LYNXDB_CLUSTER_HEARTBEAT_INTERVAL = "LYNXDB_CLUSTER_HEARTBEAT_INTERVAL"
	// LYNXDB_CLUSTER_LEASE_DURATION is the cluster lease duration.
	// YAML key: cluster.lease_duration. Default: "".
	LYNXDB_CLUSTER_LEASE_DURATION = "LYNXDB_CLUSTER_LEASE_DURATION"
	// LYNXDB_CLUSTER_MAX_CLOCK_SKEW is the max clock skew tolerance.
	// YAML key: cluster.max_clock_skew. Default: "".
	LYNXDB_CLUSTER_MAX_CLOCK_SKEW = "LYNXDB_CLUSTER_MAX_CLOCK_SKEW"
	// LYNXDB_CLUSTER_MAX_CONCURRENT_SHARD_QUERIES is the max concurrent shard queries.
	// YAML key: cluster.max_concurrent_shard_queries. Default: "".
	LYNXDB_CLUSTER_MAX_CONCURRENT_SHARD_QUERIES = "LYNXDB_CLUSTER_MAX_CONCURRENT_SHARD_QUERIES"
	// LYNXDB_CLUSTER_META_LOSS_TIMEOUT is the meta node loss timeout.
	// YAML key: cluster.meta_loss_timeout. Default: "".
	LYNXDB_CLUSTER_META_LOSS_TIMEOUT = "LYNXDB_CLUSTER_META_LOSS_TIMEOUT"
	// LYNXDB_CLUSTER_NODE_ID is the unique node identifier.
	// YAML key: cluster.node_id. Default: "".
	LYNXDB_CLUSTER_NODE_ID = "LYNXDB_CLUSTER_NODE_ID"
	// LYNXDB_CLUSTER_PARTIAL_FAILURE_THRESHOLD is the partial failure threshold.
	// YAML key: cluster.partial_failure_threshold. Default: "".
	LYNXDB_CLUSTER_PARTIAL_FAILURE_THRESHOLD = "LYNXDB_CLUSTER_PARTIAL_FAILURE_THRESHOLD"
	// LYNXDB_CLUSTER_PARTIAL_RESULTS is the allow partial query results.
	// YAML key: cluster.partial_results. Default: "".
	LYNXDB_CLUSTER_PARTIAL_RESULTS = "LYNXDB_CLUSTER_PARTIAL_RESULTS"
	// LYNXDB_CLUSTER_REPLICATION_FACTOR is the number of replicas per shard.
	// YAML key: cluster.replication_factor. Default: "".
	LYNXDB_CLUSTER_REPLICATION_FACTOR = "LYNXDB_CLUSTER_REPLICATION_FACTOR"
	// LYNXDB_CLUSTER_ROLES is the comma-separated list of node roles.
	// YAML key: cluster.roles. Default: "".
	LYNXDB_CLUSTER_ROLES = "LYNXDB_CLUSTER_ROLES"
	// LYNXDB_CLUSTER_SEEDS is the comma-separated list of seed nodes.
	// YAML key: cluster.seeds. Default: "".
	LYNXDB_CLUSTER_SEEDS = "LYNXDB_CLUSTER_SEEDS"
	// LYNXDB_CLUSTER_SHARD_QUERY_TIMEOUT is the timeout for shard queries.
	// YAML key: cluster.shard_query_timeout. Default: "".
	LYNXDB_CLUSTER_SHARD_QUERY_TIMEOUT = "LYNXDB_CLUSTER_SHARD_QUERY_TIMEOUT"
	// LYNXDB_CLUSTER_TIME_BUCKET_SIZE is the time bucket size for cluster.
	// YAML key: cluster.time_bucket_size. Default: "".
	LYNXDB_CLUSTER_TIME_BUCKET_SIZE = "LYNXDB_CLUSTER_TIME_BUCKET_SIZE"
	// LYNXDB_CLUSTER_VIRTUAL_PARTITION_COUNT is the virtual partition count.
	// YAML key: cluster.virtual_partition_count. Default: "".
	LYNXDB_CLUSTER_VIRTUAL_PARTITION_COUNT = "LYNXDB_CLUSTER_VIRTUAL_PARTITION_COUNT"
	// LYNXDB_CONFIG is the path to the YAML config file (overrides auto-discovery).
	LYNXDB_CONFIG = "LYNXDB_CONFIG"
	// LYNXDB_DATA_DIR is the root data directory for persistent storage.
	// YAML key: data_dir. Default: ~/.local/share/lynxdb.
	LYNXDB_DATA_DIR = "LYNXDB_DATA_DIR"
	// LYNXDB_HTTP_IDLE_TIMEOUT is the HTTP keep-alive idle timeout.
	// YAML key: http.idle_timeout. Default: 120s.
	LYNXDB_HTTP_IDLE_TIMEOUT = "LYNXDB_HTTP_IDLE_TIMEOUT"
	// LYNXDB_HTTP_RATE_LIMIT is the requests per second per IP (0 = unlimited).
	// YAML key: http.rate_limit. Default: 0.
	LYNXDB_HTTP_RATE_LIMIT = "LYNXDB_HTTP_RATE_LIMIT"
	// LYNXDB_HTTP_READ_HEADER_TIMEOUT is the max time allowed to read request headers.
	// YAML key: http.read_header_timeout. Default: 10s.
	LYNXDB_HTTP_READ_HEADER_TIMEOUT = "LYNXDB_HTTP_READ_HEADER_TIMEOUT"
	// LYNXDB_HTTP_SHUTDOWN_TIMEOUT is the graceful shutdown timeout.
	// YAML key: http.shutdown_timeout. Default: 30s.
	LYNXDB_HTTP_SHUTDOWN_TIMEOUT = "LYNXDB_HTTP_SHUTDOWN_TIMEOUT"
	// LYNXDB_INGEST_DEDUP_CAPACITY is the dedup LRU capacity per index.
	// YAML key: ingest.dedup_capacity. Default: 100000.
	LYNXDB_INGEST_DEDUP_CAPACITY = "LYNXDB_INGEST_DEDUP_CAPACITY"
	// LYNXDB_INGEST_DEDUP_ENABLED is the enable dedup of identical consecutive events.
	// YAML key: ingest.dedup_enabled. Default: false.
	LYNXDB_INGEST_DEDUP_ENABLED = "LYNXDB_INGEST_DEDUP_ENABLED"
	// LYNXDB_INGEST_ES_COMPAT_ADVERTISED_VERSION is the version shown to Elasticsearch shippers.
	// YAML key: ingest.es_compat.advertised_version. Default: 8.15.0.
	LYNXDB_INGEST_ES_COMPAT_ADVERTISED_VERSION = "LYNXDB_INGEST_ES_COMPAT_ADVERTISED_VERSION"
	// LYNXDB_INGEST_ES_COMPAT_CLUSTER_NAME is the cluster name in Elasticsearch handshake.
	// YAML key: ingest.es_compat.cluster_name. Default: lynxdb.
	LYNXDB_INGEST_ES_COMPAT_CLUSTER_NAME = "LYNXDB_INGEST_ES_COMPAT_CLUSTER_NAME"
	// LYNXDB_INGEST_ES_COMPAT_ENABLED is the enable drop-in Elasticsearch shipper endpoints.
	// YAML key: ingest.es_compat.enabled. Default: true.
	LYNXDB_INGEST_ES_COMPAT_ENABLED = "LYNXDB_INGEST_ES_COMPAT_ENABLED"
	// LYNXDB_INGEST_ES_COMPAT_STRIP_LOGSTASH_DATE_SUFFIX is the map Fluent Bit Logstash_Format indexes to one source.
	// YAML key: ingest.es_compat.strip_logstash_date_suffix. Default: true.
	LYNXDB_INGEST_ES_COMPAT_STRIP_LOGSTASH_DATE_SUFFIX = "LYNXDB_INGEST_ES_COMPAT_STRIP_LOGSTASH_DATE_SUFFIX"
	// LYNXDB_INGEST_FSYNC is the flush to disk on every ingest batch (empty = auto).
	// YAML key: ingest.fsync. Default: "".
	LYNXDB_INGEST_FSYNC = "LYNXDB_INGEST_FSYNC"
	// LYNXDB_INGEST_LIMITS_MAX_COMPRESSED_BODY_BYTES is the max compressed shipper request body.
	// YAML key: ingest.limits.max_compressed_body_bytes. Default: 32mb.
	LYNXDB_INGEST_LIMITS_MAX_COMPRESSED_BODY_BYTES = "LYNXDB_INGEST_LIMITS_MAX_COMPRESSED_BODY_BYTES"
	// LYNXDB_INGEST_LIMITS_MAX_DECOMPRESSED_BODY_BYTES is the max decoded shipper request body.
	// YAML key: ingest.limits.max_decompressed_body_bytes. Default: 256mb.
	LYNXDB_INGEST_LIMITS_MAX_DECOMPRESSED_BODY_BYTES = "LYNXDB_INGEST_LIMITS_MAX_DECOMPRESSED_BODY_BYTES"
	// LYNXDB_INGEST_MAX_BATCH_SIZE is the max events per streaming batch.
	// YAML key: ingest.max_batch_size. Default: 1000.
	LYNXDB_INGEST_MAX_BATCH_SIZE = "LYNXDB_INGEST_MAX_BATCH_SIZE"
	// LYNXDB_INGEST_MAX_BODY_SIZE is the max HTTP request body size.
	// YAML key: ingest.max_body_size. Default: 100mb.
	LYNXDB_INGEST_MAX_BODY_SIZE = "LYNXDB_INGEST_MAX_BODY_SIZE"
	// LYNXDB_INGEST_MAX_LINE_BYTES is the max bytes per single line in streaming ingest.
	// YAML key: ingest.max_line_bytes. Default: 1048576.
	LYNXDB_INGEST_MAX_LINE_BYTES = "LYNXDB_INGEST_MAX_LINE_BYTES"
	// LYNXDB_INGEST_MODE is the ingest mode (empty = auto).
	// YAML key: ingest.mode. Default: "".
	LYNXDB_INGEST_MODE = "LYNXDB_INGEST_MODE"
	// LYNXDB_INGEST_OTLP_GRPC_LISTEN is the OTLP/gRPC listen address (empty = disabled).
	// YAML key: ingest.otlp.grpc_listen. Default: "".
	LYNXDB_INGEST_OTLP_GRPC_LISTEN = "LYNXDB_INGEST_OTLP_GRPC_LISTEN"
	// LYNXDB_INGEST_OTLP_GRPC_MAX_RECV_BYTES is the OTLP/gRPC max receive message size.
	// YAML key: ingest.otlp.grpc_max_recv_bytes. Default: 16mb.
	LYNXDB_INGEST_OTLP_GRPC_MAX_RECV_BYTES = "LYNXDB_INGEST_OTLP_GRPC_MAX_RECV_BYTES"
	// LYNXDB_INGEST_OTLP_HTTP_LISTEN is the canonical OTLP/HTTP listen address (empty = disabled).
	// YAML key: ingest.otlp.http_listen. Default: 0.0.0.0:4318.
	LYNXDB_INGEST_OTLP_HTTP_LISTEN = "LYNXDB_INGEST_OTLP_HTTP_LISTEN"
	// LYNXDB_INGEST_SPLUNK_HEC_ENABLED is the enable Splunk HEC-compatible routes.
	// YAML key: ingest.splunk_hec.enabled. Default: true.
	LYNXDB_INGEST_SPLUNK_HEC_ENABLED = "LYNXDB_INGEST_SPLUNK_HEC_ENABLED"
	// LYNXDB_INGEST_SPLUNK_HEC_REQUIRE_TOKEN is the require valid Splunk token for HEC requests.
	// YAML key: ingest.splunk_hec.require_token. Default: false.
	LYNXDB_INGEST_SPLUNK_HEC_REQUIRE_TOKEN = "LYNXDB_INGEST_SPLUNK_HEC_REQUIRE_TOKEN"
	// LYNXDB_INGEST_STAGING_ENABLED is the coalesce small shipper batches before ingest.
	// YAML key: ingest.staging.enabled. Default: true.
	LYNXDB_INGEST_STAGING_ENABLED = "LYNXDB_INGEST_STAGING_ENABLED"
	// LYNXDB_INGEST_STAGING_FLUSH_BACKOFF_MAX is the max retry backoff for staged flush.
	// YAML key: ingest.staging.flush_backoff_max. Default: 5s.
	LYNXDB_INGEST_STAGING_FLUSH_BACKOFF_MAX = "LYNXDB_INGEST_STAGING_FLUSH_BACKOFF_MAX"
	// LYNXDB_INGEST_STAGING_FLUSH_RETRIES is the sink retries before dropping staged events.
	// YAML key: ingest.staging.flush_retries. Default: 3.
	LYNXDB_INGEST_STAGING_FLUSH_RETRIES = "LYNXDB_INGEST_STAGING_FLUSH_RETRIES"
	// LYNXDB_INGEST_STAGING_MAX_AGE is the flush oldest pending batch after this age.
	// YAML key: ingest.staging.max_age. Default: 5s.
	LYNXDB_INGEST_STAGING_MAX_AGE = "LYNXDB_INGEST_STAGING_MAX_AGE"
	// LYNXDB_INGEST_STAGING_MAX_BYTES is the flush or reject when pending bytes exceed this.
	// YAML key: ingest.staging.max_bytes. Default: 64mb.
	LYNXDB_INGEST_STAGING_MAX_BYTES = "LYNXDB_INGEST_STAGING_MAX_BYTES"
	// LYNXDB_INGEST_STAGING_MAX_INFLIGHT_EVENTS is the max pending events in staging.
	// YAML key: ingest.staging.max_inflight_events. Default: 1000000.
	LYNXDB_INGEST_STAGING_MAX_INFLIGHT_EVENTS = "LYNXDB_INGEST_STAGING_MAX_INFLIGHT_EVENTS"
	// LYNXDB_LISTEN is the listen address for the HTTP API server.
	// YAML key: listen. Default: localhost:3100.
	LYNXDB_LISTEN = "LYNXDB_LISTEN"
	// LYNXDB_LOG_LEVEL is the log level: debug, info, warn, error.
	// YAML key: log_level. Default: info.
	LYNXDB_LOG_LEVEL = "LYNXDB_LOG_LEVEL"
	// LYNXDB_NO_UI is the disable the built-in web UI.
	// YAML key: no_ui. Default: false.
	LYNXDB_NO_UI = "LYNXDB_NO_UI"
	// LYNXDB_QUERY_BITMAP_SELECTIVITY_THRESHOLD is the skip bitmap when this fraction of rows match.
	// YAML key: query.bitmap_selectivity_threshold. Default: 0.9.
	LYNXDB_QUERY_BITMAP_SELECTIVITY_THRESHOLD = "LYNXDB_QUERY_BITMAP_SELECTIVITY_THRESHOLD"
	// LYNXDB_QUERY_DEDUP_EXACT is the use exact string matching for dedup.
	// YAML key: query.dedup_exact. Default: false.
	LYNXDB_QUERY_DEDUP_EXACT = "LYNXDB_QUERY_DEDUP_EXACT"
	// LYNXDB_QUERY_DEFAULT_RESULT_LIMIT is the default result row limit.
	// YAML key: query.default_result_limit. Default: 1000.
	LYNXDB_QUERY_DEFAULT_RESULT_LIMIT = "LYNXDB_QUERY_DEFAULT_RESULT_LIMIT"
	// LYNXDB_QUERY_GLOBAL_QUERY_POOL_BYTES is the total query memory pool (0 = auto: 25% RAM).
	// YAML key: query.global_query_pool_bytes. Default: 0.
	LYNXDB_QUERY_GLOBAL_QUERY_POOL_BYTES = "LYNXDB_QUERY_GLOBAL_QUERY_POOL_BYTES"
	// LYNXDB_QUERY_JOB_GC_INTERVAL is the job garbage collection interval.
	// YAML key: query.job_gc_interval. Default: 30s.
	LYNXDB_QUERY_JOB_GC_INTERVAL = "LYNXDB_QUERY_JOB_GC_INTERVAL"
	// LYNXDB_QUERY_JOB_TTL is the completed job retention time.
	// YAML key: query.job_ttl. Default: 5m.
	LYNXDB_QUERY_JOB_TTL = "LYNXDB_QUERY_JOB_TTL"
	// LYNXDB_QUERY_MAX_BRANCH_PARALLELISM is the max goroutines for branch parallelism (0 = GOMAXPROCS).
	// YAML key: query.max_branch_parallelism. Default: 0.
	LYNXDB_QUERY_MAX_BRANCH_PARALLELISM = "LYNXDB_QUERY_MAX_BRANCH_PARALLELISM"
	// LYNXDB_QUERY_MAX_CONCURRENT is the max concurrent async queries.
	// YAML key: query.max_concurrent. Default: 10.
	LYNXDB_QUERY_MAX_CONCURRENT = "LYNXDB_QUERY_MAX_CONCURRENT"
	// LYNXDB_QUERY_MAX_QUERY_LENGTH is the max SPL2 query source length in bytes.
	// YAML key: query.max_query_length. Default: 1048576.
	LYNXDB_QUERY_MAX_QUERY_LENGTH = "LYNXDB_QUERY_MAX_QUERY_LENGTH"
	// LYNXDB_QUERY_MAX_QUERY_MEMORY_BYTES is the per-query memory budget.
	// YAML key: query.max_query_memory_bytes. Default: 1gb.
	LYNXDB_QUERY_MAX_QUERY_MEMORY_BYTES = "LYNXDB_QUERY_MAX_QUERY_MEMORY_BYTES"
	// LYNXDB_QUERY_MAX_QUERY_RUNTIME is the max query execution time (0 = unlimited).
	// YAML key: query.max_query_runtime. Default: 5m.
	LYNXDB_QUERY_MAX_QUERY_RUNTIME = "LYNXDB_QUERY_MAX_QUERY_RUNTIME"
	// LYNXDB_QUERY_MAX_RESULT_LIMIT is the maximum allowed result row limit.
	// YAML key: query.max_result_limit. Default: 50000.
	LYNXDB_QUERY_MAX_RESULT_LIMIT = "LYNXDB_QUERY_MAX_RESULT_LIMIT"
	// LYNXDB_QUERY_MAX_TEMP_DIR_SIZE_BYTES is the max disk space for spill files (0 = unlimited).
	// YAML key: query.max_temp_dir_size_bytes. Default: 10gb.
	LYNXDB_QUERY_MAX_TEMP_DIR_SIZE_BYTES = "LYNXDB_QUERY_MAX_TEMP_DIR_SIZE_BYTES"
	// LYNXDB_QUERY_PREVIEW_SIZE is the rows returned in async-job preview.
	// YAML key: query.preview_size. Default: 50.
	LYNXDB_QUERY_PREVIEW_SIZE = "LYNXDB_QUERY_PREVIEW_SIZE"
	// LYNXDB_QUERY_SLOW_QUERY_THRESHOLD_MS is the log slow queries above this threshold (0 = disabled).
	// YAML key: query.slow_query_threshold_ms. Default: 1000.
	LYNXDB_QUERY_SLOW_QUERY_THRESHOLD_MS = "LYNXDB_QUERY_SLOW_QUERY_THRESHOLD_MS"
	// LYNXDB_QUERY_SPILL_DIR is the temp dir for spill files (empty = os.TempDir).
	// YAML key: query.spill_dir. Default: "".
	LYNXDB_QUERY_SPILL_DIR = "LYNXDB_QUERY_SPILL_DIR"
	// LYNXDB_QUERY_SYNC_TIMEOUT is the max wait for synchronous query completion.
	// YAML key: query.sync_timeout. Default: 30s.
	LYNXDB_QUERY_SYNC_TIMEOUT = "LYNXDB_QUERY_SYNC_TIMEOUT"
	// LYNXDB_RETENTION is the global data retention period; events older than this are deleted.
	// YAML key: retention. Default: 7d.
	LYNXDB_RETENTION = "LYNXDB_RETENTION"
	// LYNXDB_SERVER_CACHE_RESERVE_PERCENT is the minimum cache floor as % of pool (0-50).
	// YAML key: server.cache_reserve_percent. Default: 20.
	LYNXDB_SERVER_CACHE_RESERVE_PERCENT = "LYNXDB_SERVER_CACHE_RESERVE_PERCENT"
	// LYNXDB_SERVER_TOTAL_MEMORY_POOL_BYTES is the total memory for queries + cache (0 = auto: 40% RAM).
	// YAML key: server.total_memory_pool_bytes. Default: 0.
	LYNXDB_SERVER_TOTAL_MEMORY_POOL_BYTES = "LYNXDB_SERVER_TOTAL_MEMORY_POOL_BYTES"
	// LYNXDB_STORAGE_CACHE_MAX_BYTES is the query result cache memory limit.
	// YAML key: storage.cache_max_bytes. Default: 1gb.
	LYNXDB_STORAGE_CACHE_MAX_BYTES = "LYNXDB_STORAGE_CACHE_MAX_BYTES"
	// LYNXDB_STORAGE_CACHE_TTL is the query result cache TTL.
	// YAML key: storage.cache_ttl. Default: 5m.
	LYNXDB_STORAGE_CACHE_TTL = "LYNXDB_STORAGE_CACHE_TTL"
	// LYNXDB_STORAGE_COMPACTION_INTERVAL is the compaction check interval.
	// YAML key: storage.compaction_interval. Default: 30s.
	LYNXDB_STORAGE_COMPACTION_INTERVAL = "LYNXDB_STORAGE_COMPACTION_INTERVAL"
	// LYNXDB_STORAGE_COMPACTION_RATE_LIMIT_MB is the compaction disk bandwidth limit in MB/s.
	// YAML key: storage.compaction_rate_limit_mb. Default: 100.
	LYNXDB_STORAGE_COMPACTION_RATE_LIMIT_MB = "LYNXDB_STORAGE_COMPACTION_RATE_LIMIT_MB"
	// LYNXDB_STORAGE_COMPACTION_DISABLE_BSI_ON_OUTPUT disables range BSI emission from compaction output.
	// YAML key: storage.compaction_disable_bsi_on_output. Default: false.
	LYNXDB_STORAGE_COMPACTION_DISABLE_BSI_ON_OUTPUT = "LYNXDB_STORAGE_COMPACTION_DISABLE_BSI_ON_OUTPUT"
	// LYNXDB_STORAGE_COMPACTION_WORKERS is the parallel compaction workers.
	// YAML key: storage.compaction_workers. Default: 2.
	LYNXDB_STORAGE_COMPACTION_WORKERS = "LYNXDB_STORAGE_COMPACTION_WORKERS"
	// LYNXDB_STORAGE_COMPRESSION is the block compression: lz4 or zstd.
	// YAML key: storage.compression. Default: lz4.
	LYNXDB_STORAGE_COMPRESSION = "LYNXDB_STORAGE_COMPRESSION"
	// LYNXDB_STORAGE_FLUSH_IDLE_TIMEOUT is the flush batcher after this idle period (0 = disabled).
	// YAML key: storage.flush_idle_timeout. Default: 30s.
	LYNXDB_STORAGE_FLUSH_IDLE_TIMEOUT = "LYNXDB_STORAGE_FLUSH_IDLE_TIMEOUT"
	// LYNXDB_STORAGE_FLUSH_THRESHOLD is the batcher flush threshold.
	// YAML key: storage.flush_threshold. Default: 512mb.
	LYNXDB_STORAGE_FLUSH_THRESHOLD = "LYNXDB_STORAGE_FLUSH_THRESHOLD"
	// LYNXDB_STORAGE_L0_THRESHOLD is the L0 to L1 compaction trigger.
	// YAML key: storage.l0_threshold. Default: 4.
	LYNXDB_STORAGE_L0_THRESHOLD = "LYNXDB_STORAGE_L0_THRESHOLD"
	// LYNXDB_STORAGE_L1_THRESHOLD is the L1 to L2 compaction trigger.
	// YAML key: storage.l1_threshold. Default: 4.
	LYNXDB_STORAGE_L1_THRESHOLD = "LYNXDB_STORAGE_L1_THRESHOLD"
	// LYNXDB_STORAGE_L2_TARGET_SIZE is the target size for L2 segments.
	// YAML key: storage.l2_target_size. Default: 1gb.
	LYNXDB_STORAGE_L2_TARGET_SIZE = "LYNXDB_STORAGE_L2_TARGET_SIZE"
	// LYNXDB_STORAGE_MAX_COLUMNS_PER_PART is the max columns per written part.
	// YAML key: storage.max_columns_per_part. Default: 256.
	LYNXDB_STORAGE_MAX_COLUMNS_PER_PART = "LYNXDB_STORAGE_MAX_COLUMNS_PER_PART"
	// LYNXDB_STORAGE_PARTITION_BY is the time-based partition granularity (daily, hourly).
	// YAML key: storage.partition_by. Default: daily.
	LYNXDB_STORAGE_PARTITION_BY = "LYNXDB_STORAGE_PARTITION_BY"
	// LYNXDB_STORAGE_REMOTE_FETCH_TIMEOUT is the timeout for fetching remote segments.
	// YAML key: storage.remote_fetch_timeout. Default: 30s.
	LYNXDB_STORAGE_REMOTE_FETCH_TIMEOUT = "LYNXDB_STORAGE_REMOTE_FETCH_TIMEOUT"
	// LYNXDB_STORAGE_ROW_GROUP_SIZE is the rows per row group in segments.
	// YAML key: storage.row_group_size. Default: 65536.
	LYNXDB_STORAGE_ROW_GROUP_SIZE = "LYNXDB_STORAGE_ROW_GROUP_SIZE"
	// LYNXDB_STORAGE_S3_BUCKET is the S3 bucket for warm/cold storage.
	// YAML key: storage.s3_bucket. Default: "".
	LYNXDB_STORAGE_S3_BUCKET = "LYNXDB_STORAGE_S3_BUCKET"
	// LYNXDB_STORAGE_S3_ENDPOINT is the S3-compatible endpoint (for MinIO, etc.).
	// YAML key: storage.s3_endpoint. Default: "".
	LYNXDB_STORAGE_S3_ENDPOINT = "LYNXDB_STORAGE_S3_ENDPOINT"
	// LYNXDB_STORAGE_S3_FORCE_PATH_STYLE is the use path-style addressing for S3.
	// YAML key: storage.s3_force_path_style. Default: false.
	LYNXDB_STORAGE_S3_FORCE_PATH_STYLE = "LYNXDB_STORAGE_S3_FORCE_PATH_STYLE"
	// LYNXDB_STORAGE_S3_PREFIX is the key prefix in S3.
	// YAML key: storage.s3_prefix. Default: lynxdb/.
	LYNXDB_STORAGE_S3_PREFIX = "LYNXDB_STORAGE_S3_PREFIX"
	// LYNXDB_STORAGE_S3_REGION is the AWS region for S3.
	// YAML key: storage.s3_region. Default: us-east-1.
	LYNXDB_STORAGE_S3_REGION = "LYNXDB_STORAGE_S3_REGION"
	// LYNXDB_STORAGE_SEGMENT_CACHE_SIZE is the local disk cache for remote segments.
	// YAML key: storage.segment_cache_size. Default: 10gb.
	LYNXDB_STORAGE_SEGMENT_CACHE_SIZE = "LYNXDB_STORAGE_SEGMENT_CACHE_SIZE"
	// LYNXDB_STORAGE_TIERING_INTERVAL is the tier evaluation interval.
	// YAML key: storage.tiering_interval. Default: 5m.
	LYNXDB_STORAGE_TIERING_INTERVAL = "LYNXDB_STORAGE_TIERING_INTERVAL"
	// LYNXDB_STORAGE_TIERING_PARALLELISM is the concurrent tier uploads.
	// YAML key: storage.tiering_parallelism. Default: 3.
	LYNXDB_STORAGE_TIERING_PARALLELISM = "LYNXDB_STORAGE_TIERING_PARALLELISM"
	// LYNXDB_SYSLOG_BATCH_SIZE is the flush after this many events.
	// YAML key: syslog.batch_size. Default: 1000.
	LYNXDB_SYSLOG_BATCH_SIZE = "LYNXDB_SYSLOG_BATCH_SIZE"
	// LYNXDB_SYSLOG_BATCH_TIMEOUT is the flush after this much idle time.
	// YAML key: syslog.batch_timeout. Default: 200ms.
	LYNXDB_SYSLOG_BATCH_TIMEOUT = "LYNXDB_SYSLOG_BATCH_TIMEOUT"
	// LYNXDB_SYSLOG_DEFAULT_HOSTNAME is the hostname used when wire value is missing.
	// YAML key: syslog.default_hostname. Default: "".
	LYNXDB_SYSLOG_DEFAULT_HOSTNAME = "LYNXDB_SYSLOG_DEFAULT_HOSTNAME"
	// LYNXDB_SYSLOG_DEFAULT_TIMEZONE is the timezone for RFC 3164 timestamps.
	// YAML key: syslog.default_timezone. Default: Local.
	LYNXDB_SYSLOG_DEFAULT_TIMEZONE = "LYNXDB_SYSLOG_DEFAULT_TIMEZONE"
	// LYNXDB_SYSLOG_FRAMING is the syslog framing: auto, octet-counting, non-transparent.
	// YAML key: syslog.framing. Default: auto.
	LYNXDB_SYSLOG_FRAMING = "LYNXDB_SYSLOG_FRAMING"
	// LYNXDB_SYSLOG_INDEX is the target index for syslog events.
	// YAML key: syslog.index. Default: main.
	LYNXDB_SYSLOG_INDEX = "LYNXDB_SYSLOG_INDEX"
	// LYNXDB_SYSLOG_MAX_MESSAGE_BYTES is the hard cap for a single syslog message.
	// YAML key: syslog.max_message_bytes. Default: 65536.
	LYNXDB_SYSLOG_MAX_MESSAGE_BYTES = "LYNXDB_SYSLOG_MAX_MESSAGE_BYTES"
	// LYNXDB_SYSLOG_PARSER is the syslog parser: auto, rfc5424, rfc3164, raw.
	// YAML key: syslog.parser. Default: auto.
	LYNXDB_SYSLOG_PARSER = "LYNXDB_SYSLOG_PARSER"
	// LYNXDB_SYSLOG_SOURCETYPE is the base sourcetype, suffixed by parsed dialect.
	// YAML key: syslog.sourcetype. Default: syslog.
	LYNXDB_SYSLOG_SOURCETYPE = "LYNXDB_SYSLOG_SOURCETYPE"
	// LYNXDB_SYSLOG_TCP is the TCP syslog listen address (empty = disabled).
	// YAML key: syslog.tcp. Default: "".
	LYNXDB_SYSLOG_TCP = "LYNXDB_SYSLOG_TCP"
	// LYNXDB_SYSLOG_TCP_IDLE_TIMEOUT is the TCP connection idle timeout.
	// YAML key: syslog.tcp_idle_timeout. Default: 5m.
	LYNXDB_SYSLOG_TCP_IDLE_TIMEOUT = "LYNXDB_SYSLOG_TCP_IDLE_TIMEOUT"
	// LYNXDB_SYSLOG_TCP_MAX_CONNECTIONS is the max concurrent TCP syslog connections.
	// YAML key: syslog.tcp_max_connections. Default: 1000.
	LYNXDB_SYSLOG_TCP_MAX_CONNECTIONS = "LYNXDB_SYSLOG_TCP_MAX_CONNECTIONS"
	// LYNXDB_SYSLOG_TLS is the wrap TCP syslog with server TLS config.
	// YAML key: syslog.tls. Default: false.
	LYNXDB_SYSLOG_TLS = "LYNXDB_SYSLOG_TLS"
	// LYNXDB_SYSLOG_TRAILER is the syslog trailer: auto, lf, nul, crlf.
	// YAML key: syslog.trailer. Default: auto.
	LYNXDB_SYSLOG_TRAILER = "LYNXDB_SYSLOG_TRAILER"
	// LYNXDB_SYSLOG_UDP is the UDP syslog listen address (empty = disabled).
	// YAML key: syslog.udp. Default: "".
	LYNXDB_SYSLOG_UDP = "LYNXDB_SYSLOG_UDP"
	// LYNXDB_SYSLOG_UDP_READ_BUFFER is the UDP socket receive buffer.
	// YAML key: syslog.udp_read_buffer. Default: 2mb.
	LYNXDB_SYSLOG_UDP_READ_BUFFER = "LYNXDB_SYSLOG_UDP_READ_BUFFER"
	// LYNXDB_SYSLOG_USE_PEER_AS_SOURCE is the set source to udp://peer or tcp://peer.
	// YAML key: syslog.use_peer_as_source. Default: true.
	LYNXDB_SYSLOG_USE_PEER_AS_SOURCE = "LYNXDB_SYSLOG_USE_PEER_AS_SOURCE"
	// LYNXDB_TAIL_MAX_CONCURRENT_SESSIONS is the max concurrent tail sessions (0 = unlimited).
	// YAML key: tail.max_concurrent_sessions. Default: 100.
	LYNXDB_TAIL_MAX_CONCURRENT_SESSIONS = "LYNXDB_TAIL_MAX_CONCURRENT_SESSIONS"
	// LYNXDB_TAIL_MAX_SESSION_DURATION is the max duration per tail session (0 = unlimited).
	// YAML key: tail.max_session_duration. Default: 24h.
	LYNXDB_TAIL_MAX_SESSION_DURATION = "LYNXDB_TAIL_MAX_SESSION_DURATION"
	// LYNXDB_TLS_CERT_FILE is the path to PEM certificate file.
	// YAML key: tls.cert_file. Default: "".
	LYNXDB_TLS_CERT_FILE = "LYNXDB_TLS_CERT_FILE"
	// LYNXDB_TLS_ENABLED is the enable TLS (auto self-signed if no cert/key).
	// YAML key: tls.enabled. Default: false.
	LYNXDB_TLS_ENABLED = "LYNXDB_TLS_ENABLED"
	// LYNXDB_TLS_KEY_FILE is the path to PEM private key file.
	// YAML key: tls.key_file. Default: "".
	LYNXDB_TLS_KEY_FILE = "LYNXDB_TLS_KEY_FILE"
	// LYNXDB_VIEWS_BACKFILL_BACKPRESSURE_WAIT is the wait time when pool is under pressure.
	// YAML key: views.backfill_backpressure_wait. Default: 5s.
	LYNXDB_VIEWS_BACKFILL_BACKPRESSURE_WAIT = "LYNXDB_VIEWS_BACKFILL_BACKPRESSURE_WAIT"
	// LYNXDB_VIEWS_BACKFILL_MAX_RETRIES is the max backpressure retries before failing.
	// YAML key: views.backfill_max_retries. Default: 60.
	LYNXDB_VIEWS_BACKFILL_MAX_RETRIES = "LYNXDB_VIEWS_BACKFILL_MAX_RETRIES"
	// LYNXDB_VIEWS_BACKFILL_TIMEOUT is the max time per backfill before aborting.
	// YAML key: views.backfill_timeout. Default: 4h.
	LYNXDB_VIEWS_BACKFILL_TIMEOUT = "LYNXDB_VIEWS_BACKFILL_TIMEOUT"
	// LYNXDB_VIEWS_DISPATCH_BATCH_DELAY is the delay between dispatch batches.
	// YAML key: views.dispatch_batch_delay. Default: 100ms.
	LYNXDB_VIEWS_DISPATCH_BATCH_DELAY = "LYNXDB_VIEWS_DISPATCH_BATCH_DELAY"
	// LYNXDB_VIEWS_DISPATCH_BATCH_SIZE is the rows per dispatch batch.
	// YAML key: views.dispatch_batch_size. Default: 1000.
	LYNXDB_VIEWS_DISPATCH_BATCH_SIZE = "LYNXDB_VIEWS_DISPATCH_BATCH_SIZE"
	// LYNXDB_VIEWS_INSERT_MAX_MEMORY_BYTES is the memory budget per insert-time MV batch (0 = auto).
	// YAML key: views.insert_max_memory_bytes. Default: 0.
	LYNXDB_VIEWS_INSERT_MAX_MEMORY_BYTES = "LYNXDB_VIEWS_INSERT_MAX_MEMORY_BYTES"
	// LYNXDB_VIEWS_MAX_BACKFILL_MEMORY_BYTES is the memory budget per backfill (0 = auto).
	// YAML key: views.max_backfill_memory_bytes. Default: 0.
	LYNXDB_VIEWS_MAX_BACKFILL_MEMORY_BYTES = "LYNXDB_VIEWS_MAX_BACKFILL_MEMORY_BYTES"
)
