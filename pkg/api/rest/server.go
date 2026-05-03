package rest

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lynxbase/lynxdb/internal/webui"
	"github.com/lynxbase/lynxdb/pkg/auth"
	"github.com/lynxbase/lynxdb/pkg/config"
	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/ingest/limits"
	"github.com/lynxbase/lynxdb/pkg/ingest/receiver/eshttp"
	"github.com/lynxbase/lynxdb/pkg/ingest/receiver/otlphttp"
	syslogrecv "github.com/lynxbase/lynxdb/pkg/ingest/receiver/syslog"
	"github.com/lynxbase/lynxdb/pkg/ingest/staging"
	"github.com/lynxbase/lynxdb/pkg/memgov"
	"github.com/lynxbase/lynxdb/pkg/planner"
	"github.com/lynxbase/lynxdb/pkg/server"
	"github.com/lynxbase/lynxdb/pkg/spl2"
	"github.com/lynxbase/lynxdb/pkg/storage/savedqueries"
	"github.com/lynxbase/lynxdb/pkg/usecases"
)

// Server is the main LynxDB API server.
type Server struct {
	engine             *server.Engine
	keyStore           *auth.KeyStore // Nil when auth is disabled.
	queryService       *usecases.QueryService
	viewService        *usecases.ViewService
	tailService        *usecases.TailService
	queryStore         *savedqueries.Store
	runtimeCfg         *config.Config
	cfgMu              sync.RWMutex
	httpServer         *http.Server
	listenAddr         atomic.Value  // stores resolved listen address (string)
	ready              chan struct{} // closed when server is ready to accept requests
	queryCfg           config.QueryConfig
	ingestCfg          config.IngestConfig
	shutdownTimeout    time.Duration
	rateLimiter        *RateLimiter // nil if rate limiting is disabled
	tailCfg            config.TailConfig
	activeTailSessions atomic.Int64 // current number of active tail SSE sessions
	degraded           atomic.Bool  // true when a persistent store fell back to in-memory
	tlsConfig          *tls.Config  // non-nil when TLS is enabled
	syslogReceiver     *syslogrecv.Receiver
	otlpHTTPReceiver   *otlphttp.Receiver
	stagingBuffer      *staging.Buffer
	esHandshake        *eshttp.Handshake
	esStubs            *eshttp.Stubs
	promMetrics        *PrometheusMetrics
	levelVar           *slog.LevelVar
	logger             *slog.Logger
}

// Config configures the API server.
type Config struct {
	Addr          string
	DataDir       string         // Root directory for all data (segments, parts, indexes). Empty = in-memory only.
	Retention     time.Duration  // Data retention period. 0 = use default (90 days).
	NoUI          bool           // When true, the embedded Web UI is not served.
	RuntimeConfig *config.Config // Optional full runtime snapshot used by reload/get-config paths.

	KeyStore      *auth.KeyStore
	TLSConfig     *tls.Config // If non-nil, server listens with TLS.
	Storage       config.StorageConfig
	Logger        *slog.Logger
	LevelVar      *slog.LevelVar // Optional. When set, ReloadConfig adjusts the log level through it.
	Query         config.QueryConfig
	Ingest        config.IngestConfig
	HTTP          config.HTTPConfig
	Syslog        config.SyslogConfig
	Tail          config.TailConfig
	Server        config.ServerConfig
	Views         config.ViewsConfig
	BufferManager config.BufferManagerConfig
	Cluster       config.ClusterConfig
}

// NewServer creates a new LynxDB API server.
func NewServer(cfg Config) (*Server, error) {
	engine := server.NewEngine(server.Config{
		DataDir:       cfg.DataDir,
		Retention:     cfg.Retention,
		Storage:       cfg.Storage,
		Logger:        cfg.Logger,
		Query:         cfg.Query,
		Server:        cfg.Server,
		Views:         cfg.Views,
		BufferManager: cfg.BufferManager,
		Cluster:       cfg.Cluster,
	})

	// Build planner, query service, and view service.
	p := planner.New(planner.WithViewCatalog(engine))
	queryService := usecases.NewQueryService(p, engine, cfg.Query)
	viewService := usecases.NewViewService(engine)
	tailService := usecases.NewTailService(p, engine)

	// Initialize saved queries store.
	var qStore *savedqueries.Store
	var storeDegraded bool
	var err error
	if cfg.DataDir != "" {
		qStore, err = savedqueries.OpenStore(cfg.DataDir)
		if err != nil {
			cfg.Logger.Warn("[DATA LOSS RISK] failed to open saved queries store, falling back to in-memory", "error", err)
			qStore = savedqueries.OpenInMemory()
			storeDegraded = true
		}
	} else {
		qStore = savedqueries.OpenInMemory()
	}

	shutdownTimeout := cfg.HTTP.ShutdownTimeout
	shutdownTimeout = defaultShutdownTimeout(shutdownTimeout)

	// Build the runtime config snapshot used by GET/PATCH /config and reload
	// diffing. Prefer the fully loaded config when the caller has it.
	runtimeCfg := config.DefaultConfig()
	if cfg.RuntimeConfig != nil {
		snapshot := *cfg.RuntimeConfig
		runtimeCfg = &snapshot
	} else {
		runtimeCfg.Listen = cfg.Addr
		runtimeCfg.DataDir = cfg.DataDir
		runtimeCfg.Retention = config.Duration(cfg.Retention)
		runtimeCfg.NoUI = cfg.NoUI
		if cfg.Query.SyncTimeout > 0 {
			runtimeCfg.Query = cfg.Query
		}
		if cfg.Ingest.MaxBodySize > 0 {
			runtimeCfg.Ingest = cfg.Ingest
		}
		if cfg.HTTP.IdleTimeout > 0 {
			runtimeCfg.HTTP = cfg.HTTP
		}
		runtimeCfg.Syslog = cfg.Syslog
	}

	s := &Server{
		engine:          engine,
		keyStore:        cfg.KeyStore,
		queryService:    queryService,
		viewService:     viewService,
		tailService:     tailService,
		queryStore:      qStore,
		runtimeCfg:      runtimeCfg,
		ready:           make(chan struct{}),
		queryCfg:        cfg.Query,
		ingestCfg:       cfg.Ingest,
		shutdownTimeout: shutdownTimeout,
		tailCfg:         cfg.Tail,
		tlsConfig:       cfg.TLSConfig,
		levelVar:        cfg.LevelVar,
		logger:          cfg.Logger,
	}
	if storeDegraded {
		s.degraded.Store(true)
	}

	// Register Prometheus metrics and wire the OnQueryComplete hook so that
	// every completed query records histogram observations (duration, scan,
	// pipeline, memory, rows) and increments segment-skip counters.
	promMetrics := NewPrometheusMetrics()
	s.promMetrics = promMetrics
	engine.SetOnQueryComplete(promMetrics.RecordQuery)

	if cfg.Syslog.Enabled() {
		tlsCfg := cfg.TLSConfig
		if !cfg.Syslog.TLS {
			tlsCfg = nil
		}
		syslogReceiver, err := syslogrecv.New(
			cfg.Syslog,
			engineSink{engine: engine},
			ingestPipelineForConfig(cfg.Ingest),
			tlsCfg,
			cfg.Logger,
			syslogrecv.NewMetrics(promMetrics.Registry()),
		)
		if err != nil {
			return nil, fmt.Errorf("syslog receiver: %w", err)
		}
		s.syslogReceiver = syslogReceiver
	}

	if cfg.Ingest.OTLP.HTTPListen != "" {
		otlpReceiver, err := otlphttp.New(
			otlphttp.Config{
				Listen: cfg.Ingest.OTLP.HTTPListen,
				Limits: limits.Config{
					MaxCompressedBytes:   int64(cfg.Ingest.Limits.MaxCompressedBodyBytes),
					MaxDecompressedBytes: int64(cfg.Ingest.Limits.MaxDecompressedBodyBytes),
				},
			},
			func(ctx context.Context, events []*event.Event) error {
				processed, err := ingestPipelineForConfig(s.currentIngestConfig()).Process(events)
				if err != nil {
					return err
				}
				return s.submitShipperEvents(ctx, processed)
			},
			cfg.Logger,
			promMetrics,
		)
		if err != nil {
			return nil, fmt.Errorf("otlp http receiver: %w", err)
		}
		s.otlpHTTPReceiver = otlpReceiver
	}

	esCfg := normalizedESCompatConfig(cfg.Ingest)
	esHandshake, err := eshttp.NewHandshake(eshttp.Config{
		AdvertisedVersion: esCfg.AdvertisedVersion,
		ClusterName:       esCfg.ClusterName,
		DataDir:           cfg.DataDir,
		S3Bucket:          cfg.Storage.S3Bucket,
	})
	if err != nil {
		return nil, err
	}
	s.esHandshake = esHandshake
	esStubs, err := eshttp.NewStubs(eshttp.Config{
		AdvertisedVersion: esCfg.AdvertisedVersion,
		ClusterName:       esCfg.ClusterName,
		DataDir:           cfg.DataDir,
		S3Bucket:          cfg.Storage.S3Bucket,
	})
	if err != nil {
		return nil, err
	}
	s.esStubs = esStubs

	mux := http.NewServeMux()

	// Prometheus metrics endpoint (standard /metrics path).
	// Wraps the handler to refresh storage metrics on each scrape.
	promHandler := promMetrics.Handler()
	mux.Handle("GET /metrics", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.engine != nil {
			promMetrics.RecordStorageMetrics(s.engine.Metrics())
			promMetrics.RecordGovernorStats(s.engine.GovernorStats())
			spillFiles, spillBytes := s.engine.SpillStats()
			promMetrics.RecordSpillStats(spillFiles, spillBytes)
			promMetrics.RecordRevocationStats(s.engine.RevocationFreedSpillableBytes())
		}
		promHandler.ServeHTTP(w, r)
	}))

	// pprof debug endpoints for live CPU/memory profiling.
	// Authenticated via KeyAuthMiddleware (under /api/v1/ prefix).
	mux.HandleFunc("GET /api/v1/debug/pprof/", pprof.Index)
	mux.HandleFunc("GET /api/v1/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("GET /api/v1/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("GET /api/v1/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("GET /api/v1/debug/pprof/trace", pprof.Trace)
	mux.HandleFunc("GET /api/v1/debug/pprof/heap", pprof.Handler("heap").ServeHTTP)
	mux.HandleFunc("GET /api/v1/debug/pprof/allocs", pprof.Handler("allocs").ServeHTTP)
	mux.HandleFunc("GET /api/v1/debug/pprof/goroutine", pprof.Handler("goroutine").ServeHTTP)
	mux.HandleFunc("GET /api/v1/debug/pprof/mutex", pprof.Handler("mutex").ServeHTTP)
	mux.HandleFunc("GET /api/v1/debug/pprof/block", pprof.Handler("block").ServeHTTP)
	mux.HandleFunc("GET /api/v1/debug/pprof/threadcreate", pprof.Handler("threadcreate").ServeHTTP)

	// Query endpoint (three-mode: sync/hybrid/async).
	mux.HandleFunc("POST /api/v1/query", s.handleQuery)
	mux.HandleFunc("GET /api/v1/query", s.handleQueryGet)
	mux.HandleFunc("POST /api/v1/query/stream", s.handleQueryStream)
	mux.HandleFunc("GET /api/v1/query/explain", s.handleQueryExplain)

	// Job management (for async/hybrid jobs).
	mux.HandleFunc("GET /api/v1/query/jobs/{id}", s.handleGetJob)
	mux.HandleFunc("GET /api/v1/query/jobs/{id}/stream", s.handleJobStream)
	mux.HandleFunc("DELETE /api/v1/query/jobs/{id}", s.handleCancelJob)
	mux.HandleFunc("GET /api/v1/query/jobs", s.handleListJobs)

	// Index management.
	mux.HandleFunc("GET /api/v1/indexes", s.handleListIndexes)
	mux.HandleFunc("POST /api/v1/indexes", s.handleCreateIndex)

	// Cluster status.
	mux.HandleFunc("GET /api/v1/cluster/status", s.handleClusterStatus)

	// Ingest endpoints (new paths).
	mux.HandleFunc("POST /api/v1/ingest", s.handleIngestEvents)
	mux.HandleFunc("POST /api/v1/ingest/raw", s.handleIngestRaw)
	mux.HandleFunc("POST /api/v1/ingest/hec", s.handleIngestHEC)
	mux.HandleFunc("POST /api/v1/ingest/bulk", s.handleESBulk)
	// Field catalog.
	mux.HandleFunc("GET /api/v1/fields", s.handleListFields)

	// Server stats.
	mux.HandleFunc("GET /api/v1/stats", s.handleStats)

	// Cache management.
	mux.HandleFunc("DELETE /api/v1/cache", s.handleCacheClear)
	mux.HandleFunc("GET /api/v1/cache/stats", s.handleCacheStats)

	// Storage metrics.
	mux.HandleFunc("GET /api/v1/metrics", s.handleMetrics)

	// Compaction history.
	mux.HandleFunc("GET /api/v1/compaction/history", s.handleCompactionHistory)

	// Saved queries (generic CRUD).
	registerCRUD(mux, "/api/v1/queries", CRUDOpts[savedqueries.SavedQuery, *savedqueries.SavedQueryInput]{
		Store:       s.queryStore,
		ConflictErr: savedqueries.ErrAlreadyExists,
		ServerRef:   s,
		NewEntity: func(input *savedqueries.SavedQueryInput) *savedqueries.SavedQuery {
			return input.ToSavedQuery()
		},
		MergeEntity: func(existing *savedqueries.SavedQuery, input *savedqueries.SavedQueryInput) *savedqueries.SavedQuery {
			existing.Name = input.Name
			if input.Q != "" {
				existing.Q = input.Q
			} else if input.Query != "" {
				existing.Q = input.Query
			}
			existing.From = input.From
			existing.UpdatedAt = time.Now()

			return existing
		},
	})

	// Config API.
	mux.HandleFunc("GET /api/v1/config", s.handleGetConfig)
	mux.HandleFunc("PATCH /api/v1/config", s.handlePatchConfig)

	// Histogram.
	mux.HandleFunc("GET /api/v1/histogram", s.handleHistogram)

	// Schema: field values and sources.
	mux.HandleFunc("GET /api/v1/fields/{name}/values", s.handleFieldValues)
	mux.HandleFunc("GET /api/v1/sources", s.handleListSources)

	// Views (swagger paths, aliasing existing MV handlers).
	mux.HandleFunc("GET /api/v1/views", s.handleListMV)
	mux.HandleFunc("POST /api/v1/views", s.handleCreateMV)
	mux.HandleFunc("GET /api/v1/views/{name}", s.handleGetMV)
	mux.HandleFunc("PATCH /api/v1/views/{name}", s.handlePatchView)
	mux.HandleFunc("DELETE /api/v1/views/{name}", s.handleDeleteMV)
	mux.HandleFunc("GET /api/v1/views/{name}/backfill", s.handleViewBackfill)
	mux.HandleFunc("POST /api/v1/views/{name}/backfill", s.handleViewBackfill)

	// Elasticsearch compatibility.
	mux.Handle("GET /{$}", s.esCompatibilityHandler("root", esHandshake))
	mux.Handle("GET /_xpack", s.esCompatibilityHandler("xpack", http.HandlerFunc(esStubs.XPackInfo)))
	mux.Handle("GET /_xpack/license", s.esCompatibilityHandler("license", http.HandlerFunc(esStubs.XPackLicense)))
	mux.Handle("GET /_license", s.esCompatibilityHandler("license", http.HandlerFunc(esStubs.XPackLicense)))
	mux.Handle("GET /_cat/templates", s.esCompatibilityHandler("templates", http.HandlerFunc(esStubs.EmptyArray)))
	mux.Handle("PUT /_index_template/{name...}", s.esCompatibilityHandler("template", http.HandlerFunc(esStubs.Acknowledged)))
	mux.Handle("GET /_index_template/{name...}", s.esCompatibilityHandler("template", http.HandlerFunc(esStubs.IndexTemplates)))
	mux.Handle("GET /_ilm/policy/{name...}", s.esCompatibilityHandler("ilm", http.HandlerFunc(esStubs.NotFound)))
	mux.Handle("GET /_ingest/pipeline/{name...}", s.esCompatibilityHandler("pipeline", http.HandlerFunc(esStubs.NotFound)))
	mux.Handle("GET /_nodes/{path...}", s.esCompatibilityHandler("nodes", http.HandlerFunc(esStubs.NodesHTTP)))
	mux.Handle("GET /_alias", s.esCompatibilityHandler("alias", http.HandlerFunc(esStubs.EmptyAliases)))
	mux.Handle("GET /_data_stream/{name...}", s.esCompatibilityHandler("datastream", http.HandlerFunc(esStubs.EmptyDataStreams)))
	mux.HandleFunc("POST /_bulk", s.handleESBulk)
	mux.HandleFunc("POST /{index}/_bulk", s.handleESBulk)
	mux.HandleFunc("POST /api/v1/es/_bulk", s.handleESBulk)
	mux.HandleFunc("POST /api/v1/es/{index}/_doc", s.handleESIndexDoc)
	mux.HandleFunc("GET /api/v1/es/", s.handleESClusterInfo)
	mux.HandleFunc("GET /api/v1/es", s.handleESClusterInfo)

	// Backward-compatible ES stub aliases under the legacy API prefix.
	esStub := s.handleESStub
	mux.Handle("GET /api/v1/es/_xpack", s.esCompatibilityHandler("xpack", http.HandlerFunc(esStubs.XPackInfo)))
	mux.Handle("GET /api/v1/es/_xpack/license", s.esCompatibilityHandler("license", http.HandlerFunc(esStubs.XPackLicense)))
	mux.Handle("GET /api/v1/es/_license", s.esCompatibilityHandler("license", http.HandlerFunc(esStubs.XPackLicense)))
	mux.Handle("GET /api/v1/es/_cat/templates", s.esCompatibilityHandler("templates", http.HandlerFunc(esStubs.EmptyArray)))
	mux.Handle("GET /api/v1/es/_ilm/policy/{name...}", s.esCompatibilityHandler("ilm", http.HandlerFunc(esStubs.NotFound)))
	mux.HandleFunc("PUT /api/v1/es/_ilm/policy/{name...}", esStub)
	mux.Handle("GET /api/v1/es/_index_template/{name...}", s.esCompatibilityHandler("template", http.HandlerFunc(esStubs.IndexTemplates)))
	mux.Handle("PUT /api/v1/es/_index_template/{name...}", s.esCompatibilityHandler("template", http.HandlerFunc(esStubs.Acknowledged)))
	mux.Handle("GET /api/v1/es/_ingest/pipeline/{name...}", s.esCompatibilityHandler("pipeline", http.HandlerFunc(esStubs.NotFound)))
	mux.HandleFunc("PUT /api/v1/es/_ingest/pipeline/{name...}", esStub)
	mux.Handle("GET /api/v1/es/_nodes/{path...}", s.esCompatibilityHandler("nodes", http.HandlerFunc(esStubs.NodesHTTP)))
	mux.Handle("GET /api/v1/es/_data_stream/{name...}", s.esCompatibilityHandler("datastream", http.HandlerFunc(esStubs.EmptyDataStreams)))
	mux.Handle("GET /api/v1/es/_alias", s.esCompatibilityHandler("alias", http.HandlerFunc(esStubs.EmptyAliases)))
	// PUT/HEAD /{index} must be registered after underscore-prefixed paths
	// to avoid Go 1.22+ ServeMux wildcard-vs-specific conflicts.
	mux.HandleFunc("PUT /api/v1/es/{index}", esStub)

	// OTLP HTTP Logs ingestion.
	mux.HandleFunc("POST /api/v1/otlp/v1/logs", s.handleOTLPLogs)

	// Live tail (SSE).
	mux.HandleFunc("GET /api/v1/tail", s.handleTail)

	// Unified status.
	mux.HandleFunc("GET /api/v1/status", s.handleStatus)

	// Auth management — only registered when auth is enabled, 404 otherwise.
	if cfg.KeyStore != nil {
		mux.HandleFunc("POST /api/v1/auth/keys", s.handleCreateKey)
		mux.HandleFunc("GET /api/v1/auth/keys", s.handleListKeys)
		mux.HandleFunc("DELETE /api/v1/auth/keys/{id}", s.handleRevokeKey)
		mux.HandleFunc("POST /api/v1/auth/rotate-root", s.handleRotateRoot)
	} else {
		disabled := authDisabledHandler()
		mux.HandleFunc("POST /api/v1/auth/keys", disabled)
		mux.HandleFunc("GET /api/v1/auth/keys", disabled)
		mux.HandleFunc("DELETE /api/v1/auth/keys/{id}", disabled)
		mux.HandleFunc("POST /api/v1/auth/rotate-root", disabled)
	}

	// Health.
	mux.HandleFunc("GET /health", s.handleHealth)

	// Embedded Web UI (SPA fallback — registered after all API routes).
	if !cfg.NoUI && webui.Enabled() {
		mux.Handle("/", webui.Handler())
	}

	idleTimeout := cfg.HTTP.IdleTimeout
	idleTimeout = defaultIdleTimeout(idleTimeout)
	// Build middleware chain (execution order, outer → inner):
	// Recovery → RequestID → Logging → Auth → RateLimit → MaxBody → DualLimit → mux.
	// Auth runs before rate limiting so unauthenticated requests don't consume quota.
	var handler http.Handler = mux
	handler = limits.DualLimitMiddleware(limits.Config{
		MaxCompressedBytes:   int64(cfg.Ingest.Limits.MaxCompressedBodyBytes),
		MaxDecompressedBytes: int64(cfg.Ingest.Limits.MaxDecompressedBodyBytes),
	}, promMetrics)(handler)
	handler = MaxBodyMiddleware(int64(cfg.Ingest.MaxBodySize), handler)
	if cfg.HTTP.RateLimit > 0 {
		s.rateLimiter = NewRateLimiter(cfg.HTTP.RateLimit, int(cfg.HTTP.RateLimit)*2)
		handler = RateLimitMiddleware(s.rateLimiter, handler)
	}
	handler = KeyAuthMiddleware(cfg.KeyStore, handler)
	handler = LoggingMiddleware(cfg.Logger, handler)
	handler = RequestIDMiddleware(handler)
	handler = RecoveryMiddleware(cfg.Logger, handler)

	readHeaderTimeout := cfg.HTTP.ReadHeaderTimeout
	readHeaderTimeout = defaultReadHeaderTimeout(readHeaderTimeout)

	s.httpServer = &http.Server{
		Addr:              cfg.Addr,
		Handler:           handler,
		IdleTimeout:       idleTimeout,
		ReadHeaderTimeout: readHeaderTimeout,
	}

	return s, nil
}

func normalizedESCompatConfig(ingest config.IngestConfig) config.ESCompatConfig {
	defaults := config.DefaultConfig().Ingest.ESCompat
	out := ingest.ESCompat
	if out.AdvertisedVersion == "" {
		out.AdvertisedVersion = defaults.AdvertisedVersion
	}
	if out.ClusterName == "" {
		out.ClusterName = defaults.ClusterName
	}
	return out
}

func stagingConfigFromIngest(ingest config.IngestConfig) staging.Config {
	cfg := ingest.Staging
	return staging.Config{
		Enabled:           cfg.Enabled,
		MaxBytes:          int64(cfg.MaxBytes),
		MaxAge:            cfg.MaxAge.Duration(),
		MaxInflightEvents: cfg.MaxInflightEvents,
		FlushRetries:      cfg.FlushRetries,
		FlushBackoffMax:   cfg.FlushBackoffMax.Duration(),
	}
}

// Start starts the API server. Blocks until context is canceled.
func (s *Server) Start(ctx context.Context) error {
	if err := s.engine.Start(ctx); err != nil {
		return err
	}
	s.startStagingBuffer()

	if s.syslogReceiver != nil {
		go func() {
			if err := s.syslogReceiver.Start(ctx); err != nil {
				s.engine.Logger().Error("syslog receiver stopped with error", "error", err)
			}
		}()
		s.syslogReceiver.WaitReady()
		if err := s.syslogReceiver.ReadyError(); err != nil {
			if shutErr := s.engine.Shutdown(5 * time.Second); shutErr != nil {
				slog.Error("engine shutdown failed after syslog listen error", "error", shutErr)
			}
			s.closeStagingBuffer(context.Background())
			return fmt.Errorf("syslog: %w", err)
		}
	}
	if s.otlpHTTPReceiver != nil {
		go func() {
			if err := s.otlpHTTPReceiver.Start(ctx); err != nil {
				s.engine.Logger().Error("OTLP HTTP receiver stopped with error", "error", err)
			}
		}()
		s.otlpHTTPReceiver.WaitReady()
		if err := s.otlpHTTPReceiver.ReadyError(); err != nil {
			if s.syslogReceiver != nil {
				s.syslogReceiver.Stop()
			}
			if s.otlpHTTPReceiver != nil {
				_ = s.otlpHTTPReceiver.Stop(context.Background())
			}
			if shutErr := s.engine.Shutdown(5 * time.Second); shutErr != nil {
				slog.Error("engine shutdown failed after OTLP listen error", "error", shutErr)
			}
			s.closeStagingBuffer(context.Background())
			return fmt.Errorf("otlp http: %w", err)
		}
	}

	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", s.httpServer.Addr)
	if err != nil {
		// Engine was started but we can't listen — shut it down.
		if s.syslogReceiver != nil {
			s.syslogReceiver.Stop()
		}
		if s.otlpHTTPReceiver != nil {
			_ = s.otlpHTTPReceiver.Stop(context.Background())
		}
		if shutErr := s.engine.Shutdown(5 * time.Second); shutErr != nil {
			slog.Error("engine shutdown failed after listen error", "error", shutErr)
		}
		s.closeStagingBuffer(context.Background())
		return fmt.Errorf("api: listen: %w", err)
	}

	// Wrap with TLS if configured.
	if s.tlsConfig != nil {
		ln = tls.NewListener(ln, s.tlsConfig)
	}

	s.listenAddr.Store(ln.Addr().String())

	// shutdownDone is closed after the engine has fully shut down (batcher
	// flushed, mmaps closed). Start() waits on this channel before returning
	// so callers can safely reuse the data directory.
	shutdownDone := make(chan struct{})
	go func() {
		defer close(shutdownDone)
		<-ctx.Done()
		if s.rateLimiter != nil {
			s.rateLimiter.Stop()
		}

		if s.syslogReceiver != nil {
			s.syslogReceiver.Stop()
		}
		if s.otlpHTTPReceiver != nil {
			_ = s.otlpHTTPReceiver.Stop(context.Background())
		}
		s.closeStagingBuffer(context.Background())

		// Shutdown ordering: reject ingests → drain HTTP → flush storage.
		s.engine.PrepareShutdown()
		shutdownTimeout := s.currentShutdownTimeout()
		s.engine.Logger().Info("shutting down: draining in-flight requests", "timeout", shutdownTimeout)
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		if err := s.httpServer.Shutdown(shutdownCtx); err != nil {
			s.engine.Logger().Error("HTTP server shutdown error", "error", err)
		}

		// Safe to flush batcher and close mmaps — no in-flight ingests remain.
		if err := s.engine.Shutdown(shutdownTimeout); err != nil {
			s.engine.Logger().Error("engine shutdown error", "error", err)
		}
	}()

	close(s.ready)
	s.engine.Logger().Info("API server started", "addr", s.Addr())
	if err := s.httpServer.Serve(ln); err != nil && err != http.ErrServerClosed {
		return err
	}

	// Wait for the shutdown goroutine to complete (batcher flush + mmap close)
	// before returning. Without this, callers that restart the server on the
	// same data directory may read stale data because the batcher flush has
	// not finished yet.
	<-shutdownDone

	return nil
}

type engineSink struct {
	engine *server.Engine
}

func (s engineSink) Write(events []*event.Event) error {
	return s.engine.IngestContext(context.Background(), events)
}

func (s *Server) submitShipperEvents(ctx context.Context, events []*event.Event) error {
	if s.stagingBuffer != nil {
		return s.stagingBuffer.Add(ctx, events)
	}
	return s.engine.IngestContext(ctx, events)
}

func (s *Server) startStagingBuffer() {
	if s.stagingBuffer != nil {
		return
	}
	s.stagingBuffer = staging.NewBuffer(
		stagingConfigFromIngest(s.currentIngestConfig()),
		func(ctx context.Context, events []*event.Event) error {
			return s.engine.IngestContext(ctx, events)
		},
		memgov.NewClassAccount(s.engine.Governor(), memgov.ClassTempIO),
		s.promMetrics,
	)
}

func (s *Server) closeStagingBuffer(ctx context.Context) {
	if s.stagingBuffer == nil {
		return
	}
	stagingCtx, cancel := context.WithTimeout(ctx, s.currentShutdownTimeout())
	defer cancel()
	if err := s.stagingBuffer.Close(stagingCtx); err != nil {
		s.engine.Logger().Error("staging buffer shutdown error", "error", err)
	}
}

// WaitReady blocks until the server has completed initialization and is ready.
func (s *Server) WaitReady() {
	<-s.ready
}

// Addr returns the address the server is listening on.
func (s *Server) Addr() string {
	if v := s.listenAddr.Load(); v != nil {
		return v.(string)
	}

	return s.httpServer.Addr
}

// OTLPHTTPAddr returns the resolved canonical OTLP/HTTP listener address.
// It is empty when the listener is disabled.
func (s *Server) OTLPHTTPAddr() string {
	if s.otlpHTTPReceiver == nil {
		return ""
	}
	return s.otlpHTTPReceiver.Addr()
}

// SetIndexStore sets an external IndexStore for full SPL2 queries.
func (s *Server) SetIndexStore(store *spl2.IndexStore) {
	s.engine.SetIndexStore(store)
}

// TLSEnabled reports whether the server is listening with TLS.
func (s *Server) TLSEnabled() bool {
	return s.tlsConfig != nil
}

// Engine returns the underlying Engine (for tests).
func (s *Server) Engine() *server.Engine {
	return s.engine
}

func defaultIdleTimeout(d time.Duration) time.Duration {
	if d == 0 {
		return 120 * time.Second
	}

	return d
}

func defaultReadHeaderTimeout(d time.Duration) time.Duration {
	if d == 0 {
		return 10 * time.Second
	}

	return d
}

func defaultShutdownTimeout(d time.Duration) time.Duration {
	if d == 0 {
		return 30 * time.Second
	}

	return d
}

func (s *Server) currentQueryConfig() config.QueryConfig {
	s.cfgMu.RLock()
	defer s.cfgMu.RUnlock()

	return s.queryCfg
}

func (s *Server) currentIngestConfig() config.IngestConfig {
	s.cfgMu.RLock()
	defer s.cfgMu.RUnlock()

	return s.ingestCfg
}

func (s *Server) currentTailConfig() config.TailConfig {
	s.cfgMu.RLock()
	defer s.cfgMu.RUnlock()

	return s.tailCfg
}

func (s *Server) currentShutdownTimeout() time.Duration {
	s.cfgMu.RLock()
	defer s.cfgMu.RUnlock()

	return s.shutdownTimeout
}

func httpError(w http.ResponseWriter, msg string, code int) {
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(map[string]string{"error": msg}); err != nil {
		slog.Warn("failed to write error response", "error", err)
	}
}

// requirePathValue extracts a named path parameter and validates it is non-empty.
// Returns the value and true on success. On failure, writes a 400 error response
// and returns ("", false).
func requirePathValue(r *http.Request, w http.ResponseWriter, key string) (string, bool) {
	val := r.PathValue(key)
	if val == "" {
		respondError(w, ErrCodeValidationError, http.StatusBadRequest,
			fmt.Sprintf("missing required path parameter: %s", key))

		return "", false
	}

	return val, true
}
