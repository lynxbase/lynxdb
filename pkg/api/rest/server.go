package rest

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lynxbase/lynxdb/pkg/alerts"
	"github.com/lynxbase/lynxdb/pkg/alerts/channels"
	"github.com/lynxbase/lynxdb/pkg/auth"
	"github.com/lynxbase/lynxdb/pkg/config"
	"github.com/lynxbase/lynxdb/pkg/dashboards"
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
	alertMgr           *alerts.Manager
	queryStore         *savedqueries.Store
	dashboardStore     *dashboards.DashboardStore
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
}

// Config configures the API server.
type Config struct {
	Addr      string
	DataDir   string        // Root directory for all data (segments, parts, indexes). Empty = in-memory only.
	Retention time.Duration // Data retention period. 0 = use default (90 days).

	KeyStore      *auth.KeyStore
	TLSConfig     *tls.Config // If non-nil, server listens with TLS.
	Storage       config.StorageConfig
	Logger        *slog.Logger
	Query         config.QueryConfig
	Ingest        config.IngestConfig
	HTTP          config.HTTPConfig
	Tail          config.TailConfig
	Server        config.ServerConfig
	Views         config.ViewsConfig
	BufferManager config.BufferManagerConfig
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
	})

	// Build planner, query service, and view service.
	p := planner.New(planner.WithViewCatalog(engine))
	queryService := usecases.NewQueryService(p, engine, cfg.Query)
	viewService := usecases.NewViewService(engine)
	tailService := usecases.NewTailService(p, engine)

	// Build alert query function that wraps the engine's query pipeline.
	alertQueryFn := func(ctx context.Context, query string) ([]map[string]interface{}, error) {
		plan, err := p.Plan(planner.PlanRequest{Query: query})
		if err != nil {
			return nil, err
		}
		job, err := engine.SubmitQuery(ctx, server.QueryParams{
			Query:      plan.RawQuery,
			Program:    plan.Program,
			ResultType: plan.ResultType,
		})
		if err != nil {
			return nil, err
		}
		select {
		case <-job.Done():
		case <-ctx.Done():
			job.Cancel()

			return nil, ctx.Err()
		}
		snap := job.Snapshot()
		if snap.Status == server.JobStatusError {
			return nil, fmt.Errorf("%s", snap.Error)
		}
		rows := make([]map[string]interface{}, len(snap.Results))
		for i, r := range snap.Results {
			rows[i] = r.Fields
		}

		return rows, nil
	}

	registry := channels.NewRegistry()
	alertMgr, err := alerts.NewManager(cfg.DataDir, registry.Factory(), alertQueryFn, cfg.Logger)
	if err != nil {
		if cfg.DataDir != "" {
			return nil, fmt.Errorf("alert manager init: %w", err)
		}
		cfg.Logger.Error("failed to initialize alert manager", "error", err)
	}

	// Initialize saved queries store.
	var qStore *savedqueries.Store
	var storeDegraded bool
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

	// Initialize dashboards store.
	var dStore *dashboards.DashboardStore
	if cfg.DataDir != "" {
		dStore, err = dashboards.OpenStore(cfg.DataDir)
		if err != nil {
			cfg.Logger.Warn("[DATA LOSS RISK] failed to open dashboards store, falling back to in-memory", "error", err)
			dStore = dashboards.OpenInMemory()
			storeDegraded = true
		}
	} else {
		dStore = dashboards.OpenInMemory()
	}

	shutdownTimeout := cfg.HTTP.ShutdownTimeout
	if shutdownTimeout == 0 {
		shutdownTimeout = 30 * time.Second
	}

	// Build runtime config snapshot. Only override defaults for sub-configs
	// that the caller explicitly provided (non-zero SyncTimeout indicates
	// the QueryConfig was set, etc.).
	runtimeCfg := config.DefaultConfig()
	runtimeCfg.Listen = cfg.Addr
	runtimeCfg.DataDir = cfg.DataDir
	if cfg.Query.SyncTimeout > 0 {
		runtimeCfg.Query = cfg.Query
	}
	if cfg.Ingest.MaxBodySize > 0 {
		runtimeCfg.Ingest = cfg.Ingest
	}
	if cfg.HTTP.IdleTimeout > 0 {
		runtimeCfg.HTTP = cfg.HTTP
	}

	s := &Server{
		engine:          engine,
		keyStore:        cfg.KeyStore,
		queryService:    queryService,
		viewService:     viewService,
		tailService:     tailService,
		alertMgr:        alertMgr,
		queryStore:      qStore,
		dashboardStore:  dStore,
		runtimeCfg:      runtimeCfg,
		ready:           make(chan struct{}),
		queryCfg:        cfg.Query,
		ingestCfg:       cfg.Ingest,
		shutdownTimeout: shutdownTimeout,
		tailCfg:         cfg.Tail,
		tlsConfig:       cfg.TLSConfig,
	}
	if storeDegraded {
		s.degraded.Store(true)
	}

	// Register Prometheus metrics and wire the OnQueryComplete hook so that
	// every completed query records histogram observations (duration, scan,
	// pipeline, memory, rows) and increments segment-skip counters.
	promMetrics := NewPrometheusMetrics()
	engine.SetOnQueryComplete(promMetrics.RecordQuery)

	mux := http.NewServeMux()

	// Prometheus metrics endpoint (standard /metrics path).
	mux.Handle("GET /metrics", promMetrics.Handler())

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

	// Saved queries (generic CRUD).
	registerCRUD(mux, "/api/v1/queries", CRUDOpts[savedqueries.SavedQuery, *savedqueries.SavedQueryInput]{
		Store:       s.queryStore,
		ConflictErr: savedqueries.ErrAlreadyExists,
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

	// Dashboards (generic CRUD).
	registerCRUD(mux, "/api/v1/dashboards", CRUDOpts[dashboards.Dashboard, dashboards.DashboardInput]{
		Store:       s.dashboardStore,
		ConflictErr: dashboards.ErrDashboardAlreadyExists,
		NewEntity: func(input dashboards.DashboardInput) *dashboards.Dashboard {
			return input.ToDashboard()
		},
		MergeEntity: func(existing *dashboards.Dashboard, input dashboards.DashboardInput) *dashboards.Dashboard {
			existing.Name = input.Name
			existing.Panels = input.Panels
			existing.Variables = input.Variables
			existing.UpdatedAt = time.Now()

			return existing
		},
	})

	// Alerts.
	mux.HandleFunc("POST /api/v1/alerts", s.handleCreateAlert)
	mux.HandleFunc("GET /api/v1/alerts", s.handleListAlerts)
	mux.HandleFunc("GET /api/v1/alerts/{id}", s.handleGetAlert)
	mux.HandleFunc("PUT /api/v1/alerts/{id}", s.handleUpdateAlert)
	mux.HandleFunc("DELETE /api/v1/alerts/{id}", s.handleDeleteAlert)
	mux.HandleFunc("POST /api/v1/alerts/{id}/test", s.handleTestAlert)
	mux.HandleFunc("POST /api/v1/alerts/{id}/test-channels", s.handleTestAlertChannels)

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
	mux.HandleFunc("POST /api/v1/es/_bulk", s.handleESBulk)
	mux.HandleFunc("POST /api/v1/es/{index}/_doc", s.handleESIndexDoc)
	mux.HandleFunc("GET /api/v1/es/", s.handleESClusterInfo)
	mux.HandleFunc("GET /api/v1/es", s.handleESClusterInfo)

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

	idleTimeout := cfg.HTTP.IdleTimeout
	if idleTimeout == 0 {
		idleTimeout = 120 * time.Second
	}
	// Build middleware chain: logging → rate limiter → auth → body limit → mux.
	var handler http.Handler = mux
	handler = MaxBodyMiddleware(int64(cfg.Ingest.MaxBodySize), handler)
	handler = KeyAuthMiddleware(cfg.KeyStore, handler)
	if cfg.HTTP.RateLimit > 0 {
		s.rateLimiter = NewRateLimiter(cfg.HTTP.RateLimit, int(cfg.HTTP.RateLimit)*2)
		handler = RateLimitMiddleware(s.rateLimiter, handler)
	}
	handler = LoggingMiddleware(cfg.Logger, handler)
	handler = RequestIDMiddleware(handler)
	handler = RecoveryMiddleware(cfg.Logger, handler)

	s.httpServer = &http.Server{
		Addr:              cfg.Addr,
		Handler:           handler,
		IdleTimeout:       idleTimeout,
		ReadHeaderTimeout: 10 * time.Second,
	}

	return s, nil
}

// Start starts the API server. Blocks until context is canceled.
func (s *Server) Start(ctx context.Context) error {
	if err := s.engine.Start(ctx); err != nil {
		return err
	}

	if s.alertMgr != nil {
		s.alertMgr.Start(ctx)
	}

	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", s.httpServer.Addr)
	if err != nil {
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
		if s.alertMgr != nil {
			s.alertMgr.Stop()
		}

		// Shutdown ordering: reject ingests → drain HTTP → flush storage.
		s.engine.PrepareShutdown()

		s.engine.Logger().Info("shutting down: draining in-flight requests", "timeout", s.shutdownTimeout)
		shutdownCtx, cancel := context.WithTimeout(context.Background(), s.shutdownTimeout)
		defer cancel()
		if err := s.httpServer.Shutdown(shutdownCtx); err != nil {
			s.engine.Logger().Error("HTTP server shutdown error", "error", err)
		}

		// Safe to flush batcher and close mmaps — no in-flight ingests remain.
		if err := s.engine.Shutdown(s.shutdownTimeout); err != nil {
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

func httpError(w http.ResponseWriter, msg string, code int) {
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
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
