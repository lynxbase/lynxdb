package rest

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lynxbase/lynxdb/pkg/auth"
	"github.com/lynxbase/lynxdb/pkg/config"
	"github.com/lynxbase/lynxdb/pkg/database"
	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/ingest/limits"
	"github.com/lynxbase/lynxdb/pkg/ingest/receiver/eshttp"
	"github.com/lynxbase/lynxdb/pkg/ingest/receiver/otlpgrpc"
	"github.com/lynxbase/lynxdb/pkg/ingest/receiver/otlphttp"
	"github.com/lynxbase/lynxdb/pkg/ingest/receiver/splunkhec"
	syslogrecv "github.com/lynxbase/lynxdb/pkg/ingest/receiver/syslog"
)

// Server is the main LynxDB API server.
type Server struct {
	db               *database.DB
	started          time.Time
	keyStore         *auth.KeyStore // Nil when auth is disabled.
	runtimeCfg       *config.Config
	cfgMu            sync.RWMutex
	httpServer       *http.Server
	listenAddr       atomic.Value  // stores resolved listen address (string)
	ready            chan struct{} // closed when server is ready to accept requests
	stopping         chan struct{} // closed when shutdown begins
	queryCfg         config.QueryConfig
	ingestCfg        config.IngestConfig
	shutdownTimeout  time.Duration
	rateLimiter      *RateLimiter // nil if rate limiting is disabled
	tlsConfig        *tls.Config  // non-nil when TLS is enabled
	syslogReceiver   *syslogrecv.Receiver
	otlpHTTPReceiver *otlphttp.Receiver
	otlpGRPCReceiver *otlpgrpc.Receiver
	esHandshake      *eshttp.Handshake
	esStubs          *eshttp.Stubs
	promMetrics      *PrometheusMetrics
	splunkAuth       splunkhec.AuthConfig
	splunkAckStore   *splunkhec.AckStore
	levelVar         *slog.LevelVar
	logger           *slog.Logger
}

// Config configures the API server.
type Config struct {
	Addr          string
	DataDir       string         // Root directory of the indexes. Empty keeps them in a temporary directory.
	Retention     time.Duration  // How long events are kept; zero keeps them all.
	RuntimeConfig *config.Config // Optional full runtime snapshot used by reload/get-config paths.

	KeyStore  *auth.KeyStore
	TLSConfig *tls.Config // If non-nil, server listens with TLS.
	Logger    *slog.Logger
	LevelVar  *slog.LevelVar // Optional. When set, ReloadConfig adjusts the log level through it.
	Storage   config.StorageConfig
	Query     config.QueryConfig
	Ingest    config.IngestConfig
	HTTP      config.HTTPConfig
	Syslog    config.SyslogConfig
}

// NewServer opens the database in cfg.DataDir and builds the API server.
func NewServer(cfg Config) (*Server, error) {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	db, err := database.Open(database.Config{
		Dir:            indexDir(cfg.DataDir),
		Retention:      cfg.Retention,
		Partition:      cfg.Storage.Partition.Duration(),
		FlushInterval:  cfg.Storage.FlushInterval.Duration(),
		TimeFields:     cfg.Storage.TimeFields,
		SortKey:        cfg.Storage.SortKey,
		PartDictionary: cfg.Storage.PartDictionary,
		MaxConcurrent:  cfg.Query.MaxConcurrent,
		JobTTL:         cfg.Query.JobTTL,
		Logger:         cfg.Logger,
	})
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}
	s, err := newServer(cfg, db)
	if err != nil {
		return nil, errors.Join(err, db.Close())
	}

	return s, nil
}

func newServer(cfg Config, db *database.DB) (*Server, error) {
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
		if cfg.Storage.Partition > 0 {
			runtimeCfg.Storage = cfg.Storage
		}
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
		runtimeCfg.Auth.Enabled = cfg.KeyStore != nil
	}
	if runtimeCfg.Auth.Enabled && cfg.KeyStore == nil {
		return nil, errors.New("auth is enabled but the server has no key store")
	}

	s := &Server{
		db:              db,
		started:         time.Now(),
		keyStore:        cfg.KeyStore,
		runtimeCfg:      runtimeCfg,
		ready:           make(chan struct{}),
		stopping:        make(chan struct{}),
		queryCfg:        cfg.Query,
		ingestCfg:       cfg.Ingest,
		shutdownTimeout: defaultShutdownTimeout(cfg.HTTP.ShutdownTimeout),
		tlsConfig:       cfg.TLSConfig,
		splunkAckStore:  splunkhec.NewAckStore(10000),
		levelVar:        cfg.LevelVar,
		logger:          cfg.Logger,
	}

	if s.keyStore != nil {
		s.splunkAuth.Check = s.checkSplunkToken
	}

	promMetrics := NewPrometheusMetrics()
	promMetrics.CountExpired(db.Expired)
	s.promMetrics = promMetrics
	promMetrics.SetListenerUp("es", false)
	promMetrics.SetListenerUp("otlp_http", false)
	promMetrics.SetListenerUp("otlp_grpc", false)

	if cfg.Syslog.Enabled() {
		tlsCfg := cfg.TLSConfig
		if !cfg.Syslog.TLS {
			tlsCfg = nil
		}
		syslogReceiver, err := syslogrecv.New(
			cfg.Syslog,
			dbSink{s: s},
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
		otlpCfg := otlphttp.Config{
			Listen: cfg.Ingest.OTLP.HTTPListen,
			Limits: limits.Config{
				MaxCompressedBytes:   int64(cfg.Ingest.Limits.MaxCompressedBodyBytes),
				MaxDecompressedBytes: int64(cfg.Ingest.Limits.MaxDecompressedBodyBytes),
			},
		}
		if s.keyStore != nil {
			otlpCfg.Middleware = s.otlpHTTPAuth
		}
		otlpReceiver, err := otlphttp.New(
			otlpCfg,
			s.submitPipelineEvents,
			cfg.Logger,
			promMetrics,
		)
		if err != nil {
			return nil, fmt.Errorf("otlp http receiver: %w", err)
		}
		s.otlpHTTPReceiver = otlpReceiver
	}
	if cfg.Ingest.OTLP.GRPCListen != "" {
		otlpCfg := otlpgrpc.Config{
			Listen:       cfg.Ingest.OTLP.GRPCListen,
			MaxRecvBytes: int(cfg.Ingest.OTLP.GRPCMaxRecvBytes),
		}
		if s.keyStore != nil {
			otlpCfg.UnaryInterceptor = s.otlpGRPCAuth
		}
		otlpReceiver, err := otlpgrpc.New(
			otlpCfg,
			s.submitPipelineEvents,
			cfg.Logger,
			promMetrics,
		)
		if err != nil {
			return nil, fmt.Errorf("otlp grpc receiver: %w", err)
		}
		s.otlpGRPCReceiver = otlpReceiver
	}

	esCfg := normalizedESCompatConfig(cfg.Ingest)
	esHandshake, err := eshttp.NewHandshake(eshttp.Config{
		AdvertisedVersion: esCfg.AdvertisedVersion,
		ClusterName:       esCfg.ClusterName,
		DataDir:           cfg.DataDir,
	})
	if err != nil {
		return nil, err
	}
	s.esHandshake = esHandshake
	esStubs, err := eshttp.NewStubs(eshttp.Config{
		AdvertisedVersion: esCfg.AdvertisedVersion,
		ClusterName:       esCfg.ClusterName,
		DataDir:           cfg.DataDir,
	})
	if err != nil {
		return nil, err
	}
	s.esStubs = esStubs

	mux := http.NewServeMux()

	mux.Handle("GET /metrics", promMetrics.Handler())

	// Profiles show what the server runs, so they take an admin key.
	profile := func(h http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			if s.requireScope(w, r, auth.ScopeAdmin) {
				h(w, r)
			}
		}
	}
	mux.HandleFunc("GET /api/v1/debug/pprof/", profile(pprof.Index))
	mux.HandleFunc("GET /api/v1/debug/pprof/cmdline", profile(pprof.Cmdline))
	mux.HandleFunc("GET /api/v1/debug/pprof/profile", profile(pprof.Profile))
	mux.HandleFunc("GET /api/v1/debug/pprof/symbol", profile(pprof.Symbol))
	mux.HandleFunc("GET /api/v1/debug/pprof/trace", profile(pprof.Trace))
	for _, name := range []string{"heap", "allocs", "goroutine", "mutex", "block", "threadcreate"} {
		mux.HandleFunc("GET /api/v1/debug/pprof/"+name, profile(pprof.Handler(name).ServeHTTP))
	}

	// Query endpoint (three-mode: sync/hybrid/async).
	mux.HandleFunc("POST /api/v1/query", s.handleQuery)
	mux.HandleFunc("GET /api/v1/query", s.handleQueryGet)
	mux.HandleFunc("POST /api/v1/query/stream", s.handleQueryStream)
	mux.HandleFunc("GET /api/v1/query/explain", s.handleQueryExplain)

	// Job management (for async/hybrid jobs).
	mux.HandleFunc("GET /api/v1/query/jobs/{id}", s.handleGetJob)
	mux.HandleFunc("DELETE /api/v1/query/jobs/{id}", s.handleCancelJob)
	mux.HandleFunc("GET /api/v1/query/jobs", s.handleListJobs)

	mux.HandleFunc("GET /api/v1/indexes", s.handleListIndexes)
	mux.HandleFunc("GET /api/v1/indexes/{index}", s.handleGetIndex)
	mux.HandleFunc("PUT /api/v1/indexes/{index}", s.handleCreateIndex)
	mux.HandleFunc("GET /api/v1/indexes/{index}/columns", s.handleIndexColumns)
	mux.HandleFunc("POST /api/v1/indexes/{index}/compact", s.handleCompactIndex)

	mux.HandleFunc("POST /api/v1/ingest", s.handleIngestEvents)
	mux.HandleFunc("POST /api/v1/ingest/raw", s.handleIngestRaw)
	mux.HandleFunc("POST /services/collector/event", s.handleSplunkHECEvent)
	mux.HandleFunc("POST /services/collector", s.handleSplunkHECEvent)
	mux.HandleFunc("POST /services/collector/raw", s.handleSplunkHECRaw)
	mux.HandleFunc("POST /services/collector/ack", s.handleSplunkHECAck)
	mux.HandleFunc("GET /services/collector/health", s.handleSplunkHECHealth)
	// PPL command and function catalog.
	mux.HandleFunc("GET /api/v1/catalog", s.handleCatalog)

	mux.HandleFunc("GET /api/v1/fields", s.handleListFields)
	mux.HandleFunc("GET /api/v1/fields/{name}/values", s.handleFieldValues)

	mux.HandleFunc("GET /api/v1/stats", s.handleStats)

	mux.HandleFunc("GET /api/v1/config", s.handleGetConfig)
	mux.HandleFunc("PATCH /api/v1/config", s.handlePatchConfig)

	mux.HandleFunc("GET /api/v1/histogram", s.handleHistogram)

	// Elasticsearch compatibility.
	mux.Handle("GET /{$}", s.esCompatibilityHandler("root", esHandshake))
	mux.Handle("GET /_xpack", s.esCompatibilityHandler("xpack", http.HandlerFunc(esStubs.XPackInfo)))
	mux.Handle("GET /_xpack/license", s.esCompatibilityHandler("license", http.HandlerFunc(esStubs.XPackLicense)))
	mux.Handle("GET /_license", s.esCompatibilityHandler("license", http.HandlerFunc(esStubs.XPackLicense)))
	mux.Handle("GET /_cat/templates", s.esCompatibilityHandler("templates", http.HandlerFunc(esStubs.EmptyArray)))
	mux.Handle("GET /_cat/indices", s.esCompatibilityHandler("cat_indices", http.HandlerFunc(esStubs.EmptyArray)))
	mux.Handle("GET /_cluster/health", s.esCompatibilityHandler("cluster_health", http.HandlerFunc(esStubs.ClusterHealth)))
	mux.Handle("PUT /_index_template/{name...}", s.esCompatibilityHandler("template", http.HandlerFunc(esStubs.Acknowledged)))
	mux.Handle("GET /_index_template/{name...}", s.esCompatibilityHandler("template", http.HandlerFunc(esStubs.IndexTemplates)))
	mux.Handle("GET /_ilm/policy/{name...}", s.esCompatibilityHandler("ilm", http.HandlerFunc(esStubs.NotFound)))
	mux.Handle("GET /_ingest/pipeline/{name...}", s.esCompatibilityHandler("pipeline", http.HandlerFunc(esStubs.NotFound)))
	mux.Handle("GET /_nodes/{path...}", s.esCompatibilityHandler("nodes", http.HandlerFunc(esStubs.NodesHTTP)))
	mux.Handle("GET /_alias", s.esCompatibilityHandler("alias", http.HandlerFunc(esStubs.EmptyAliases)))
	mux.Handle("GET /_data_stream/{name...}", s.esCompatibilityHandler("datastream", http.HandlerFunc(esStubs.EmptyDataStreams)))
	mux.Handle("PUT /_data_stream/{name...}", s.esCompatibilityHandler("datastream", http.HandlerFunc(esStubs.Acknowledged)))
	mux.Handle("GET /_search", s.esCompatibilityHandler("search", http.HandlerFunc(esStubs.EmptySearch)))
	mux.Handle("POST /_security/user/_authenticate", s.esCompatibilityHandler("security", http.HandlerFunc(esStubs.Authenticated)))
	mux.HandleFunc("POST /_bulk", s.handleESBulk)
	mux.HandleFunc("POST /_data_stream/{data_stream}/_bulk", s.handleESBulk)
	mux.HandleFunc("POST /{index}/_bulk", s.handleESBulk)
	mux.HandleFunc("POST /{index}/_doc", s.handleESIndexDoc)

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

	mux.HandleFunc("GET /health", s.handleHealth)

	// Build middleware chain (execution order, outer → inner):
	// Recovery → RequestID → Logging → Auth → RateLimit → MaxBody → DualLimit → mux.
	// Auth runs before rate limiting so unauthenticated requests don't consume quota.
	var handler http.Handler = mux
	handler = s.esIndexHeadMiddleware(handler)
	handler = bodyLimits(cfg.Ingest, promMetrics, handler)
	handler = MaxBodyMiddleware(int64(cfg.Ingest.MaxBodySize), handler)
	if cfg.HTTP.RateLimit > 0 {
		s.rateLimiter = NewRateLimiter(cfg.HTTP.RateLimit, int(cfg.HTTP.RateLimit)*2)
		handler = RateLimitMiddleware(s.rateLimiter, handler)
	}
	handler = KeyAuthMiddleware(cfg.KeyStore, handler)
	handler = LoggingMiddleware(cfg.Logger, handler)
	handler = RequestIDMiddleware(handler)
	handler = RecoveryMiddleware(cfg.Logger, handler)

	s.httpServer = &http.Server{
		Addr:              cfg.Addr,
		Handler:           handler,
		IdleTimeout:       defaultIdleTimeout(cfg.HTTP.IdleTimeout),
		ReadHeaderTimeout: defaultReadHeaderTimeout(cfg.HTTP.ReadHeaderTimeout),
	}

	return s, nil
}

func (s *Server) esIndexHeadMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead && isESIndexHeadProbe(r.URL.Path) {
			s.esCompatibilityHandler("index_head", http.HandlerFunc(s.esStubs.HeadOK)).ServeHTTP(w, r)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func isESIndexHeadProbe(path string) bool {
	trimmed := strings.Trim(path, "/")
	if trimmed == "health" || trimmed == "metrics" {
		return false
	}
	return trimmed != "" && !strings.Contains(trimmed, "/") && !strings.HasPrefix(trimmed, "_")
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

// Start starts the receivers and serves the API until ctx is canceled, then
// shuts the server down and closes the database.
func (s *Server) Start(ctx context.Context) error {
	if err := s.startReceivers(ctx); err != nil {
		s.stopReceivers()
		s.closeDB()
		return err
	}

	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", s.httpServer.Addr)
	if err != nil {
		s.stopReceivers()
		s.closeDB()
		return fmt.Errorf("api: listen: %w", err)
	}
	if s.tlsConfig != nil {
		ln = tls.NewListener(ln, s.tlsConfig)
	}

	s.listenAddr.Store(ln.Addr().String())
	s.promMetrics.SetListenerUp("es", true)

	// Start returns only after shutdown has closed the database, so that a
	// caller may reopen the data directory at once.
	shutdownDone := make(chan struct{})
	go func() {
		defer close(shutdownDone)
		<-ctx.Done()
		s.shutdown()
	}()

	close(s.ready)
	s.logger.Info("API server started", "addr", s.Addr())
	if err := s.httpServer.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	<-shutdownDone

	return nil
}

// startReceivers starts the syslog and OTLP listeners and waits until each
// is bound.
func (s *Server) startReceivers(ctx context.Context) error {
	if s.syslogReceiver != nil {
		go func() {
			if err := s.syslogReceiver.Start(ctx); err != nil {
				s.logger.Error("syslog receiver stopped with error", "error", err)
			}
		}()
		s.syslogReceiver.WaitReady()
		if err := s.syslogReceiver.ReadyError(); err != nil {
			return fmt.Errorf("syslog: %w", err)
		}
	}
	if s.otlpHTTPReceiver != nil {
		go func() {
			if err := s.otlpHTTPReceiver.Start(ctx); err != nil {
				s.logger.Error("OTLP HTTP receiver stopped with error", "error", err)
			}
		}()
		s.otlpHTTPReceiver.WaitReady()
		if err := s.otlpHTTPReceiver.ReadyError(); err != nil {
			return fmt.Errorf("otlp http: %w", err)
		}
		s.promMetrics.SetListenerUp("otlp_http", true)
	}
	if s.otlpGRPCReceiver != nil {
		go func() {
			if err := s.otlpGRPCReceiver.Start(ctx); err != nil {
				s.logger.Error("OTLP gRPC receiver stopped with error", "error", err)
			}
		}()
		s.otlpGRPCReceiver.WaitReady()
		if err := s.otlpGRPCReceiver.ReadyError(); err != nil {
			return fmt.Errorf("otlp grpc: %w", err)
		}
		s.promMetrics.SetListenerUp("otlp_grpc", true)
	}

	return nil
}

func (s *Server) stopReceivers() {
	if s.syslogReceiver != nil {
		s.syslogReceiver.Stop()
	}
	if s.otlpHTTPReceiver != nil {
		_ = s.otlpHTTPReceiver.Stop(context.Background())
		s.promMetrics.SetListenerUp("otlp_http", false)
	}
	if s.otlpGRPCReceiver != nil {
		_ = s.otlpGRPCReceiver.Stop(context.Background())
		s.promMetrics.SetListenerUp("otlp_grpc", false)
	}
}

// shutdown stops the receivers, drains the HTTP requests in flight and
// closes the database.
func (s *Server) shutdown() {
	close(s.stopping)
	if s.rateLimiter != nil {
		s.rateLimiter.Stop()
	}
	s.stopReceivers()

	timeout := s.currentShutdownTimeout()
	s.logger.Info("shutting down: draining in-flight requests", "timeout", timeout)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := s.httpServer.Shutdown(ctx); err != nil {
		s.logger.Error("HTTP server shutdown error", "error", err)
	}
	s.promMetrics.SetListenerUp("es", false)
	s.closeDB()
}

// syncForRead makes the events appended so far count in the stats and fields
// of the indexes, which see only written events.
func (s *Server) syncForRead() {
	if err := s.db.Sync(); err != nil {
		s.logger.Warn("database sync failed", "error", err)
	}
}

func (s *Server) closeDB() {
	if err := s.db.Close(); err != nil {
		s.logger.Error("database close error", "error", err)
	}
}

// dbSink hands the events of the syslog receiver to the database.
type dbSink struct{ s *Server }

func (d dbSink) Write(events []*event.Event) error {
	return d.s.submitEvents(context.Background(), events)
}

// submitEvents adds decoded events to the indexes they name.
func (s *Server) submitEvents(_ context.Context, events []*event.Event) error {
	if err := s.db.AppendEvents(events); err != nil {
		s.promMetrics.RecordIngestError()
		return err
	}
	s.promMetrics.RecordIngest(len(events), 0)

	return nil
}

func (s *Server) submitPipelineEvents(ctx context.Context, events []*event.Event) error {
	processed, err := ingestPipelineForConfig(s.currentIngestConfig()).Process(events)
	if err != nil {
		return err
	}
	return s.submitEvents(ctx, processed)
}

// checkSplunkToken accepts a Splunk HEC token that is an API key with the
// ingest scope.
func (s *Server) checkSplunkToken(token string) error {
	info := s.keyStore.Verify(token)
	if info == nil {
		return splunkhec.ErrInvalidToken
	}
	if !info.Allows(auth.ScopeIngest) {
		return splunkhec.ErrForbidden
	}

	return nil
}

func (s *Server) splunkHECHandler() *splunkhec.Handler {
	ingestCfg := s.currentIngestConfig()
	return splunkhec.NewHandler(splunkhec.Config{
		Auth:               s.splunkAuth,
		MaxBatchSize:       ingestCfg.MaxBatchSize,
		MaxLineBytes:       ingestCfg.MaxLineBytes,
		AckStore:           s.splunkAckStore,
		AckFlush:           func(context.Context) error { return s.db.Sync() },
		RespondIngestError: func(w http.ResponseWriter, err error) { respondIngestError(w, err) },
	}, s.submitPipelineEvents)
}

func (s *Server) handleSplunkHECEvent(w http.ResponseWriter, r *http.Request) {
	if !s.currentIngestConfig().SplunkHEC.Enabled {
		respondJSON(w, http.StatusNotFound, map[string]interface{}{"text": "HEC is disabled", "code": 4})
		return
	}
	s.splunkHECHandler().HandleEvent(w, r)
}

func (s *Server) handleSplunkHECRaw(w http.ResponseWriter, r *http.Request) {
	if !s.currentIngestConfig().SplunkHEC.Enabled {
		respondJSON(w, http.StatusNotFound, map[string]interface{}{"text": "HEC is disabled", "code": 4})
		return
	}
	s.splunkHECHandler().HandleRaw(w, r)
}

func (s *Server) handleSplunkHECHealth(w http.ResponseWriter, r *http.Request) {
	if !s.currentIngestConfig().SplunkHEC.Enabled {
		respondJSON(w, http.StatusNotFound, map[string]interface{}{"text": "HEC is disabled", "code": 4})
		return
	}
	s.splunkHECHandler().HandleHealth(w, r)
}

func (s *Server) handleSplunkHECAck(w http.ResponseWriter, r *http.Request) {
	if !s.currentIngestConfig().SplunkHEC.Enabled {
		respondJSON(w, http.StatusNotFound, map[string]interface{}{"text": "HEC is disabled", "code": 4})
		return
	}
	s.splunkHECHandler().HandleAck(w, r)
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

// OTLPGRPCAddr returns the resolved canonical OTLP/gRPC listener address.
// It is empty when the listener is disabled.
func (s *Server) OTLPGRPCAddr() string {
	if s.otlpGRPCReceiver == nil {
		return ""
	}
	return s.otlpGRPCReceiver.Addr()
}

// TLSEnabled reports whether the server is listening with TLS.
func (s *Server) TLSEnabled() bool {
	return s.tlsConfig != nil
}

// DB returns the database the server serves.
func (s *Server) DB() *database.DB {
	return s.db
}

// indexDir is the directory of the indexes under a data directory, apart
// from the server's keys and certificates; an empty data directory keeps
// the indexes in a temporary one.
func indexDir(dataDir string) string {
	if dataDir == "" {
		return ""
	}

	return filepath.Join(dataDir, "indexes")
}

// bodyLimits bounds request bodies: the native ingest endpoints by
// ingest.max_body_size, every other route by the shipper limits.
func bodyLimits(cfg config.IngestConfig, hook limits.Hook, next http.Handler) http.Handler {
	maxBody := int64(cfg.MaxBodySize)
	native := limits.DualLimitMiddleware(limits.Config{
		MaxCompressedBytes:   maxBody,
		MaxDecompressedBytes: maxBody,
		CompressedSetting:    "ingest.max_body_size",
		DecompressedSetting:  "ingest.max_body_size",
	}, hook)(next)
	shipper := limits.DualLimitMiddleware(limits.Config{
		MaxCompressedBytes:   int64(cfg.Limits.MaxCompressedBodyBytes),
		MaxDecompressedBytes: int64(cfg.Limits.MaxDecompressedBodyBytes),
		CompressedSetting:    "ingest.limits.max_compressed_body_bytes",
		DecompressedSetting:  "ingest.limits.max_decompressed_body_bytes",
	}, hook)(next)

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/ingest", "/api/v1/ingest/raw":
			native.ServeHTTP(w, r)
		default:
			shipper.ServeHTTP(w, r)
		}
	})
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

func (s *Server) currentShutdownTimeout() time.Duration {
	s.cfgMu.RLock()
	defer s.cfgMu.RUnlock()

	return s.shutdownTimeout
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
