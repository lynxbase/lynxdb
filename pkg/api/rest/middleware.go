package rest

import (
	"container/heap"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/lynxbase/lynxdb/pkg/auth"
)

// statusWriter wraps http.ResponseWriter to capture the status code.
type statusWriter struct {
	http.ResponseWriter
	code int
}

func (sw *statusWriter) WriteHeader(code int) {
	sw.code = code
	sw.ResponseWriter.WriteHeader(code)
}

// Flush forwards to the underlying ResponseWriter if it supports http.Flusher.
// The NDJSON stream of /api/v1/query/stream needs it.
func (sw *statusWriter) Flush() {
	if f, ok := sw.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// LoggingMiddleware logs each HTTP request with method, path, status, duration,
// and query_id (when present in the response headers). The query_id allows
// correlating HTTP request logs with engine-level query execution logs (O1).
func LoggingMiddleware(logger *slog.Logger, next http.Handler) http.Handler {
	if logger == nil {
		return next
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// /health is exempt from logging to avoid noise.
		if r.URL.Path == "/health" {
			next.ServeHTTP(w, r)

			return
		}

		start := time.Now()
		sw := &statusWriter{ResponseWriter: w, code: http.StatusOK}
		next.ServeHTTP(sw, r)
		elapsed := time.Since(start)

		attrs := []any{
			"method", r.Method,
			"path", r.URL.Path,
			"status", sw.code,
			"duration_ms", elapsed.Milliseconds(),
			"remote", r.RemoteAddr,
		}
		if qid := sw.Header().Get("X-Query-ID"); qid != "" {
			attrs = append(attrs, "query_id", qid)
		}
		if rid := sw.Header().Get("X-Request-ID"); rid != "" {
			attrs = append(attrs, "request_id", rid)
		}

		logger.Info("http request", attrs...)
	})
}

// RequestIDMiddleware generates a unique request ID for each request and sets
// it as the X-Request-ID response header. If the client provides an X-Request-ID
// header, that value is used instead (for end-to-end trace correlation).
func RequestIDMiddleware(next http.Handler) http.Handler {
	var counter atomic.Uint64

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqID := r.Header.Get("X-Request-ID")
		if reqID == "" {
			seq := counter.Add(1)
			reqID = fmt.Sprintf("%x-%04x", time.Now().UnixNano()>>20, seq&0xFFFF)
		}
		w.Header().Set("X-Request-ID", reqID)
		next.ServeHTTP(w, r)
	})
}

// KeyAuthMiddleware checks the API key of every request when a KeyStore is
// provided, except on /health, /metrics and the Splunk HEC routes, which
// check their own Authorization: Splunk header. Supports three schemes:
//
//   - Bearer <token>          — standard LynxDB token
//   - ApiKey <base64(id:secret)> — Elasticsearch/Filebeat compatible
//   - Basic <base64(user:pass)>  — password is used as token, username informational
//
// Verifies the token with the key store and attaches the authenticated key
// info to the request context.
//
// When ks is nil, auth is disabled and all requests pass through.
func KeyAuthMiddleware(ks *auth.KeyStore, next http.Handler) http.Handler {
	if ks == nil {
		return next
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if keyAuthExempt(r.URL.Path) {
			next.ServeHTTP(w, r)

			return
		}

		info, err := authenticate(ks, r.Header.Get("Authorization"))
		if err != nil {
			respondAuthError(w, r, err)

			return
		}

		ctx := auth.WithKeyInfo(r.Context(), info)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// keyAuthExempt reports whether a path is served without an API key: the
// probes, and the Splunk HEC routes, whose handlers check the key in the
// Authorization: Splunk header. Every other path, including unknown ones,
// needs a key.
func keyAuthExempt(path string) bool {
	switch path {
	case "/health", "/metrics",
		"/services/collector", "/services/collector/event", "/services/collector/raw",
		"/services/collector/ack", "/services/collector/health":
		return true
	}

	return false
}

var (
	errNoCredentials  = errors.New("authentication required")
	errBadCredentials = errors.New("invalid Authorization header format")
	errInvalidKey     = errors.New("invalid API key")
)

// authenticate returns the key named by the value of an Authorization header.
func authenticate(ks *auth.KeyStore, header string) (*auth.KeyInfo, error) {
	if header == "" {
		return nil, errNoCredentials
	}
	keyID, token, ok := extractToken(header)
	if !ok {
		return nil, errBadCredentials
	}
	info := ks.Verify(token)
	if info == nil || (keyID != "" && keyID != info.ID) {
		return nil, errInvalidKey
	}

	return info, nil
}

// respondAuthError answers a request without a valid key. Outside /api/ the
// routes serve Elasticsearch clients, which get the error and the challenges
// Elasticsearch sends; some clients send Basic credentials only after one.
func respondAuthError(w http.ResponseWriter, r *http.Request, err error) {
	if !strings.HasPrefix(r.URL.Path, "/api/") {
		setESHeaders(w)
		w.Header().Add("WWW-Authenticate", `Basic realm="lynxdb", charset="UTF-8"`)
		w.Header().Add("WWW-Authenticate", "ApiKey")
		respondESError(w, http.StatusUnauthorized, "security_exception", err.Error())

		return
	}
	switch {
	case errors.Is(err, errNoCredentials):
		respondError(w, ErrCodeAuthRequired, http.StatusUnauthorized,
			"Authentication required. Provide: Authorization: Bearer <key>",
			WithSuggestion("Run 'lynxdb login' or set LYNXDB_TOKEN environment variable"))
	case errors.Is(err, errBadCredentials):
		respondError(w, ErrCodeInvalidToken, http.StatusUnauthorized, "Invalid Authorization header format")
	default:
		respondError(w, ErrCodeInvalidToken, http.StatusUnauthorized, "Invalid API key")
	}
}

// otlpHTTPAuth requires a key with the ingest scope on the OTLP/HTTP listener.
func (s *Server) otlpHTTPAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		info, err := authenticate(s.keyStore, r.Header.Get("Authorization"))
		if err != nil {
			http.Error(w, err.Error(), http.StatusUnauthorized)

			return
		}
		if !info.Allows(auth.ScopeIngest) {
			http.Error(w, "the API key lacks the ingest scope", http.StatusForbidden)

			return
		}
		next.ServeHTTP(w, r)
	})
}

// otlpGRPCAuth requires a key with the ingest scope, in the authorization
// metadata, on the OTLP/gRPC listener.
func (s *Server) otlpGRPCAuth(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	var header string
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if v := md.Get("authorization"); len(v) > 0 {
			header = v[0]
		}
	}
	info, err := authenticate(s.keyStore, header)
	if err != nil {
		return nil, status.Error(codes.Unauthenticated, err.Error())
	}
	if !info.Allows(auth.ScopeIngest) {
		return nil, status.Error(codes.PermissionDenied, "the API key lacks the ingest scope")
	}

	return handler(ctx, req)
}

// extractToken parses the Authorization header and returns (keyID, token, ok).
// keyID is non-empty only for the ApiKey scheme, whose id must name the key
// of the token.
func extractToken(header string) (keyID, token string, ok bool) {
	switch {
	case strings.HasPrefix(header, "Bearer "):
		return "", strings.TrimSpace(header[7:]), true

	case strings.HasPrefix(header, "ApiKey "):
		// Filebeat format: ApiKey <base64(id:secret)>
		decoded, err := base64.StdEncoding.DecodeString(strings.TrimSpace(header[7:]))
		if err != nil {
			return "", "", false
		}
		parts := strings.SplitN(string(decoded), ":", 2)
		if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
			return "", "", false
		}
		return parts[0], parts[1], true

	case strings.HasPrefix(header, "Basic "):
		// Basic base64(username:password) — password = token, username informational.
		decoded, err := base64.StdEncoding.DecodeString(strings.TrimSpace(header[6:]))
		if err != nil {
			return "", "", false
		}
		parts := strings.SplitN(string(decoded), ":", 2)
		if len(parts) != 2 || parts[1] == "" {
			return "", "", false
		}
		return "", parts[1], true
	}

	return "", "", false
}

// requireScope checks that the request was authenticated with a key that has
// at least one of the required scopes. Root keys and full-scope keys pass all
// checks. Returns true if access is allowed; writes an error response and
// returns false otherwise.
func (s *Server) requireScope(w http.ResponseWriter, r *http.Request, required ...auth.Scope) bool {
	// When auth is disabled (no keyStore), all requests pass through.
	if s.keyStore == nil {
		return true
	}

	info := auth.KeyInfoFromContext(r.Context())
	if info == nil {
		respondError(w, ErrCodeAuthRequired, http.StatusUnauthorized, "Authentication required")
		return false
	}

	for _, req := range required {
		if info.Allows(req) {
			return true
		}
	}

	respondError(w, ErrCodeForbidden, http.StatusForbidden,
		fmt.Sprintf("This operation requires scope: %s (your key has scope: %s)", required[0], info.Scope))
	return false
}

// RateLimiter implements a per-IP token bucket rate limiter.
type RateLimiter struct {
	mu      sync.Mutex
	buckets map[string]*rateBucket
	heap    rateBucketHeap // min-heap by lastTime for O(log n) eviction
	rate    float64        // tokens per second
	burst   int            // max tokens
	maxSize int            // max entries before eviction
	stopCh  chan struct{}
}

type rateBucket struct {
	ip       string
	tokens   float64
	lastTime time.Time
	heapIdx  int // index in heap, -1 if removed
}

const (
	rateLimiterMaxSize    = 100_000
	rateLimiterStaleAfter = 10 * time.Minute
	rateLimiterGCInterval = time.Minute
)

// rateBucketHeap is a min-heap ordered by lastTime (oldest first) for O(log n) eviction.
type rateBucketHeap []*rateBucket

func (h rateBucketHeap) Len() int           { return len(h) }
func (h rateBucketHeap) Less(i, j int) bool { return h[i].lastTime.Before(h[j].lastTime) }
func (h rateBucketHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i]; h[i].heapIdx = i; h[j].heapIdx = j }
func (h *rateBucketHeap) Push(x interface{}) {
	b := x.(*rateBucket)
	b.heapIdx = len(*h)
	*h = append(*h, b)
}
func (h *rateBucketHeap) Pop() interface{} {
	old := *h
	n := len(old)
	b := old[n-1]
	old[n-1] = nil
	b.heapIdx = -1
	*h = old[:n-1]
	return b
}

// NewRateLimiter creates a rate limiter. Rate is requests per second, burst is max burst.
// Call Stop() on shutdown to release the cleanup goroutine.
func NewRateLimiter(rate float64, burst int) *RateLimiter {
	rl := &RateLimiter{
		buckets: make(map[string]*rateBucket),
		rate:    rate,
		burst:   burst,
		maxSize: rateLimiterMaxSize,
		stopCh:  make(chan struct{}),
	}
	heap.Init(&rl.heap)
	go rl.cleanupLoop()

	return rl
}

// Stop releases the cleanup goroutine. Safe to call multiple times.
func (rl *RateLimiter) Stop() {
	select {
	case <-rl.stopCh:
		// Already stopped.
	default:
		close(rl.stopCh)
	}
}

// cleanupLoop periodically removes stale entries from the rate limiter.
func (rl *RateLimiter) cleanupLoop() {
	ticker := time.NewTicker(rateLimiterGCInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			rl.mu.Lock()
			now := time.Now()
			// Pop stale entries from the min-heap (oldest first).
			for rl.heap.Len() > 0 {
				oldest := rl.heap[0]
				if now.Sub(oldest.lastTime) <= rateLimiterStaleAfter {
					break
				}
				heap.Pop(&rl.heap)
				delete(rl.buckets, oldest.ip)
			}
			rl.mu.Unlock()
		case <-rl.stopCh:
			return
		}
	}
}

// Allow checks if a request from the given IP is allowed.
func (rl *RateLimiter) Allow(ip string) bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	b, ok := rl.buckets[ip]
	now := time.Now()
	if !ok {
		if len(rl.buckets) >= rl.maxSize {
			// Evict the oldest bucket via min-heap: O(log n) instead of O(n).
			if rl.heap.Len() > 0 {
				evicted := heap.Pop(&rl.heap).(*rateBucket)
				delete(rl.buckets, evicted.ip)
			}
		}
		b = &rateBucket{ip: ip, tokens: float64(rl.burst), lastTime: now}
		rl.buckets[ip] = b
		heap.Push(&rl.heap, b)
	}

	elapsed := now.Sub(b.lastTime).Seconds()
	b.tokens += elapsed * rl.rate
	if b.tokens > float64(rl.burst) {
		b.tokens = float64(rl.burst)
	}
	b.lastTime = now
	if b.heapIdx >= 0 {
		heap.Fix(&rl.heap, b.heapIdx)
	}

	if b.tokens < 1 {
		return false
	}
	b.tokens--

	return true
}

// MaxBodyMiddleware limits request body size to prevent OOM from oversized payloads.
func MaxBodyMiddleware(maxBytes int64, next http.Handler) http.Handler {
	if maxBytes <= 0 {
		return next
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, maxBytes)
		next.ServeHTTP(w, r)
	})
}

// RateLimitMiddleware applies rate limiting to all requests.
func RateLimitMiddleware(rl *RateLimiter, next http.Handler) http.Handler {
	if rl == nil {
		return next
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// /health is exempt.
		if r.URL.Path == "/health" {
			next.ServeHTTP(w, r)

			return
		}

		ip, _, err := net.SplitHostPort(r.RemoteAddr)
		if err != nil {
			ip = r.RemoteAddr
		}

		if !rl.Allow(ip) {
			w.Header().Set("Retry-After", "1")
			respondError(w, ErrCodeTooManyRequests, http.StatusTooManyRequests, "rate limit exceeded")

			return
		}
		next.ServeHTTP(w, r)
	})
}

// RecoveryMiddleware catches panics in downstream handlers, logs the stack
// trace, and returns a 500 Internal Server Error. This prevents a single
// panicking handler from crashing the entire server process.
func RecoveryMiddleware(logger *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				// Capture stack trace for debugging.
				buf := make([]byte, 4096)
				n := runtime.Stack(buf, false)
				if logger != nil {
					logger.Error("panic recovered in HTTP handler",
						"panic", fmt.Sprintf("%v", rec),
						"method", r.Method,
						"path", r.URL.Path,
						"stack", string(buf[:n]),
					)
				}
				respondError(w, ErrCodeInternalError, http.StatusInternalServerError, "internal server error")
			}
		}()
		next.ServeHTTP(w, r)
	})
}
