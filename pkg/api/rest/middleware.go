package rest

import (
	"container/heap"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

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
// This is required for SSE (Server-Sent Events) streaming endpoints like /tail.
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

// KeyAuthMiddleware checks for Bearer token on all routes (except /health)
// when a KeyStore is provided. Verifies the token against stored argon2id
// hashes and attaches the authenticated key info to the request context.
//
// When ks is nil, auth is disabled and all requests pass through.
func KeyAuthMiddleware(ks *auth.KeyStore, next http.Handler) http.Handler {
	if ks == nil {
		return next
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// /health is always exempt from auth.
		if r.URL.Path == "/health" {
			next.ServeHTTP(w, r)

			return
		}

		header := r.Header.Get("Authorization")
		if header == "" {
			respondError(w, ErrCodeAuthRequired, http.StatusUnauthorized,
				"Authentication required. Provide: Authorization: Bearer <key>",
				WithSuggestion("Run 'lynxdb login' or set LYNXDB_TOKEN environment variable"))

			return
		}

		if !strings.HasPrefix(header, "Bearer ") {
			respondError(w, ErrCodeInvalidToken, http.StatusUnauthorized, "Invalid API key")

			return
		}

		token := header[7:]
		info := ks.Verify(token)

		if info == nil {
			respondError(w, ErrCodeInvalidToken, http.StatusUnauthorized, "Invalid API key")

			return
		}

		// Attach authenticated key info to context.
		ctx := auth.WithKeyInfo(r.Context(), info)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
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
	// Update heap position since lastTime changed.
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
