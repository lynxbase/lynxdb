package limits

import (
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"

	"github.com/klauspost/compress/snappy"
	"github.com/klauspost/compress/zstd"
)

const (
	StageCompressed   = "compressed"
	StageDecompressed = "decompressed"
)

type Config struct {
	MaxCompressedBytes   int64
	MaxDecompressedBytes int64
}

type Hook interface {
	OnReject(stage, encoding string)
}

type TooLargeError struct {
	Stage    string
	Encoding string
	Limit    int64
}

func (e *TooLargeError) Error() string {
	if e.Encoding == "" {
		return fmt.Sprintf("%s body exceeds limit %d", e.Stage, e.Limit)
	}
	return fmt.Sprintf("%s body exceeds limit %d for %s encoding", e.Stage, e.Limit, e.Encoding)
}

func IsTooLarge(err error) bool {
	var tooLarge *TooLargeError
	return errors.As(err, &tooLarge)
}

func DualLimitMiddleware(cfg Config, hook Hook) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Body == nil || r.Body == http.NoBody {
				next.ServeHTTP(w, r)
				return
			}

			encoding := normalizeEncoding(r.Header.Get("Content-Encoding"))
			if cfg.MaxCompressedBytes > 0 && r.ContentLength > cfg.MaxCompressedBytes {
				recordReject(hook, StageCompressed, encoding)
				writeError(w, http.StatusRequestEntityTooLarge, (&TooLargeError{
					Stage:    StageCompressed,
					Encoding: encoding,
					Limit:    cfg.MaxCompressedBytes,
				}).Error())
				return
			}

			state := &state{}
			wrapped, err := WrapBody(r.Body, encoding, cfg, hook, state)
			if err != nil {
				status := http.StatusBadRequest
				if IsTooLarge(err) {
					status = http.StatusRequestEntityTooLarge
				}
				writeError(w, status, err.Error())
				return
			}

			r.Body = wrapped
			r.ContentLength = -1
			r.Header.Del("Content-Encoding")

			if encoding == "" || encoding == "identity" {
				next.ServeHTTP(w, r)
				return
			}

			rw := newCaptureResponseWriter(w)
			next.ServeHTTP(rw, r)
			if tooLarge := state.tooLarge(); tooLarge != nil {
				writeError(w, http.StatusRequestEntityTooLarge, tooLarge.Error())
				return
			}
			rw.FlushTo(w)
		})
	}
}

func WrapBody(body io.ReadCloser, encoding string, cfg Config, hook Hook, shared *state) (io.ReadCloser, error) {
	if shared == nil {
		shared = &state{}
	}

	raw := newCapReadCloser(body, cfg.MaxCompressedBytes, StageCompressed, encoding, hook, shared)
	var decoded io.ReadCloser = raw

	switch encoding {
	case "", "identity":
	case "gzip":
		gz, err := gzip.NewReader(raw)
		if err != nil {
			_ = raw.Close()
			return nil, fmt.Errorf("gzip decode: %w", err)
		}
		decoded = &joinedReadCloser{Reader: gz, closers: []io.Closer{gz, raw}}
	case "zstd":
		zr, err := zstd.NewReader(raw)
		if err != nil {
			_ = raw.Close()
			return nil, fmt.Errorf("zstd decode: %w", err)
		}
		decoded = &joinedReadCloser{Reader: zr, closers: []io.Closer{zstdCloser{zr}, raw}}
	case "snappy":
		decoded = &joinedReadCloser{Reader: snappy.NewReader(raw), closers: []io.Closer{raw}}
	default:
		_ = raw.Close()
		return nil, fmt.Errorf("unsupported content encoding %q", encoding)
	}

	return newCapReadCloser(decoded, cfg.MaxDecompressedBytes, StageDecompressed, encoding, hook, shared), nil
}

type state struct {
	err atomic.Pointer[TooLargeError]
}

func (s *state) setTooLarge(err *TooLargeError) {
	s.err.CompareAndSwap(nil, err)
}

func (s *state) tooLarge() *TooLargeError {
	return s.err.Load()
}

type capReadCloser struct {
	r        io.ReadCloser
	limit    int64
	read     int64
	stage    string
	encoding string
	hook     Hook
	state    *state
}

func newCapReadCloser(r io.ReadCloser, limit int64, stage, encoding string, hook Hook, shared *state) *capReadCloser {
	return &capReadCloser{r: r, limit: limit, stage: stage, encoding: encoding, hook: hook, state: shared}
}

func (r *capReadCloser) Read(p []byte) (int, error) {
	if r.limit <= 0 {
		return r.r.Read(p)
	}
	if r.state.tooLarge() != nil {
		return 0, r.state.tooLarge()
	}
	if r.read >= r.limit {
		var one [1]byte
		n, err := r.r.Read(one[:])
		if n > 0 {
			return 0, r.reject()
		}
		return 0, err
	}

	remaining := r.limit - r.read
	if int64(len(p)) > remaining+1 {
		p = p[:remaining+1]
	}
	n, err := r.r.Read(p)
	if n == 0 {
		return 0, err
	}
	if r.read+int64(n) > r.limit {
		return 0, r.reject()
	}
	r.read += int64(n)
	return n, err
}

func (r *capReadCloser) Close() error {
	return r.r.Close()
}

func (r *capReadCloser) reject() error {
	err := &TooLargeError{Stage: r.stage, Encoding: r.encoding, Limit: r.limit}
	r.state.setTooLarge(err)
	recordReject(r.hook, r.stage, r.encoding)
	return err
}

type joinedReadCloser struct {
	io.Reader
	closers []io.Closer
}

type zstdCloser struct {
	*zstd.Decoder
}

func (c zstdCloser) Close() error {
	c.Decoder.Close()
	return nil
}

func (r *joinedReadCloser) Close() error {
	var first error
	for _, c := range r.closers {
		if err := c.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

type captureResponseWriter struct {
	header http.Header
	code   int
	body   []byte
}

func newCaptureResponseWriter(w http.ResponseWriter) *captureResponseWriter {
	h := make(http.Header)
	for k, values := range w.Header() {
		h[k] = append([]string(nil), values...)
	}
	return &captureResponseWriter{header: h}
}

func (w *captureResponseWriter) Header() http.Header {
	return w.header
}

func (w *captureResponseWriter) WriteHeader(code int) {
	if w.code == 0 {
		w.code = code
	}
}

func (w *captureResponseWriter) Write(p []byte) (int, error) {
	if w.code == 0 {
		w.code = http.StatusOK
	}
	w.body = append(w.body, p...)
	return len(p), nil
}

func (w *captureResponseWriter) FlushTo(dst http.ResponseWriter) {
	for k, values := range w.header {
		dst.Header()[k] = append([]string(nil), values...)
	}
	if w.code == 0 {
		w.code = http.StatusOK
	}
	dst.WriteHeader(w.code)
	_, _ = dst.Write(w.body)
}

func normalizeEncoding(v string) string {
	return strings.ToLower(strings.TrimSpace(v))
}

func recordReject(hook Hook, stage, encoding string) {
	if hook != nil {
		hook.OnReject(stage, encoding)
	}
}

func writeError(w http.ResponseWriter, status int, reason string) {
	errType := "invalid_request"
	if status == http.StatusRequestEntityTooLarge {
		errType = "request_body_too_large"
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"error": map[string]interface{}{
			"type":   errType,
			"reason": reason,
		},
		"status": status,
	})
}
