package limits

import (
	"bytes"
	"compress/gzip"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/klauspost/compress/snappy"
	"github.com/klauspost/compress/zstd"
)

type recordingHook struct {
	stage    string
	encoding string
	count    int
}

func (h *recordingHook) OnReject(stage, encoding string) {
	h.stage = stage
	h.encoding = encoding
	h.count++
}

func TestDualLimitMiddleware_CompressedContentLengthExceeded_Returns413(t *testing.T) {
	hook := &recordingHook{}
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("handler should not be called")
	})
	handler := DualLimitMiddleware(Config{MaxCompressedBytes: 4, MaxDecompressedBytes: 1024}, hook)(next)

	req := httptest.NewRequest(http.MethodPost, "/_bulk", bytes.NewReader([]byte("12345")))
	req.Header.Set("Content-Encoding", "gzip")
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusRequestEntityTooLarge)
	}
	if hook.count != 1 || hook.stage != StageCompressed || hook.encoding != "gzip" {
		t.Fatalf("hook = (%d, %q, %q), want compressed gzip", hook.count, hook.stage, hook.encoding)
	}
}

func TestDualLimitMiddleware_DecompressedExceeded_Returns413(t *testing.T) {
	hook := &recordingHook{}
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("accepted"))
	})
	handler := DualLimitMiddleware(Config{MaxCompressedBytes: 1024, MaxDecompressedBytes: 8}, hook)(next)

	req := httptest.NewRequest(http.MethodPost, "/_bulk", bytes.NewReader(gzipBytes(t, []byte("this expands past the limit"))))
	req.Header.Set("Content-Encoding", "gzip")
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusRequestEntityTooLarge)
	}
	if hook.count != 1 || hook.stage != StageDecompressed || hook.encoding != "gzip" {
		t.Fatalf("hook = (%d, %q, %q), want decompressed gzip", hook.count, hook.stage, hook.encoding)
	}
}

func TestDualLimitMiddleware_UnsupportedEncoding_Returns400(t *testing.T) {
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("handler should not be called")
	})
	handler := DualLimitMiddleware(Config{MaxCompressedBytes: 1024, MaxDecompressedBytes: 1024}, nil)(next)

	req := httptest.NewRequest(http.MethodPost, "/_bulk", bytes.NewReader([]byte("body")))
	req.Header.Set("Content-Encoding", "br")
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusBadRequest)
	}
}

func TestDualLimitMiddleware_GzipBody_DecodedOnce(t *testing.T) {
	assertEncodedBodyDecodedOnce(t, "gzip", gzipBytes(t, []byte("hello")))
}

func TestDualLimitMiddleware_ZstdBody_DecodedOnce(t *testing.T) {
	assertEncodedBodyDecodedOnce(t, "zstd", zstdBytes(t, []byte("hello")))
}

func TestDualLimitMiddleware_SnappyBody_DecodedOnce(t *testing.T) {
	assertEncodedBodyDecodedOnce(t, "snappy", snappyBytes(t, []byte("hello")))
}

func assertEncodedBodyDecodedOnce(t *testing.T, encoding string, encoded []byte) {
	t.Helper()
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Content-Encoding"); got != "" {
			t.Fatalf("Content-Encoding = %q, want cleared", got)
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("ReadAll: %v", err)
		}
		if string(body) != "hello" {
			t.Fatalf("body = %q, want hello", body)
		}
		w.WriteHeader(http.StatusAccepted)
	})
	handler := DualLimitMiddleware(Config{MaxCompressedBytes: 1024, MaxDecompressedBytes: 1024}, nil)(next)

	req := httptest.NewRequest(http.MethodPost, "/_bulk", bytes.NewReader(encoded))
	req.Header.Set("Content-Encoding", encoding)
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusAccepted)
	}
}

func TestCapReadCloser_LimitReaderCounts_NoLeak(t *testing.T) {
	state := &state{}
	reader := newCapReadCloser(io.NopCloser(bytes.NewReader([]byte("abcdef"))), 3, StageDecompressed, "identity", nil, state)

	buf := make([]byte, 4)
	n, err := reader.Read(buf)
	if n != 0 || !IsTooLarge(err) {
		t.Fatalf("first read = (%d, %v), want too large with no bytes", n, err)
	}

	n, err = reader.Read(buf)
	if n != 0 || !IsTooLarge(err) {
		t.Fatalf("second read = (%d, %v), want persistent too large", n, err)
	}
}

func gzipBytes(t *testing.T, in []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	if _, err := gz.Write(in); err != nil {
		t.Fatalf("gzip write: %v", err)
	}
	if err := gz.Close(); err != nil {
		t.Fatalf("gzip close: %v", err)
	}
	return buf.Bytes()
}

func zstdBytes(t *testing.T, in []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw, err := zstd.NewWriter(&buf)
	if err != nil {
		t.Fatalf("zstd writer: %v", err)
	}
	if _, err := zw.Write(in); err != nil {
		t.Fatalf("zstd write: %v", err)
	}
	if err := zw.Close(); err != nil {
		t.Fatalf("zstd close: %v", err)
	}
	return buf.Bytes()
}

func snappyBytes(t *testing.T, in []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	sw := snappy.NewBufferedWriter(&buf)
	if _, err := sw.Write(in); err != nil {
		t.Fatalf("snappy write: %v", err)
	}
	if err := sw.Close(); err != nil {
		t.Fatalf("snappy close: %v", err)
	}
	return buf.Bytes()
}
