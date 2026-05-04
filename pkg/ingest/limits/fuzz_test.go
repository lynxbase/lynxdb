package limits

import (
	"bytes"
	"compress/gzip"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func FuzzLimits_GzipBomb(f *testing.F) {
	f.Add([]byte("hello"))
	f.Add(bytes.Repeat([]byte("x"), 1024))

	f.Fuzz(func(t *testing.T, payload []byte) {
		if len(payload) > 4096 {
			t.Skip("keep default fuzz execution bounded")
		}
		encoded := gzipBytesForFuzz(t, payload)
		handler := DualLimitMiddleware(Config{
			MaxCompressedBytes:   1 << 20,
			MaxDecompressedBytes: 512,
		}, nil)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, err := io.ReadAll(r.Body)
			if err != nil && !IsTooLarge(err) {
				t.Fatalf("ReadAll: %v", err)
			}
			w.WriteHeader(http.StatusOK)
		}))

		req := httptest.NewRequest(http.MethodPost, "/_bulk", bytes.NewReader(encoded))
		req.Header.Set("Content-Encoding", "gzip")
		rr := httptest.NewRecorder()

		handler.ServeHTTP(rr, req)
		if len(payload) > 512 && rr.Code != http.StatusRequestEntityTooLarge {
			t.Fatalf("status = %d, want 413 for decompressed length %d", rr.Code, len(payload))
		}
	})
}

func gzipBytesForFuzz(t *testing.T, in []byte) []byte {
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
