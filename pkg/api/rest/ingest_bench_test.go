package rest

import (
	"bytes"
	"compress/gzip"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/klauspost/compress/zstd"

	"github.com/lynxbase/lynxdb/internal/benchdata"
	"github.com/lynxbase/lynxdb/pkg/config"
)

// BenchmarkIngestEvents posts 64 MB of generated events to /api/v1/ingest
// through the middleware chain as a body of unknown length, as curl -T -
// sends it, plain and compressed. The time is the time of the request; the
// buffers are written between requests.
func BenchmarkIngestEvents(b *testing.B) {
	data := benchdata.New(1, 1<<20).Append(nil, 150000)
	for _, encoding := range []string{"identity", "gzip", "zstd"} {
		b.Run(encoding, func(b *testing.B) {
			body := encodeBenchBody(b, encoding, data)
			cfg := Config{Logger: testLogger()}
			cfg.Ingest.MaxBodySize = config.ByteSize(1 << 30)
			cfg.Storage.SortKey = []string{"kind", "commit.operation", "commit.collection", "did"}
			srv, err := NewServer(cfg)
			if err != nil {
				b.Fatal(err)
			}
			defer srv.db.Close()
			handler := srv.httpServer.Handler
			b.SetBytes(int64(len(data)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				req := httptest.NewRequest(http.MethodPost, "/api/v1/ingest?index=bench", struct{ io.Reader }{bytes.NewReader(body)})
				req.ContentLength = -1
				if encoding != "identity" {
					req.Header.Set("Content-Encoding", encoding)
				}
				rec := httptest.NewRecorder()
				handler.ServeHTTP(rec, req)
				if rec.Code != http.StatusOK {
					b.Fatalf("status %d: %s", rec.Code, rec.Body.String())
				}
				b.StopTimer()
				if err := srv.db.Sync(); err != nil {
					b.Fatal(err)
				}
				b.StartTimer()
			}
		})
	}
}

func encodeBenchBody(b *testing.B, encoding string, data []byte) []byte {
	b.Helper()
	var buf bytes.Buffer
	switch encoding {
	case "identity":
		return data
	case "gzip":
		zw := gzip.NewWriter(&buf)
		_, _ = zw.Write(data)
		_ = zw.Close()
	case "zstd":
		zw, err := zstd.NewWriter(&buf)
		if err != nil {
			b.Fatal(err)
		}
		_, _ = zw.Write(data)
		_ = zw.Close()
	}

	return buf.Bytes()
}
