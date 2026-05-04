package rest

import (
	"bytes"
	"compress/gzip"
	"testing"

	"github.com/klauspost/compress/snappy"
	"github.com/klauspost/compress/zstd"
)

func encodeTestBody(t *testing.T, encoding string, body []byte) *bytes.Buffer {
	t.Helper()

	var buf bytes.Buffer
	switch encoding {
	case "", "identity":
		buf.Write(body)
	case "gzip":
		zw := gzip.NewWriter(&buf)
		if _, err := zw.Write(body); err != nil {
			t.Fatalf("gzip write: %v", err)
		}
		if err := zw.Close(); err != nil {
			t.Fatalf("gzip close: %v", err)
		}
	case "zstd":
		zw, err := zstd.NewWriter(&buf)
		if err != nil {
			t.Fatalf("zstd writer: %v", err)
		}
		if _, err := zw.Write(body); err != nil {
			t.Fatalf("zstd write: %v", err)
		}
		if err := zw.Close(); err != nil {
			t.Fatalf("zstd close: %v", err)
		}
	case "snappy":
		zw := snappy.NewBufferedWriter(&buf)
		if _, err := zw.Write(body); err != nil {
			t.Fatalf("snappy write: %v", err)
		}
		if err := zw.Close(); err != nil {
			t.Fatalf("snappy close: %v", err)
		}
	default:
		t.Fatalf("unsupported test encoding %q", encoding)
	}
	return &buf
}
