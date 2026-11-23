package rest

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/config"
)

func ndjsonBody(lines int) []byte {
	var b bytes.Buffer
	for i := 0; i < lines; i++ {
		fmt.Fprintf(&b, `{"n":%d,"msg":"%s"}`+"\n", i, strings.Repeat("x", 40))
	}

	return b.Bytes()
}

func TestNativeIngestTakesBodiesUpToMaxBodySize(t *testing.T) {
	var cfg Config
	cfg.Ingest.MaxBodySize = config.ByteSize(1 << 20)
	cfg.Ingest.Limits.MaxCompressedBodyBytes = config.ByteSize(4 << 10)
	cfg.Ingest.Limits.MaxDecompressedBodyBytes = config.ByteSize(8 << 10)
	srv, cleanup := startTestServerWithConfig(t, cfg)
	defer cleanup()
	post := func(path string, body []byte) (int, string) {
		t.Helper()
		resp, err := http.Post("http://"+srv.Addr()+path, "application/x-ndjson", bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		out, _ := io.ReadAll(resp.Body)

		return resp.StatusCode, string(out)
	}

	between := ndjsonBody(1000)
	if len(between) <= 8<<10 || len(between) >= 1<<20 {
		t.Fatalf("body of %d bytes is not between the limits", len(between))
	}
	if code, out := post("/api/v1/ingest?index=web", between); code != http.StatusOK || !strings.Contains(out, `"accepted":1000`) {
		t.Fatalf("native ingest: %d %s", code, out)
	}
	if code, out := post("/_bulk", between); code != http.StatusRequestEntityTooLarge || !strings.Contains(out, "ingest.limits.max_compressed_body_bytes") {
		t.Fatalf("bulk ingest: %d %s", code, out)
	}
	if code, out := post("/api/v1/ingest", ndjsonBody(30000)); code != http.StatusRequestEntityTooLarge || !strings.Contains(out, "ingest.max_body_size") {
		t.Fatalf("native ingest over max_body_size: %d %s", code, out)
	}
}
