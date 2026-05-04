package rest

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/config"
)

func startHECTestServer(t *testing.T) (*Server, func()) {
	t.Helper()

	ingestCfg := config.DefaultConfig().Ingest
	ingestCfg.OTLP.HTTPListen = ""
	ingestCfg.OTLP.GRPCListen = ""
	return startTestServerWithConfig(t, Config{Ingest: ingestCfg})
}

func TestIntegration_HEC_Health_Returns200(t *testing.T) {
	srv, cleanup := startHECTestServer(t)
	defer cleanup()

	resp, err := http.Get(fmt.Sprintf("http://%s/services/collector/health", srv.Addr()))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var body map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body["code"] != float64(17) {
		t.Fatalf("code = %#v, want 17", body["code"])
	}
}

func TestIntegration_HEC_CanonicalRequiresSplunkToken(t *testing.T) {
	srv, cleanup := startHECTestServer(t)
	defer cleanup()

	resp, err := http.Post(
		fmt.Sprintf("http://%s/services/collector/event", srv.Addr()),
		"application/json",
		bytes.NewBufferString(`{"event":"hello"}`),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusUnauthorized {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 401, body=%s", resp.StatusCode, body)
	}
}

func TestIntegration_HEC_CanonicalIngestsWithSplunkToken(t *testing.T) {
	srv, cleanup := startHECTestServer(t)
	defer cleanup()

	req, _ := http.NewRequest(http.MethodPost,
		fmt.Sprintf("http://%s/services/collector/event", srv.Addr()),
		bytes.NewBufferString(`{"event":"hello","index":"splunk-main","fields":{"code":200}}`),
	)
	req.Header.Set("Authorization", "Splunk token")
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 200, body=%s", resp.StatusCode, body)
	}
	if srv.stagingBuffer == nil {
		t.Fatal("expected HEC ingest to use staging buffer")
	}
	if err := srv.stagingBuffer.Flush(context.Background()); err != nil {
		t.Fatalf("flush staging: %v", err)
	}
	if srv.engine.SegmentCount() == 0 {
		t.Fatal("expected segment after HEC ingest")
	}
}

func TestIntegration_HEC_CompressedBodies_Decoded(t *testing.T) {
	for _, encoding := range []string{"gzip", "zstd"} {
		t.Run(encoding, func(t *testing.T) {
			srv, cleanup := startHECTestServer(t)
			defer cleanup()

			body := []byte(`{"event":"hello compressed","index":"splunk-main"}`)
			req, _ := http.NewRequest(http.MethodPost,
				fmt.Sprintf("http://%s/services/collector/event", srv.Addr()),
				encodeTestBody(t, encoding, body),
			)
			req.Header.Set("Authorization", "Splunk token")
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Content-Encoding", encoding)
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				body, _ := io.ReadAll(resp.Body)
				t.Fatalf("status = %d, want 200, body=%s", resp.StatusCode, body)
			}
			if err := srv.stagingBuffer.Flush(context.Background()); err != nil {
				t.Fatalf("flush staging: %v", err)
			}
			if srv.engine.SegmentCount() == 0 {
				t.Fatal("expected segment after compressed HEC ingest")
			}
		})
	}
}

func TestIntegration_HEC_LegacyAlias_StillWorksWithoutSplunkToken(t *testing.T) {
	srv, cleanup := startHECTestServer(t)
	defer cleanup()

	resp, err := http.Post(
		fmt.Sprintf("http://%s/api/v1/ingest/hec", srv.Addr()),
		"application/json",
		bytes.NewBufferString(`{"event":"hello legacy","index":"legacy"}`),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 200, body=%s", resp.StatusCode, body)
	}
}
