package rest

import (
	"bytes"
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

// storedRows returns the rows the indexes hold now, without a Sync.
func storedRows(srv *Server) uint64 {
	var rows uint64
	for _, ix := range srv.DB().Stats() {
		rows += ix.Rows
	}

	return rows
}

// requireSingleEvent queries every index and checks that it holds one event
// with the text raw.
func requireSingleEvent(t *testing.T, srv *Server, raw string) map[string]interface{} {
	t.Helper()
	events := queryEvents(t, srv.Addr(), `{"q":"source=*"}`)
	if len(events) != 1 {
		t.Fatalf("events = %v, want one", events)
	}
	if events[0]["_raw"] != raw {
		t.Fatalf("_raw = %v, want %q", events[0]["_raw"], raw)
	}

	return events[0]
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
	ev := requireSingleEvent(t, srv, "hello")
	if ev["code"] != float64(200) {
		t.Fatalf("code = %v, want 200", ev["code"])
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
			requireSingleEvent(t, srv, "hello compressed")
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
	requireSingleEvent(t, srv, "hello legacy")
}

func TestIntegration_HEC_AckMode_RoundTrip(t *testing.T) {
	srv, cleanup := startHECTestServer(t)
	defer cleanup()

	req, _ := http.NewRequest(http.MethodPost,
		fmt.Sprintf("http://%s/services/collector/event", srv.Addr()),
		bytes.NewBufferString(`{"event":"ack hello","index":"splunk-main"}`),
	)
	req.Header.Set("Authorization", "Splunk token")
	req.Header.Set("X-Splunk-Request-Channel", "channel-a")
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
	var eventBody map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&eventBody); err != nil {
		t.Fatalf("decode event response: %v", err)
	}
	ackID, ok := eventBody["ackId"].(float64)
	if !ok || ackID == 0 {
		t.Fatalf("ackId = %#v, want positive number", eventBody["ackId"])
	}
	if got := storedRows(srv); got != 1 {
		t.Fatalf("stored rows when the ack is issued = %d, want 1", got)
	}

	ackReq, _ := http.NewRequest(http.MethodPost,
		fmt.Sprintf("http://%s/services/collector/ack", srv.Addr()),
		bytes.NewBufferString(fmt.Sprintf(`{"acks":[%d]}`, int(ackID))),
	)
	ackReq.Header.Set("Authorization", "Splunk token")
	ackReq.Header.Set("X-Splunk-Request-Channel", "channel-a")
	ackReq.Header.Set("Content-Type", "application/json")
	ackResp, err := http.DefaultClient.Do(ackReq)
	if err != nil {
		t.Fatal(err)
	}
	defer ackResp.Body.Close()
	if ackResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(ackResp.Body)
		t.Fatalf("ack status = %d, want 200, body=%s", ackResp.StatusCode, body)
	}
	var ackBody struct {
		Acks map[string]bool `json:"acks"`
	}
	if err := json.NewDecoder(ackResp.Body).Decode(&ackBody); err != nil {
		t.Fatalf("decode ack response: %v", err)
	}
	if !ackBody.Acks[fmt.Sprint(int(ackID))] {
		t.Fatalf("ack %d = false, want true", int(ackID))
	}
}
