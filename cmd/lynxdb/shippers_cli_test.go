package main

import (
	"bytes"
	"fmt"
	"net/http"
	"strings"
	"testing"
)

func TestShippersConfigFilebeat_RendersRemote(t *testing.T) {
	stdout, _, err := runCmd(t, "shippers", "config", "filebeat", "--remote", "http://logs.example:3100")
	if err != nil {
		t.Fatalf("runCmd: %v", err)
	}
	if !strings.Contains(stdout, `hosts: ["http://logs.example:3100"]`) {
		t.Fatalf("filebeat config missing remote:\n%s", stdout)
	}
	if !strings.Contains(stdout, "allow_older_versions: true") {
		t.Fatalf("filebeat config missing compatibility hint:\n%s", stdout)
	}
}

func TestShippersConfigVector_ContainsZstdHint(t *testing.T) {
	stdout, _, err := runCmd(t, "shippers", "config", "vector", "--remote", "http://logs.example:3100")
	if err != nil {
		t.Fatalf("runCmd: %v", err)
	}
	if !strings.Contains(stdout, "compression: zstd") {
		t.Fatalf("vector config missing zstd hint:\n%s", stdout)
	}
}

func TestShippersConfigSplunkHEC_ContainsTokenHint(t *testing.T) {
	stdout, _, err := runCmd(t, "shippers", "config", "splunk-hec", "--remote", "http://logs.example:3100")
	if err != nil {
		t.Fatalf("runCmd: %v", err)
	}
	if !strings.Contains(stdout, "Authorization: Splunk changeme") {
		t.Fatalf("splunk config missing token hint:\n%s", stdout)
	}
}

func TestShippersList_NoServer_Errors(t *testing.T) {
	_, _, err := runCmd(t, "--server", "http://127.0.0.1:1", "shippers")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestShippersList_WithServer_PrintsTable(t *testing.T) {
	baseURL := newTestServer(t)
	body := []byte(`{"index":{"_index":"logs"}}
{"message":"cli shippers hello"}
`)
	req, _ := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/_bulk", baseURL), bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/x-ndjson")
	req.Header.Set("User-Agent", "Filebeat/8.15.0")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("bulk status = %d, want 200", resp.StatusCode)
	}

	stdout, _, err := runCmd(t, "--server", baseURL, "shippers")
	if err != nil {
		t.Fatalf("runCmd: %v", err)
	}
	for _, want := range []string{"TOOL", "filebeat", "8.15.0", "/_bulk"} {
		if !strings.Contains(stdout, want) {
			t.Fatalf("shippers output missing %q:\n%s", want, stdout)
		}
	}
}

func TestShippersTest_Filebeat_RoundTrip(t *testing.T) {
	baseURL := newTestServer(t)
	stdout, _, err := runCmd(t, "--server", baseURL, "shippers", "test", "filebeat")
	if err != nil {
		t.Fatalf("runCmd: %v", err)
	}
	if !strings.Contains(stdout, "OK filebeat roundtrip succeeded") {
		t.Fatalf("unexpected output:\n%s", stdout)
	}
}
