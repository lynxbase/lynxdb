package main

import (
	"bytes"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/client"
)

func TestDoctorShippers_AllHealthy(t *testing.T) {
	baseURL := newTestServer(t)
	body := []byte(`{"index":{"_index":"logs"}}
{"message":"doctor shippers hello"}
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

	stdout, _, err := runCmd(t, "--server", baseURL, "doctor", "shippers")
	if err != nil {
		t.Fatalf("runCmd: %v", err)
	}
	for _, want := range []string{"shipper diagnostics", "ES bulk", "bound", "filebeat/8.15.0"} {
		if !strings.Contains(stdout, want) {
			t.Fatalf("doctor output missing %q:\n%s", want, stdout)
		}
	}
}

func TestDoctorShippers_FilebeatVersionMismatchWarning(t *testing.T) {
	report := buildShipperDoctorReport(nil, []client.ShipperObservation{
		{Tool: "filebeat", Version: "8.15.0", Endpoint: "/_bulk", LastSeenAt: time.Now()},
	}, shipperDoctorContext{ESAdvertisedVersion: "7.17.0"})

	if len(report.Warnings) == 0 || !strings.Contains(strings.Join(report.Warnings, "\n"), "Filebeat 8.x") {
		t.Fatalf("warnings = %#v, want Filebeat mismatch warning", report.Warnings)
	}
}

func TestDoctorShippers_StagingPressureWarning(t *testing.T) {
	report := buildShipperDoctorReport(nil, []client.ShipperObservation{
		{Tool: "splunk-hec", Endpoint: "/services/collector/event", LastSeenAt: time.Now()},
	}, shipperDoctorContext{
		Metrics:         map[string]float64{"lynxdb_ingest_staging_bytes": 90},
		StagingMaxBytes: 100,
	})

	if len(report.Warnings) == 0 || !strings.Contains(strings.Join(report.Warnings, "\n"), "staging buffer 90% full") {
		t.Fatalf("warnings = %#v, want staging pressure warning", report.Warnings)
	}
}

func TestDoctorShippers_TLSPlainHTTPDiagnosis(t *testing.T) {
	err := formatShipperDoctorConnectError(fmt.Errorf("Get \"https://127.0.0.1:3100/metrics\": http: server gave HTTP response to HTTPS client"))

	if !strings.Contains(err.Error(), "serving plain HTTP") {
		t.Fatalf("error = %q, want plain HTTP diagnosis", err.Error())
	}
}
