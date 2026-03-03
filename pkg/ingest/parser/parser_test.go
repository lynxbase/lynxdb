package parser

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func testdataDir() string {
	_, filename, _, _ := runtime.Caller(0)

	return filepath.Join(filepath.Dir(filename), "..", "..", "..", "testdata", "logs")
}

func TestParseNginx(t *testing.T) {
	line := `192.168.1.42 - john.doe [10/Feb/2026:08:12:03 +0000] "GET /api/v2/users/profile HTTP/1.1" 200 1243 "https://app.example.com/dashboard" "Mozilla/5.0" rt=0.032`
	row, err := parseNginx(line, "idx_nginx")
	if err != nil {
		t.Fatalf("parseNginx: %v", err)
	}

	tests := []struct {
		field string
		want  interface{}
	}{
		{"clientip", "192.168.1.42"},
		{"user", "john.doe"},
		{"method", "GET"},
		{"uri_path", "/api/v2/users/profile"},
		{"status", int64(200)},
		{"bytes", int64(1243)},
		{"response_time", 0.032},
		{"index", "idx_nginx"},
	}

	for _, tt := range tests {
		got := row.Fields[tt.field]
		if got != tt.want {
			t.Errorf("field %s: got %v (%T), want %v (%T)", tt.field, got, got, tt.want, tt.want)
		}
	}
}

func TestParseNginxQueryString(t *testing.T) {
	line := `203.0.113.77 - - [10/Feb/2026:09:01:15 +0000] "GET /api/v2/users/search?q=admin'%20OR%201=1--&type=all HTTP/1.1" 400 89 "-" "sqlmap/1.7" rt=0.001`
	row, err := parseNginx(line, "idx_nginx")
	if err != nil {
		t.Fatalf("parseNginx: %v", err)
	}

	if row.Fields["status"] != int64(400) {
		t.Errorf("status: got %v, want 400", row.Fields["status"])
	}
	if row.Fields["request_args"] == nil || row.Fields["request_args"] == "" {
		t.Error("request_args should be populated for URL with query string")
	}
}

func TestParseAudit(t *testing.T) {
	line := `2026-02-10T08:00:01.123Z audit[1201]: type=USER_LOGIN msg=audit(1707552001.123:4401): pid=1201 uid=0 auid=1001 ses=301 subj=unconfined msg='op=login id=1001 exe="/usr/sbin/sshd" hostname=workstation-jd addr=192.168.1.42 terminal=ssh res=success'`
	row, err := parseAudit(line, "idx_audit")
	if err != nil {
		t.Fatalf("parseAudit: %v", err)
	}

	if row.Fields["type"] != "USER_LOGIN" {
		t.Errorf("type: got %v, want USER_LOGIN", row.Fields["type"])
	}
	if row.Fields["res"] != "success" {
		t.Errorf("res: got %v, want success", row.Fields["res"])
	}
	if row.Fields["addr"] != "192.168.1.42" {
		t.Errorf("addr: got %v, want 192.168.1.42", row.Fields["addr"])
	}
	if row.Fields["index"] != "idx_audit" {
		t.Errorf("index: got %v, want idx_audit", row.Fields["index"])
	}
}

func TestParseJSON(t *testing.T) {
	line := `{"timestamp":"2026-02-10T08:12:03.045Z","level":"INFO","service":"user-service","status":200,"duration_ms":28,"memory_mb":245.3,"message":"Profile fetched"}`
	row, err := parseJSON(line, "idx_backend")
	if err != nil {
		t.Fatalf("parseJSON: %v", err)
	}

	if row.Fields["service"] != "user-service" {
		t.Errorf("service: got %v, want user-service", row.Fields["service"])
	}
	if row.Fields["status"] != int64(200) {
		t.Errorf("status: got %v (%T), want int64(200)", row.Fields["status"], row.Fields["status"])
	}
	if row.Fields["duration_ms"] != int64(28) {
		t.Errorf("duration_ms: got %v (%T), want int64(28)", row.Fields["duration_ms"], row.Fields["duration_ms"])
	}
	if row.Fields["memory_mb"] != 245.3 {
		t.Errorf("memory_mb: got %v, want 245.3", row.Fields["memory_mb"])
	}
}

func TestParseFrontend(t *testing.T) {
	line := `2026-02-10T08:11:55.001Z [INFO] [app-init] Application bootstrap started | version=3.14.2 | build=20260209-a7f3c1 | env=production`
	row, err := parseFrontend(line, "idx_frontend")
	if err != nil {
		t.Fatalf("parseFrontend: %v", err)
	}

	if row.Fields["level"] != "INFO" {
		t.Errorf("level: got %v, want INFO", row.Fields["level"])
	}
	if row.Fields["component"] != "app-init" {
		t.Errorf("component: got %v, want app-init", row.Fields["component"])
	}
	msg, _ := row.Fields["message"].(string)
	if !strings.Contains(msg, "bootstrap") {
		t.Errorf("message should contain 'bootstrap': got %v", msg)
	}
	if row.Fields["env"] != "production" {
		t.Errorf("env: got %v, want production", row.Fields["env"])
	}
}

func TestParseTransactions(t *testing.T) {
	line := `2026-02-10T08:00:00.000Z|TXN-100001|SYSTEM|BATCH_OPEN|status=initiated|batch_id=BATCH-20260210-001|description="Daily reconciliation batch opened"|source_system=ledger-core|approver=SYSTEM|ip=10.0.1.1|risk_score=0`
	row, err := parseTransactions(line, "idx_transactions")
	if err != nil {
		t.Fatalf("parseTransactions: %v", err)
	}

	if row.Fields["txn_id"] != "TXN-100001" {
		t.Errorf("txn_id: got %v, want TXN-100001", row.Fields["txn_id"])
	}
	if row.Fields["actor"] != "SYSTEM" {
		t.Errorf("actor: got %v, want SYSTEM", row.Fields["actor"])
	}
	if row.Fields["action_type"] != "BATCH_OPEN" {
		t.Errorf("action_type: got %v, want BATCH_OPEN", row.Fields["action_type"])
	}
	if row.Fields["risk_score"] != int64(0) {
		t.Errorf("risk_score: got %v (%T), want int64(0)", row.Fields["risk_score"], row.Fields["risk_score"])
	}
}

func TestLoadAllIndexes(t *testing.T) {
	dir := testdataDir()
	if _, err := os.Stat(dir); err != nil {
		t.Skipf("testdata/logs not found: %v", err)
	}

	store, err := LoadAllIndexes(dir)
	if err != nil {
		t.Fatalf("LoadAllIndexes: %v", err)
	}

	expected := map[string]int{
		"idx_nginx":        34,
		"idx_audit":        27,
		"idx_backend":      26,
		"idx_frontend":     37,
		"idx_transactions": 20,
	}

	for name, minCount := range expected {
		rows, ok := store.Indexes[name]
		if !ok {
			t.Errorf("missing index %s", name)

			continue
		}
		if len(rows) < minCount-5 { // Allow some tolerance for parsing failures
			t.Errorf("%s: got %d rows, expected at least %d", name, len(rows), minCount-5)
		}
		t.Logf("%s: %d rows", name, len(rows))
	}
}

func TestLoadIndex_NginxFile(t *testing.T) {
	dir := testdataDir()
	path := filepath.Join(dir, "nginx_access.log")
	if _, err := os.Stat(path); err != nil {
		t.Skipf("test file not found: %v", err)
	}

	rows, err := LoadIndex(path, "idx_nginx", "nginx")
	if err != nil {
		t.Fatalf("LoadIndex: %v", err)
	}

	if len(rows) == 0 {
		t.Fatal("expected at least 1 row")
	}

	// All rows should have _raw and clientip.
	for i, row := range rows {
		if row.Fields["_raw"] == nil {
			t.Errorf("row %d: missing _raw", i)
		}
		if row.Fields["clientip"] == nil {
			t.Errorf("row %d: missing clientip", i)
		}
	}
}

func TestLoadIndex_UnknownFormat(t *testing.T) {
	_, err := parseLine("test line", "test_idx", "unknown_format")
	if err == nil {
		t.Error("expected error for unknown format")
	}
}
