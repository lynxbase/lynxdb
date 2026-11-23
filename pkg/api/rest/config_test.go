package rest

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/config"
)

func TestConfig_Get(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	resp, err := http.Get(fmt.Sprintf("http://%s/api/v1/config", srv.Addr()))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Fatalf("status: %d", resp.StatusCode)
	}

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	data := result["data"].(map[string]interface{})
	if data["log_level"] == nil {
		t.Fatal("missing log_level in config")
	}
}

func TestConfig_GetReportsAuth(t *testing.T) {
	srv, _, rootToken, cleanup := startAuthServer(t)
	defer cleanup()

	resp, err := authReq("GET", fmt.Sprintf("http://%s/api/v1/config", srv.Addr()), rootToken, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	var result struct {
		Data struct {
			Auth struct {
				Enabled bool `json:"enabled"`
			} `json:"auth"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatal(err)
	}
	if !result.Data.Auth.Enabled {
		t.Error("auth.enabled = false, want true")
	}
}

func TestNewServer_AuthWithoutKeyStoreFails(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Auth.Enabled = true
	cfg.DataDir = t.TempDir()

	srv, err := NewServer(Config{Addr: "127.0.0.1:0", DataDir: cfg.DataDir, RuntimeConfig: cfg, Logger: testLogger()})
	if err == nil {
		srv.closeDB()
		t.Fatal("NewServer with auth on and no key store should fail")
	}
}

func TestConfig_Patch(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	body, _ := json.Marshal(map[string]interface{}{
		"log_level": "debug",
	})
	req, _ := http.NewRequest("PATCH", fmt.Sprintf("http://%s/api/v1/config", srv.Addr()), bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Fatalf("status: %d", resp.StatusCode)
	}

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	data := result["data"].(map[string]interface{})
	cfg := data["config"].(map[string]interface{})
	if cfg["log_level"] != "debug" {
		t.Fatalf("log_level: %v", cfg["log_level"])
	}
}

// patchConfig sends a PATCH /api/v1/config and returns the status and the
// decoded body.
func patchConfig(t *testing.T, srv *Server, body string) (int, map[string]interface{}) {
	t.Helper()
	req, _ := http.NewRequest("PATCH", fmt.Sprintf("http://%s/api/v1/config", srv.Addr()), strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatal(err)
	}

	return resp.StatusCode, result
}

func TestConfig_PatchRestartRequiredRejected(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	for _, tt := range []struct{ body, field string }{
		{`{"listen": "0.0.0.0:1"}`, "listen"},
		{`{"retention": "1h"}`, "retention"},
		{`{"query": {"max_concurrent": 1}}`, "query.max_concurrent"},
		{`{"ingest": {"limits": {"max_compressed_body_bytes": "1mb"}}}`, "ingest.limits.max_compressed_body_bytes"},
		{`{"ingest": {"es_compat": {"advertised_version": "8.17.0"}}}`, "ingest.es_compat.advertised_version"},
		{`{"log_level": "debug", "auth": {"enabled": true}}`, "auth.enabled"},
	} {
		status, result := patchConfig(t, srv, tt.body)
		if status != http.StatusBadRequest {
			t.Fatalf("PATCH %s: status %d, want 400", tt.body, status)
		}
		msg, _ := result["error"].(map[string]interface{})["message"].(string)
		if !strings.Contains(msg, "startup") || !strings.Contains(msg, tt.field) {
			t.Errorf("PATCH %s: message %q, want one that names %s and says it needs a restart", tt.body, msg, tt.field)
		}
	}

	resp, err := http.Get(fmt.Sprintf("http://%s/api/v1/config", srv.Addr()))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var got struct {
		Data config.Config `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	if got.Data.Listen != "127.0.0.1:0" || got.Data.LogLevel != "info" || got.Data.Auth.Enabled {
		t.Errorf("config after rejected patches: listen %q, log_level %q, auth %v", got.Data.Listen, got.Data.LogLevel, got.Data.Auth.Enabled)
	}
}

func TestConfig_PatchUnknownNestedKey(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	if status, _ := patchConfig(t, srv, `{"query": {"sync_timeot": 1}}`); status != http.StatusBadRequest {
		t.Fatalf("status: %d, want 400", status)
	}
}

func TestConfig_PatchEmpty(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	req, _ := http.NewRequest("PATCH", fmt.Sprintf("http://%s/api/v1/config", srv.Addr()), bytes.NewReader([]byte(`{}`)))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 400 {
		t.Fatalf("status: %d, want 400", resp.StatusCode)
	}
}

func TestConfig_PatchAppliesLogLevel(t *testing.T) {
	var levelVar slog.LevelVar
	levelVar.Set(slog.LevelInfo)

	logger := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: &levelVar}))
	srv, cleanup := startTestServerWithConfig(t, Config{
		Logger:   logger,
		LevelVar: &levelVar,
	})
	defer cleanup()

	body, _ := json.Marshal(map[string]interface{}{"log_level": "debug"})
	req, _ := http.NewRequest("PATCH", fmt.Sprintf("http://%s/api/v1/config", srv.Addr()), bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Fatalf("status: %d", resp.StatusCode)
	}

	if levelVar.Level() != slog.LevelDebug {
		t.Fatalf("levelVar = %v, want Debug — PATCH did not propagate log_level", levelVar.Level())
	}
}

func TestConfig_PatchUnknownKey(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	body, _ := json.Marshal(map[string]interface{}{
		"unknown_field": "value",
	})
	req, _ := http.NewRequest("PATCH", fmt.Sprintf("http://%s/api/v1/config", srv.Addr()), bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 400 {
		t.Fatalf("status: %d, want 400", resp.StatusCode)
	}
}

func TestReloadConfigReportsStartupTimeFields(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	srv.cfgMu.RLock()
	updated := *srv.runtimeCfg
	srv.cfgMu.RUnlock()

	updated.TLS.Enabled = !updated.TLS.Enabled
	updated.Auth.Enabled = !updated.Auth.Enabled
	updated.DataDir = t.TempDir()
	updated.Storage.Partition = config.Duration(time.Hour)
	updated.Storage.FlushInterval = config.Duration(time.Second)
	updated.Storage.TimeFields = []string{"ts"}

	restartRequired, err := srv.ReloadConfig(&updated)
	if err != nil {
		t.Fatalf("ReloadConfig: %v", err)
	}

	for _, field := range []string{"tls.enabled", "auth.enabled", "storage.partition", "storage.flush_interval", "storage.time_fields"} {
		found := false
		for _, got := range restartRequired {
			if got == field {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("restart_required missing %q in %v", field, restartRequired)
		}
	}
}

func TestServerTakesStorageTimeFields(t *testing.T) {
	srv, cleanup := startTestServerWithConfig(t, Config{Storage: config.StorageConfig{
		Partition:  config.Duration(time.Hour),
		TimeFields: []string{"ts"},
	}})
	defer cleanup()

	ingestNDJSON(t, srv.Addr(), "main", `{"timestamp":"2020-01-01T00:00:00Z","ts":"2026-01-02T03:04:05Z"}`+"\n")

	events := queryEvents(t, srv.Addr(), `{"q":"source=main"}`)
	if len(events) != 1 {
		t.Fatalf("events: got %d, want 1", len(events))
	}
	if events[0]["_time"] != "2026-01-02T03:04:05Z" {
		t.Errorf("_time = %v, want the value of ts", events[0]["_time"])
	}
}

func TestServerWritesPartsInSortKeyOrder(t *testing.T) {
	srv, cleanup := startTestServerWithConfig(t, Config{Storage: config.StorageConfig{
		SortKey: []string{"kind", "did"},
	}})
	defer cleanup()

	ingestNDJSON(t, srv.Addr(), "main", `{"kind":"b","did":"x"}`+"\n"+`{"kind":"a","did":"y"}`+"\n")
	if err := srv.db.Sync(); err != nil {
		t.Fatal(err)
	}
	snap, ok := srv.db.Snapshot("main")
	if !ok {
		t.Fatal("no index main")
	}
	defer snap.Release()
	if len(snap.Parts()) == 0 {
		t.Fatal("no parts")
	}
	for _, h := range snap.Parts() {
		if got := strings.Join(h.SortKey(), ","); got != "kind,did" {
			t.Errorf("part sort key = %q, want kind,did", got)
		}
	}
}

func TestReloadConfigReportsDatabaseFields(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	srv.cfgMu.RLock()
	updated := *srv.runtimeCfg
	srv.cfgMu.RUnlock()

	updated.Retention += config.Duration(time.Hour)
	updated.Query.MaxConcurrent++
	updated.Query.JobTTL *= 2

	restartRequired, err := srv.ReloadConfig(&updated)
	if err != nil {
		t.Fatalf("ReloadConfig: %v", err)
	}
	for _, field := range []string{"retention", "query.max_concurrent", "query.job_ttl"} {
		if !slices.Contains(restartRequired, field) {
			t.Errorf("restart_required missing %q in %v", field, restartRequired)
		}
	}

	// The running config keeps the values in effect.
	srv.cfgMu.RLock()
	running := *srv.runtimeCfg
	srv.cfgMu.RUnlock()
	if running.Retention == updated.Retention || running.Query.MaxConcurrent == updated.Query.MaxConcurrent ||
		running.Query.JobTTL == updated.Query.JobTTL {
		t.Errorf("running config took settings that need a restart: %+v", running)
	}
}

func TestReloadConfigAppliesQueryMaxLength(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	srv.cfgMu.RLock()
	updated := *srv.runtimeCfg
	srv.cfgMu.RUnlock()
	updated.Query.MaxQueryLength = 12

	if _, err := srv.ReloadConfig(&updated); err != nil {
		t.Fatalf("ReloadConfig: %v", err)
	}

	body, _ := json.Marshal(map[string]interface{}{
		"q": "source=main | head 1",
	})
	resp, err := http.Post(fmt.Sprintf("http://%s/api/v1/query", srv.Addr()), "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status: got %d, want 400", resp.StatusCode)
	}

	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	errObj, ok := result["error"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected structured error, got %v", result)
	}
	if errObj["code"] != "QUERY_TOO_LARGE" {
		t.Fatalf("code: got %v, want QUERY_TOO_LARGE", errObj["code"])
	}
}
