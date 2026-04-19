package rest

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
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

func TestConfig_PatchRestartRequired(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	body, _ := json.Marshal(map[string]interface{}{
		"listen": "0.0.0.0:4000",
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
	restart, ok := data["restart_required"].([]interface{})
	if !ok || len(restart) == 0 {
		t.Fatal("expected restart_required")
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
	srv, err := NewServer(Config{
		Addr:     "127.0.0.1:0",
		Logger:   logger,
		LevelVar: &levelVar,
		Query:    config.QueryConfig{SpillDir: t.TempDir()},
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	go srv.Start(ctx)
	srv.WaitReady()
	defer func() {
		cancel()
		time.Sleep(50 * time.Millisecond)
	}()

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

	updated.NoUI = !updated.NoUI
	updated.TLS.Enabled = !updated.TLS.Enabled
	updated.Auth.Enabled = !updated.Auth.Enabled
	updated.Storage.CompactionWorkers++

	restartRequired, err := srv.ReloadConfig(&updated)
	if err != nil {
		t.Fatalf("ReloadConfig: %v", err)
	}

	for _, field := range []string{"no_ui", "tls.enabled", "auth.enabled", "storage.compaction_workers"} {
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
		"q": "FROM main | head 1",
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
