package rest

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/auth"
	"github.com/lynxbase/lynxdb/pkg/config"
)

func startAuthServer(t *testing.T) (*Server, *auth.KeyStore, string, func()) {
	t.Helper()

	dir := t.TempDir()

	ks, err := auth.OpenKeyStore(dir)
	if err != nil {
		t.Fatal(err)
	}

	root, err := ks.CreateKey("root", true)
	if err != nil {
		t.Fatal(err)
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	srv, err := NewServer(Config{
		Addr:     "127.0.0.1:0",
		DataDir:  dir,
		KeyStore: ks,
		Logger:   logger,
		Query:    config.QueryConfig{SpillDir: t.TempDir()},
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	go srv.Start(ctx)
	srv.WaitReady()

	return srv, ks, root.Token, func() {
		cancel()
		time.Sleep(50 * time.Millisecond)
	}
}

func authReq(method, url, token string, body []byte) (*http.Response, error) {
	var bodyReader *bytes.Reader
	if body != nil {
		bodyReader = bytes.NewReader(body)
	}

	var req *http.Request

	var err error

	if bodyReader != nil {
		req, err = http.NewRequest(method, url, bodyReader)
	} else {
		req, err = http.NewRequest(method, url, http.NoBody)
	}

	if err != nil {
		return nil, err
	}

	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	return http.DefaultClient.Do(req)
}

func TestAuth_HealthExempt(t *testing.T) {
	srv, _, _, cleanup := startAuthServer(t)
	defer cleanup()

	// /health should work without any token.
	resp, err := http.Get(fmt.Sprintf("http://%s/health", srv.Addr()))
	if err != nil {
		t.Fatal(err)
	}

	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
}

func TestAuth_RequiresToken(t *testing.T) {
	srv, _, _, cleanup := startAuthServer(t)
	defer cleanup()

	// Request without token should get 401.
	resp, err := http.Get(fmt.Sprintf("http://%s/api/v1/stats", srv.Addr()))
	if err != nil {
		t.Fatal(err)
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", resp.StatusCode)
	}

	var errResp struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&errResp); err != nil {
		t.Fatal(err)
	}

	if errResp.Error.Code != "AUTH_REQUIRED" {
		t.Errorf("code = %q, want AUTH_REQUIRED", errResp.Error.Code)
	}
}

func TestAuth_InvalidToken(t *testing.T) {
	srv, _, _, cleanup := startAuthServer(t)
	defer cleanup()

	resp, err := authReq("GET",
		fmt.Sprintf("http://%s/api/v1/stats", srv.Addr()),
		"lynx_rk_invalidinvalidinvalidinvalid", nil)
	if err != nil {
		t.Fatal(err)
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", resp.StatusCode)
	}

	var errResp struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&errResp); err != nil {
		t.Fatal(err)
	}

	if errResp.Error.Code != "INVALID_TOKEN" {
		t.Errorf("code = %q, want INVALID_TOKEN", errResp.Error.Code)
	}
}

func TestAuth_ValidToken(t *testing.T) {
	srv, _, rootToken, cleanup := startAuthServer(t)
	defer cleanup()

	resp, err := authReq("GET",
		fmt.Sprintf("http://%s/api/v1/stats", srv.Addr()),
		rootToken, nil)
	if err != nil {
		t.Fatal(err)
	}

	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
}

func TestAuth_CreateKey(t *testing.T) {
	srv, _, rootToken, cleanup := startAuthServer(t)
	defer cleanup()

	body, _ := json.Marshal(map[string]string{"name": "ci-pipeline"})

	resp, err := authReq("POST",
		fmt.Sprintf("http://%s/api/v1/auth/keys", srv.Addr()),
		rootToken, body)
	if err != nil {
		t.Fatal(err)
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Errorf("status = %d, want 201", resp.StatusCode)
	}

	var result struct {
		Data struct {
			Token string `json:"token"`
			Name  string `json:"name"`
		} `json:"data"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatal(err)
	}

	if result.Data.Token == "" {
		t.Error("token should be returned on create")
	}

	if result.Data.Name != "ci-pipeline" {
		t.Errorf("name = %q, want ci-pipeline", result.Data.Name)
	}

	// New key should work for queries.
	resp2, err := authReq("GET",
		fmt.Sprintf("http://%s/api/v1/stats", srv.Addr()),
		result.Data.Token, nil)
	if err != nil {
		t.Fatal(err)
	}

	resp2.Body.Close()

	if resp2.StatusCode != http.StatusOK {
		t.Errorf("new key status = %d, want 200", resp2.StatusCode)
	}
}

func TestAuth_RegularKeyForbidden(t *testing.T) {
	srv, ks, _, cleanup := startAuthServer(t)
	defer cleanup()

	regular, err := ks.CreateKey("ci", false)
	if err != nil {
		t.Fatal(err)
	}

	// Regular key should get 403 on auth management.
	resp, err := authReq("GET",
		fmt.Sprintf("http://%s/api/v1/auth/keys", srv.Addr()),
		regular.Token, nil)
	if err != nil {
		t.Fatal(err)
	}

	resp.Body.Close()

	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("status = %d, want 403", resp.StatusCode)
	}
}

func TestAuth_ListKeys(t *testing.T) {
	srv, ks, rootToken, cleanup := startAuthServer(t)
	defer cleanup()

	_, err := ks.CreateKey("ci", false)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := authReq("GET",
		fmt.Sprintf("http://%s/api/v1/auth/keys", srv.Addr()),
		rootToken, nil)
	if err != nil {
		t.Fatal(err)
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}

	var result struct {
		Data struct {
			Keys []json.RawMessage `json:"keys"`
		} `json:"data"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatal(err)
	}

	if len(result.Data.Keys) != 2 {
		t.Errorf("keys = %d, want 2", len(result.Data.Keys))
	}
}

func TestAuth_RevokeKey(t *testing.T) {
	srv, ks, rootToken, cleanup := startAuthServer(t)
	defer cleanup()

	regular, err := ks.CreateKey("ci", false)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := authReq("DELETE",
		fmt.Sprintf("http://%s/api/v1/auth/keys/%s", srv.Addr(), regular.ID),
		rootToken, nil)
	if err != nil {
		t.Fatal(err)
	}

	resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		t.Errorf("status = %d, want 204", resp.StatusCode)
	}

	// Revoked key should no longer work.
	resp2, err := authReq("GET",
		fmt.Sprintf("http://%s/api/v1/stats", srv.Addr()),
		regular.Token, nil)
	if err != nil {
		t.Fatal(err)
	}

	resp2.Body.Close()

	if resp2.StatusCode != http.StatusUnauthorized {
		t.Errorf("revoked key status = %d, want 401", resp2.StatusCode)
	}
}

func TestAuth_RevokeLastRootKeyFails(t *testing.T) {
	srv, ks, rootToken, cleanup := startAuthServer(t)
	defer cleanup()

	keys := ks.List()
	rootID := ""

	for _, k := range keys {
		if k.IsRoot {
			rootID = k.ID

			break
		}
	}

	resp, err := authReq("DELETE",
		fmt.Sprintf("http://%s/api/v1/auth/keys/%s", srv.Addr(), rootID),
		rootToken, nil)
	if err != nil {
		t.Fatal(err)
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusConflict {
		t.Errorf("status = %d, want 409", resp.StatusCode)
	}

	var errResp struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&errResp); err != nil {
		t.Fatal(err)
	}

	if errResp.Error.Code != "LAST_ROOT_KEY" {
		t.Errorf("code = %q, want LAST_ROOT_KEY", errResp.Error.Code)
	}
}

func TestAuth_RotateRoot(t *testing.T) {
	srv, _, rootToken, cleanup := startAuthServer(t)
	defer cleanup()

	resp, err := authReq("POST",
		fmt.Sprintf("http://%s/api/v1/auth/rotate-root", srv.Addr()),
		rootToken, nil)
	if err != nil {
		t.Fatal(err)
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}

	var result struct {
		Data struct {
			Token        string `json:"token"`
			RevokedKeyID string `json:"revoked_key_id"`
		} `json:"data"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatal(err)
	}

	if result.Data.Token == "" {
		t.Error("new token should be returned")
	}

	if result.Data.RevokedKeyID == "" {
		t.Error("revoked key ID should be returned")
	}

	// Old token should not work.
	resp2, err := authReq("GET",
		fmt.Sprintf("http://%s/api/v1/stats", srv.Addr()),
		rootToken, nil)
	if err != nil {
		t.Fatal(err)
	}

	resp2.Body.Close()

	if resp2.StatusCode != http.StatusUnauthorized {
		t.Errorf("old token status = %d, want 401", resp2.StatusCode)
	}

	// New token should work.
	resp3, err := authReq("GET",
		fmt.Sprintf("http://%s/api/v1/stats", srv.Addr()),
		result.Data.Token, nil)
	if err != nil {
		t.Fatal(err)
	}

	resp3.Body.Close()

	if resp3.StatusCode != http.StatusOK {
		t.Errorf("new token status = %d, want 200", resp3.StatusCode)
	}
}

func TestAuth_DisabledReturns404(t *testing.T) {
	// Server without auth (no KeyStore).
	srv, cleanup := startTestServer(t)
	defer cleanup()

	resp, err := http.Get(fmt.Sprintf("http://%s/api/v1/auth/keys", srv.Addr()))
	if err != nil {
		t.Fatal(err)
	}

	resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status = %d, want 404", resp.StatusCode)
	}
}

func TestAuth_ApiKeyFormat(t *testing.T) {
	srv, ks, _, cleanup := startAuthServer(t)
	defer cleanup()

	// Create a regular key.
	regular, err := ks.CreateKey("apikey-test", false)
	if err != nil {
		t.Fatal(err)
	}

	// Construct ApiKey header: base64(id:token).
	composite := regular.ID + ":" + regular.Token
	b64 := base64.StdEncoding.EncodeToString([]byte(composite))
	header := "ApiKey " + b64

	req, _ := http.NewRequest("GET",
		fmt.Sprintf("http://%s/api/v1/stats", srv.Addr()),
		http.NoBody,
	)
	req.Header.Set("Authorization", header)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
}

func TestAuth_BasicFormat(t *testing.T) {
	srv, ks, _, cleanup := startAuthServer(t)
	defer cleanup()

	regular, err := ks.CreateKey("basic-test", false)
	if err != nil {
		t.Fatal(err)
	}

	// Basic auth: base64(username:password), password = token.
	composite := "lynxdb:" + regular.Token
	b64 := base64.StdEncoding.EncodeToString([]byte(composite))
	header := "Basic " + b64

	req, _ := http.NewRequest("GET",
		fmt.Sprintf("http://%s/api/v1/stats", srv.Addr()),
		http.NoBody,
	)
	req.Header.Set("Authorization", header)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
}

func TestAuth_ApiKeyFormat_Invalid(t *testing.T) {
	srv, _, _, cleanup := startAuthServer(t)
	defer cleanup()

	// Invalid base64.
	req, _ := http.NewRequest("GET",
		fmt.Sprintf("http://%s/api/v1/stats", srv.Addr()),
		http.NoBody,
	)
	req.Header.Set("Authorization", "ApiKey not-valid-base64!!!")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", resp.StatusCode)
	}
}

func TestAuth_ExpiredKey(t *testing.T) {
	srv, ks, _, cleanup := startAuthServer(t)
	defer cleanup()

	// Create a key that already expired.
	expired, err := ks.CreateKeyWithOpts(auth.CreateKeyOpts{
		Name:      "expired-test",
		Scope:     auth.ScopeFull,
		ExpiresAt: time.Now().Add(-1 * time.Hour), // 1 hour ago
	})
	if err != nil {
		t.Fatal(err)
	}

	resp, err := authReq("GET",
		fmt.Sprintf("http://%s/api/v1/stats", srv.Addr()),
		expired.Token, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401 (expired key)", resp.StatusCode)
	}
}

func TestAuth_ScopeIngest_CanIngest(t *testing.T) {
	srv, ks, _, cleanup := startAuthServer(t)
	defer cleanup()

	ingestKey, err := ks.CreateKeyWithOpts(auth.CreateKeyOpts{
		Name:  "ingest-only",
		Scope: auth.ScopeIngest,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Ingest should work.
	body := []byte(`[{"event":"test","source":"test","index":"main"}]`)
	resp, err := authReq("POST",
		fmt.Sprintf("http://%s/api/v1/ingest", srv.Addr()),
		ingestKey.Token, body)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("ingest status = %d, want 200", resp.StatusCode)
	}
}

func TestAuth_CreateKeyWithScope(t *testing.T) {
	srv, _, rootToken, cleanup := startAuthServer(t)
	defer cleanup()

	body, _ := json.Marshal(map[string]string{
		"name":        "scoped-key",
		"scope":       "ingest",
		"description": "Filebeat agent",
		"expires_in":  "30d",
	})

	resp, err := authReq("POST",
		fmt.Sprintf("http://%s/api/v1/auth/keys", srv.Addr()),
		rootToken, body)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 201, body: %s", resp.StatusCode, b)
	}

	var result struct {
		Data struct {
			Token       string `json:"token"`
			APIKey      string `json:"api_key"`
			Name        string `json:"name"`
			Scope       string `json:"scope"`
			Description string `json:"description"`
			ExpiresAt   string `json:"expires_at"`
			ID          string `json:"id"`
		} `json:"data"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatal(err)
	}

	if result.Data.Token == "" {
		t.Error("token should be returned on create")
	}
	if result.Data.APIKey == "" {
		t.Error("api_key should be returned on create")
	}
	if result.Data.Scope != "ingest" {
		t.Errorf("scope = %q, want ingest", result.Data.Scope)
	}
	if result.Data.Description != "Filebeat agent" {
		t.Errorf("description = %q, want 'Filebeat agent'", result.Data.Description)
	}
	if result.Data.ExpiresAt == "" {
		t.Error("expires_at should be set")
	}
	// Verify new ID format (ak_ prefix).
	if !strings.HasPrefix(result.Data.ID, "ak_") {
		t.Errorf("ID = %q, expected ak_ prefix", result.Data.ID)
	}
	// api_key should be id:token.
	if result.Data.APIKey != result.Data.ID+":"+result.Data.Token {
		t.Errorf("api_key = %q, want %q", result.Data.APIKey, result.Data.ID+":"+result.Data.Token)
	}
}

func TestAuth_CreateKeyInvalidScope(t *testing.T) {
	srv, _, rootToken, cleanup := startAuthServer(t)
	defer cleanup()

	body, _ := json.Marshal(map[string]string{
		"name":  "bad-scope",
		"scope": "superadmin",
	})

	resp, err := authReq("POST",
		fmt.Sprintf("http://%s/api/v1/auth/keys", srv.Addr()),
		rootToken, body)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", resp.StatusCode)
	}
}

func TestAuth_ScopeIngest_CannotQuery(t *testing.T) {
	srv, ks, _, cleanup := startAuthServer(t)
	defer cleanup()

	ingestKey, err := ks.CreateKeyWithOpts(auth.CreateKeyOpts{
		Name:  "ingest-nq",
		Scope: auth.ScopeIngest,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Query should be forbidden.
	body := []byte(`{"q":"FROM main | head 5"}`)
	resp, err := authReq("POST",
		fmt.Sprintf("http://%s/api/v1/query", srv.Addr()),
		ingestKey.Token, body)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusForbidden {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 403, body: %s", resp.StatusCode, b)
	}

	var errResp struct {
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	json.NewDecoder(resp.Body).Decode(&errResp)
	if errResp.Error.Code != "FORBIDDEN" {
		t.Errorf("code = %q, want FORBIDDEN", errResp.Error.Code)
	}
}

func TestAuth_ScopeQuery_CannotIngest(t *testing.T) {
	srv, ks, _, cleanup := startAuthServer(t)
	defer cleanup()

	queryKey, err := ks.CreateKeyWithOpts(auth.CreateKeyOpts{
		Name:  "query-only",
		Scope: auth.ScopeQuery,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Ingest should be forbidden.
	body := []byte(`[{"event":"test","source":"test","index":"main"}]`)
	resp, err := authReq("POST",
		fmt.Sprintf("http://%s/api/v1/ingest", srv.Addr()),
		queryKey.Token, body)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("status = %d, want 403", resp.StatusCode)
	}
}

func TestAuth_ScopeQuery_CannotESBulk(t *testing.T) {
	srv, ks, _, cleanup := startAuthServer(t)
	defer cleanup()

	queryKey, err := ks.CreateKeyWithOpts(auth.CreateKeyOpts{
		Name:  "query-noes",
		Scope: auth.ScopeQuery,
	})
	if err != nil {
		t.Fatal(err)
	}

	// ES _bulk should be forbidden for query-scoped key.
	body := `{"index":{"_index":"logs"}}
{"message":"test"}
`
	req, _ := http.NewRequest("POST",
		fmt.Sprintf("http://%s/api/v1/es/_bulk", srv.Addr()),
		strings.NewReader(body),
	)
	req.Header.Set("Authorization", "Bearer "+queryKey.Token)
	req.Header.Set("Content-Type", "application/x-ndjson")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("status = %d, want 403", resp.StatusCode)
	}
}

func TestAuth_VerifyByID(t *testing.T) {
	dir := t.TempDir()
	ks, err := auth.OpenKeyStore(dir)
	if err != nil {
		t.Fatal(err)
	}

	created, err := ks.CreateKey("test", false)
	if err != nil {
		t.Fatal(err)
	}

	// VerifyByID with correct ID + token should succeed.
	info := ks.VerifyByID(created.ID, created.Token)
	if info == nil {
		t.Fatal("VerifyByID returned nil for valid ID+token")
	}
	if info.ID != created.ID {
		t.Errorf("info.ID = %q, want %q", info.ID, created.ID)
	}

	// Wrong ID should fail.
	info = ks.VerifyByID("nonexistent", created.Token)
	if info != nil {
		t.Error("VerifyByID should return nil for wrong ID")
	}

	// Wrong token should fail.
	info = ks.VerifyByID(created.ID, "wrong_token")
	if info != nil {
		t.Error("VerifyByID should return nil for wrong token")
	}
}

func TestAuth_ListKeysShowsScope(t *testing.T) {
	srv, ks, rootToken, cleanup := startAuthServer(t)
	defer cleanup()

	_, err := ks.CreateKeyWithOpts(auth.CreateKeyOpts{
		Name:  "ingest-key",
		Scope: auth.ScopeIngest,
	})
	if err != nil {
		t.Fatal(err)
	}

	resp, err := authReq("GET",
		fmt.Sprintf("http://%s/api/v1/auth/keys", srv.Addr()),
		rootToken, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	// Just verify the response decodes with scope fields.
	var result struct {
		Data struct {
			Keys []struct {
				ID    string `json:"id"`
				Scope string `json:"scope"`
			} `json:"keys"`
		} `json:"data"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatal(err)
	}

	if len(result.Data.Keys) < 2 {
		t.Fatalf("expected at least 2 keys, got %d", len(result.Data.Keys))
	}

	// Find the ingest key.
	found := false
	for _, k := range result.Data.Keys {
		if k.Scope == "ingest" {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected to find a key with scope=ingest in list")
	}
}
