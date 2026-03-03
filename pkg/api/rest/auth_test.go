package rest

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/OrlovEvgeny/Lynxdb/pkg/auth"
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
