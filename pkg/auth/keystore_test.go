package auth_test

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/auth"
)

func TestGenerateKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		keyType auth.KeyType
		wantPfx string
	}{
		{"Root key", auth.KeyTypeRoot, "lynx_rk_"},
		{"Regular key", auth.KeyTypeRegular, "lynx_ak_"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			id, token, err := auth.GenerateKey(tt.keyType)
			if err != nil {
				t.Fatal(err)
			}

			if len(token) != 53 {
				t.Errorf("len = %d, want 53", len(token))
			}

			if !strings.HasPrefix(token, tt.wantPfx) {
				t.Errorf("token %q, want prefix %q", token, tt.wantPfx)
			}

			if got, ok := auth.KeyIDFromToken(token); !ok || got != id {
				t.Errorf("KeyIDFromToken(%q) = %q, %v, want %q", token, got, ok, id)
			}

			if got := auth.KeyPrefix(token); got != "lynx_"+id {
				t.Errorf("KeyPrefix = %q, want %q", got, "lynx_"+id)
			}
		})
	}

	// Uniqueness check.
	t.Run("Unique", func(t *testing.T) {
		t.Parallel()

		_, a, _ := auth.GenerateKey(auth.KeyTypeRoot)
		_, b, _ := auth.GenerateKey(auth.KeyTypeRoot)

		if a == b {
			t.Error("two generated keys should not be identical")
		}
	})
}

func TestKeyIDFromTokenRejectsMalformedTokens(t *testing.T) {
	t.Parallel()

	_, token, err := auth.GenerateKey(auth.KeyTypeRegular)
	if err != nil {
		t.Fatal(err)
	}

	for _, bad := range []string{
		"",
		token[:len(token)-1],
		token + "x",
		"lynx_xx" + token[7:],
		"lynx_ak-" + token[8:],
		token[:20] + "-" + token[21:],
		"Bearer " + token[7:],
	} {
		if id, ok := auth.KeyIDFromToken(bad); ok {
			t.Errorf("KeyIDFromToken(%q) = %q, want no id", bad, id)
		}
	}
}

func TestKeyStoreCreateAndVerify(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	ks, err := auth.OpenKeyStore(dir)
	if err != nil {
		t.Fatal(err)
	}

	if !ks.IsEmpty() {
		t.Fatal("new store should be empty")
	}

	created, err := ks.CreateKey("root", true)
	if err != nil {
		t.Fatal(err)
	}

	if created.Token == "" {
		t.Fatal("token should not be empty")
	}

	if !created.IsRoot {
		t.Error("key should be root")
	}

	if ks.Len() != 1 {
		t.Errorf("len = %d, want 1", ks.Len())
	}

	// Verify the token works.
	info := ks.Verify(created.Token)
	if info == nil {
		t.Fatal("verify should succeed with correct token")
	}

	if info.ID != created.ID {
		t.Errorf("ID = %q, want %q", info.ID, created.ID)
	}

	// Verify wrong token fails.
	if ks.Verify("lynx_rk_wrongwrongwrongwrongwrongwro") != nil {
		t.Error("verify should fail with wrong token")
	}
}

func TestKeyStoreRevoke(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	ks, err := auth.OpenKeyStore(dir)
	if err != nil {
		t.Fatal(err)
	}

	root, err := ks.CreateKey("root", true)
	if err != nil {
		t.Fatal(err)
	}

	regular, err := ks.CreateKey("ci", false)
	if err != nil {
		t.Fatal(err)
	}

	// Revoke regular key.
	if err := ks.Revoke(regular.ID); err != nil {
		t.Fatal(err)
	}

	if ks.Verify(regular.Token) != nil {
		t.Error("revoked key should not verify")
	}

	// Cannot revoke last root key.
	if err := ks.Revoke(root.ID); err == nil {
		t.Error("should not be able to revoke last root key")
	}
}

func TestKeyStoreRotateRoot(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	ks, err := auth.OpenKeyStore(dir)
	if err != nil {
		t.Fatal(err)
	}

	old, err := ks.CreateKey("root", true)
	if err != nil {
		t.Fatal(err)
	}

	rotated, err := ks.RotateRoot(old.ID)
	if err != nil {
		t.Fatal(err)
	}

	// Old token should not verify.
	if ks.Verify(old.Token) != nil {
		t.Error("old root token should not verify after rotation")
	}

	if ks.Verify(rotated.Token) == nil {
		t.Error("new root token should verify")
	}

	if !rotated.IsRoot {
		t.Error("rotated key should be root")
	}
}

func TestKeyStorePersistence(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	ks1, err := auth.OpenKeyStore(dir)
	if err != nil {
		t.Fatal(err)
	}

	created, err := ks1.CreateKey("root", true)
	if err != nil {
		t.Fatal(err)
	}

	// Reopen and verify the key persisted.
	ks2, err := auth.OpenKeyStore(dir)
	if err != nil {
		t.Fatal(err)
	}

	if ks2.Len() != 1 {
		t.Fatalf("len = %d, want 1", ks2.Len())
	}

	if ks2.Verify(created.Token) == nil {
		t.Error("key should verify after reopen")
	}

	info, err := os.Stat(filepath.Join(dir, "keys.json"))
	if err != nil {
		t.Fatal(err)
	}

	perm := info.Mode().Perm()
	if perm != 0o600 {
		t.Errorf("permissions = %o, want 0600", perm)
	}
}

func TestKeyStoreList(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	ks, err := auth.OpenKeyStore(dir)
	if err != nil {
		t.Fatal(err)
	}

	_, err = ks.CreateKey("root", true)
	if err != nil {
		t.Fatal(err)
	}

	_, err = ks.CreateKey("ci", false)
	if err != nil {
		t.Fatal(err)
	}

	keys := ks.List()
	if len(keys) != 2 {
		t.Fatalf("len = %d, want 2", len(keys))
	}

	// List should never include hash or token.
	for _, k := range keys {
		if k.ID == "" {
			t.Error("key ID should not be empty")
		}

		if k.Prefix == "" {
			t.Error("key prefix should not be empty")
		}
	}
}

func TestKeyStoreVerifyChecksTheSecret(t *testing.T) {
	t.Parallel()

	ks, err := auth.OpenKeyStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ks.Close() })

	created, err := ks.CreateKey("ci", false)
	if err != nil {
		t.Fatal(err)
	}

	last := "a"
	if strings.HasSuffix(created.Token, "a") {
		last = "b"
	}
	if ks.Verify(created.Token[:len(created.Token)-1]+last) != nil {
		t.Error("a token with the id of a key and another secret should not verify")
	}
	if ks.Verify(created.Token) == nil {
		t.Error("the token should verify")
	}
}

func TestKeyStoreVerifyWritesLastUseOnClose(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "keys.json")

	ks, err := auth.OpenKeyStore(dir)
	if err != nil {
		t.Fatal(err)
	}

	created, err := ks.CreateKey("root", true)
	if err != nil {
		t.Fatal(err)
	}

	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	start := time.Now()
	info := ks.Verify(created.Token)
	if info == nil {
		t.Fatal("verify should succeed")
	}
	if info.LastUsedAt.Before(start.Add(-time.Second)) {
		t.Errorf("LastUsedAt = %v, want about %v", info.LastUsedAt, start)
	}

	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(before) != string(after) {
		t.Error("verify should not write keys.json")
	}

	if err := ks.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := auth.OpenKeyStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })

	keys := reopened.List()
	if len(keys) != 1 || !keys[0].LastUsedAt.Equal(info.LastUsedAt) {
		t.Errorf("keys after reopen = %+v, want last_used_at %v", keys, info.LastUsedAt)
	}
}

func TestKeyStoreFileHoldsDigestsOnly(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	ks, err := auth.OpenKeyStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ks.Close() })

	created, err := ks.CreateKey("root", true)
	if err != nil {
		t.Fatal(err)
	}

	data, err := os.ReadFile(filepath.Join(dir, "keys.json"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), created.Token) {
		t.Fatal("keys.json holds the token")
	}

	var f struct {
		Version int `json:"version"`
		Keys    []struct {
			ID     string `json:"id"`
			SHA256 string `json:"sha256"`
		} `json:"keys"`
	}
	if err := json.Unmarshal(data, &f); err != nil {
		t.Fatal(err)
	}
	if f.Version != 1 || len(f.Keys) != 1 || f.Keys[0].ID != created.ID || len(f.Keys[0].SHA256) != 64 {
		t.Errorf("keys.json = %s", data)
	}
}

func TestOpenKeyStoreRejectsUnsupportedFormat(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	unsupported := `{"keys":[{"id":"rk_abcdefghijkl","prefix":"lynx_rk_abcd","hash":"$argon2id$v=19$m=65536,t=3,p=4$c2FsdA$aGFzaA","name":"root","created_at":"2026-01-01T00:00:00Z","is_root":true}],"seq":0}`
	if err := os.WriteFile(filepath.Join(dir, "keys.json"), []byte(unsupported), 0o600); err != nil {
		t.Fatal(err)
	}

	_, err := auth.OpenKeyStore(dir)
	if err == nil || !strings.Contains(err.Error(), "delete it") {
		t.Fatalf("OpenKeyStore error = %v, want one that says to delete the file", err)
	}
}

func TestKeyStoreVerifyWhileKeysChange(t *testing.T) {
	t.Parallel()

	ks, err := auth.OpenKeyStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	root, err := ks.CreateKey("root", true)
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan struct{})
	var verified atomic.Int64
	var wg, running sync.WaitGroup
	for range 8 {
		wg.Add(1)
		running.Add(1)
		go func() {
			defer wg.Done()
			first := sync.OnceFunc(running.Done)
			defer first()
			for {
				select {
				case <-done:
					return
				default:
				}
				if ks.Verify(root.Token) == nil {
					t.Error("the root key should verify")
					return
				}
				verified.Add(1)
				first()
				// A verifier that never yields keeps the only processor
				// of a small machine from the changes below.
				runtime.Gosched()
			}
		}()
	}
	// The keys change once every verifier runs: twenty changes can end
	// before a goroutine started above is scheduled.
	running.Wait()
	for i := range 20 {
		k, err := ks.CreateKey(fmt.Sprintf("k%d", i), false)
		if err != nil {
			t.Fatal(err)
		}
		if err := ks.Revoke(k.ID); err != nil {
			t.Fatal(err)
		}
	}
	close(done)
	wg.Wait()
	if err := ks.Close(); err != nil {
		t.Fatal(err)
	}
	if verified.Load() == 0 {
		t.Fatal("no verification ran")
	}
}
