package auth_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/OrlovEvgeny/Lynxdb/pkg/auth"
)

func TestGenerateKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		keyType auth.KeyType
		wantPfx string
		wantLen int
	}{
		{"Root key", auth.KeyTypeRoot, "lynx_rk_", 40},
		{"Regular key", auth.KeyTypeRegular, "lynx_ak_", 40},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			token, err := auth.GenerateKey(tt.keyType)
			if err != nil {
				t.Fatal(err)
			}

			if len(token) != tt.wantLen {
				t.Errorf("len = %d, want %d", len(token), tt.wantLen)
			}

			if token[:len(tt.wantPfx)] != tt.wantPfx {
				t.Errorf("prefix = %q, want %q", token[:len(tt.wantPfx)], tt.wantPfx)
			}
		})
	}

	// Uniqueness check.
	t.Run("Unique", func(t *testing.T) {
		t.Parallel()

		a, _ := auth.GenerateKey(auth.KeyTypeRoot)
		b, _ := auth.GenerateKey(auth.KeyTypeRoot)

		if a == b {
			t.Error("two generated keys should not be identical")
		}
	})
}

func TestParseKeyType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		token   string
		want    auth.KeyType
		wantErr bool
	}{
		{"lynx_rk_abcdefghij1234567890abcdefghij12", auth.KeyTypeRoot, false},
		{"lynx_ak_abcdefghij1234567890abcdefghij12", auth.KeyTypeRegular, false},
		{"lynx_xx_abcdefghij", "", true},
		{"bad", "", true},
		{"", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.token, func(t *testing.T) {
			t.Parallel()

			got, err := auth.ParseKeyType(tt.token)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error")
				}

				return
			}

			if err != nil {
				t.Fatal(err)
			}

			if got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
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

	// Create a root key.
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

	// New token should verify.
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

	// Create a key and close.
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

	// Check file permissions.
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
