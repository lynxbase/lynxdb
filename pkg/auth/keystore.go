package auth

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"golang.org/x/crypto/argon2"
)

// Argon2id parameters — tuned for server-side key verification.
// Memory=64MB, iterations=3, parallelism=4 gives ~50ms per verify on modern hardware.
const (
	argon2Time    = 3
	argon2Memory  = 64 * 1024 // 64 MB.
	argon2Threads = 4
	argon2KeyLen  = 32
	argon2SaltLen = 16
)

// KeyStore manages API keys on disk with argon2id hashing.
// All methods are safe for concurrent use.
type KeyStore struct {
	mu   sync.RWMutex
	path string   // Path to keys.json.
	keys []APIKey // In-memory copy.
	seq  int      // Monotonic sequence for key IDs.
}

// File format for keys.json.
type keysFile struct {
	Keys []APIKey `json:"keys"`
	Seq  int      `json:"seq"`
}

// OpenKeyStore opens or creates a key store at the given directory.
// The directory must already exist. The keys.json file is created on first write.
func OpenKeyStore(dir string) (*KeyStore, error) {
	path := filepath.Join(dir, "keys.json")

	ks := &KeyStore{path: path}

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return ks, nil
		}

		return nil, fmt.Errorf("auth.OpenKeyStore: read %s: %w", path, err)
	}

	var f keysFile
	if err := json.Unmarshal(data, &f); err != nil {
		return nil, fmt.Errorf("auth.OpenKeyStore: parse %s: %w", path, err)
	}

	ks.keys = f.Keys
	ks.seq = f.Seq

	return ks, nil
}

// Len returns the number of stored keys.
func (ks *KeyStore) Len() int {
	ks.mu.RLock()
	defer ks.mu.RUnlock()

	return len(ks.keys)
}

// IsEmpty reports whether the key store has no keys.
func (ks *KeyStore) IsEmpty() bool {
	return ks.Len() == 0
}

// CreateKey generates a new API key, hashes it, stores it, and returns the
// plaintext token. The token is never stored and cannot be retrieved later.
func (ks *KeyStore) CreateKey(name string, isRoot bool) (*CreatedKey, error) {
	keyType := KeyTypeRegular
	if isRoot {
		keyType = KeyTypeRoot
	}

	token, err := GenerateKey(keyType)
	if err != nil {
		return nil, fmt.Errorf("auth.CreateKey: %w", err)
	}

	hash, err := hashToken(token)
	if err != nil {
		return nil, fmt.Errorf("auth.CreateKey: %w", err)
	}

	ks.mu.Lock()
	defer ks.mu.Unlock()

	ks.seq++
	id := fmt.Sprintf("key_%05d", ks.seq)

	key := APIKey{
		ID:        id,
		Prefix:    KeyPrefix(token),
		Hash:      hash,
		Name:      name,
		CreatedAt: time.Now().UTC(),
		IsRoot:    isRoot,
	}

	ks.keys = append(ks.keys, key)

	if err := ks.saveLocked(); err != nil {
		// Roll back.
		ks.keys = ks.keys[:len(ks.keys)-1]
		ks.seq--

		return nil, fmt.Errorf("auth.CreateKey: %w", err)
	}

	return &CreatedKey{
		KeyInfo: key.Info(),
		Token:   token,
	}, nil
}

// Verify checks a plaintext token against all stored key hashes.
// Returns the matching key info on success, or nil if no key matches.
// Updates last_used_at on the matched key.
func (ks *KeyStore) Verify(token string) *KeyInfo {
	ks.mu.Lock()
	defer ks.mu.Unlock()

	for i := range ks.keys {
		if verifyToken(token, ks.keys[i].Hash) {
			ks.keys[i].LastUsedAt = time.Now().UTC()
			// Best-effort persist last_used_at — don't fail auth on write error.
			_ = ks.saveLocked()

			info := ks.keys[i].Info()

			return &info
		}
	}

	return nil
}

// List returns public info for all stored keys.
func (ks *KeyStore) List() []KeyInfo {
	ks.mu.RLock()
	defer ks.mu.RUnlock()

	infos := make([]KeyInfo, len(ks.keys))
	for i := range ks.keys {
		infos[i] = ks.keys[i].Info()
	}

	return infos
}

// Revoke removes a key by ID. Returns an error if the key is the last root key.
func (ks *KeyStore) Revoke(id string) error {
	ks.mu.Lock()
	defer ks.mu.Unlock()

	idx := -1

	for i := range ks.keys {
		if ks.keys[i].ID == id {
			idx = i

			break
		}
	}

	if idx < 0 {
		return fmt.Errorf("auth.Revoke: key %q not found", id)
	}

	// Prevent revoking the last root key.
	if ks.keys[idx].IsRoot {
		rootCount := 0

		for i := range ks.keys {
			if ks.keys[i].IsRoot {
				rootCount++
			}
		}

		if rootCount <= 1 {
			return ErrLastRootKey
		}
	}

	// Remove from slice.
	ks.keys = append(ks.keys[:idx], ks.keys[idx+1:]...)

	if err := ks.saveLocked(); err != nil {
		return fmt.Errorf("auth.Revoke: %w", err)
	}

	return nil
}

// RotateRoot creates a new root key and revokes the old one identified by oldID.
// Returns the new key with its plaintext token.
func (ks *KeyStore) RotateRoot(oldID string) (*CreatedKey, error) {
	token, err := GenerateKey(KeyTypeRoot)
	if err != nil {
		return nil, fmt.Errorf("auth.RotateRoot: %w", err)
	}

	hash, err := hashToken(token)
	if err != nil {
		return nil, fmt.Errorf("auth.RotateRoot: %w", err)
	}

	ks.mu.Lock()
	defer ks.mu.Unlock()

	// Find the old key.
	oldIdx := -1

	for i := range ks.keys {
		if ks.keys[i].ID == oldID {
			if !ks.keys[i].IsRoot {
				return nil, fmt.Errorf("auth.RotateRoot: key %q is not a root key", oldID)
			}

			oldIdx = i

			break
		}
	}

	if oldIdx < 0 {
		return nil, fmt.Errorf("auth.RotateRoot: key %q not found", oldID)
	}

	// Create new root key.
	ks.seq++
	newKey := APIKey{
		ID:        fmt.Sprintf("key_%05d", ks.seq),
		Prefix:    KeyPrefix(token),
		Hash:      hash,
		Name:      "root",
		CreatedAt: time.Now().UTC(),
		IsRoot:    true,
	}

	// Add new key, then remove old.
	ks.keys = append(ks.keys, newKey)
	ks.keys = append(ks.keys[:oldIdx], ks.keys[oldIdx+1:]...)

	if err := ks.saveLocked(); err != nil {
		return nil, fmt.Errorf("auth.RotateRoot: %w", err)
	}

	return &CreatedKey{
		KeyInfo: newKey.Info(),
		Token:   token,
	}, nil
}

// saveLocked persists the key store to disk. Caller must hold ks.mu.
func (ks *KeyStore) saveLocked() error {
	f := keysFile{
		Keys: ks.keys,
		Seq:  ks.seq,
	}

	data, err := json.MarshalIndent(f, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal keys: %w", err)
	}

	// Atomic write: write to temp file, then rename.
	dir := filepath.Dir(ks.path)
	tmp, err := os.CreateTemp(dir, ".keys-*.tmp")
	if err != nil {
		return fmt.Errorf("create temp: %w", err)
	}

	tmpName := tmp.Name()

	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		os.Remove(tmpName)

		return fmt.Errorf("write temp: %w", err)
	}

	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)

		return fmt.Errorf("close temp: %w", err)
	}

	if err := os.Chmod(tmpName, 0o600); err != nil {
		os.Remove(tmpName)

		return fmt.Errorf("chmod temp: %w", err)
	}

	if err := os.Rename(tmpName, ks.path); err != nil {
		os.Remove(tmpName)

		return fmt.Errorf("rename temp: %w", err)
	}

	return nil
}

// hashToken produces an argon2id hash string for a plaintext token.
func hashToken(token string) (string, error) {
	salt := make([]byte, argon2SaltLen)
	if _, err := rand.Read(salt); err != nil {
		return "", fmt.Errorf("generate salt: %w", err)
	}

	hash := argon2.IDKey([]byte(token), salt, argon2Time, argon2Memory, argon2Threads, argon2KeyLen)

	// Encode as: $argon2id$v=19$m=65536,t=3,p=4$<salt>$<hash>.
	b64Salt := base64.RawStdEncoding.EncodeToString(salt)
	b64Hash := base64.RawStdEncoding.EncodeToString(hash)

	return fmt.Sprintf("$argon2id$v=19$m=%d,t=%d,p=%d$%s$%s",
		argon2Memory, argon2Time, argon2Threads, b64Salt, b64Hash), nil
}

// verifyToken checks a plaintext token against an argon2id hash string.
func verifyToken(token, encoded string) bool {
	// Parse: $argon2id$v=19$m=65536,t=3,p=4$<salt>$<hash>.
	var memory uint32
	var iterations uint32
	var parallelism uint8
	var b64Salt, b64Hash string

	_, err := fmt.Sscanf(encoded, "$argon2id$v=19$m=%d,t=%d,p=%d$", &memory, &iterations, &parallelism)
	if err != nil {
		return false
	}

	// Extract salt and hash from the tail.
	parts := splitDollar(encoded)
	if len(parts) < 6 {
		return false
	}

	b64Salt = parts[4]
	b64Hash = parts[5]

	salt, err := base64.RawStdEncoding.DecodeString(b64Salt)
	if err != nil {
		return false
	}

	expectedHash, err := base64.RawStdEncoding.DecodeString(b64Hash)
	if err != nil {
		return false
	}

	computedHash := argon2.IDKey([]byte(token), salt, iterations, memory, parallelism, uint32(len(expectedHash)))

	return subtle.ConstantTimeCompare(computedHash, expectedHash) == 1
}

// splitDollar splits s on '$' characters, keeping empty initial element.
func splitDollar(s string) []string {
	var parts []string
	start := 0

	for i := range len(s) {
		if s[i] == '$' {
			parts = append(parts, s[start:i])
			start = i + 1
		}
	}

	parts = append(parts, s[start:])

	return parts
}
