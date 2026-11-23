package auth

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

// keysFileVersion is the format version of keys.json.
const keysFileVersion = 1

// lastUsedFlushInterval bounds how often the use of keys alone rewrites keys.json.
const lastUsedFlushInterval = time.Minute

// KeyStore manages API keys on disk. A token carries the ID of its key, so
// verification looks the key up by ID and compares the SHA-256 digest of
// the token with the stored one; tokens are random, which makes a slow
// password hash unnecessary.
//
// The time a key was last used is kept in memory and written with the
// other changes to the keys, at most once a minute after a use, and on
// Close. All methods are safe for concurrent use.
type KeyStore struct {
	// mu guards keys, byID and closed. Writers of keys.json hold it: changes
	// to the keys exclusively, last-used flushes shared with saveMu.
	mu     sync.RWMutex
	saveMu sync.Mutex
	path   string
	keys   []*storedKey // In creation order.
	byID   map[string]*storedKey
	closed bool
	// dirty is set when a key was used since keys.json was last written.
	dirty atomic.Bool
}

// storedKey is a key with its digest decoded and its last use updated
// without the write lock.
type storedKey struct {
	APIKey
	digest   [sha256.Size]byte
	lastUsed atomic.Int64 // Unix nanoseconds; zero means never used.
}

func (k *storedKey) info() KeyInfo {
	info := k.Info()
	if ns := k.lastUsed.Load(); ns != 0 {
		info.LastUsedAt = time.Unix(0, ns).UTC()
	}

	return info
}

func (k *storedKey) record() APIKey {
	rec := k.APIKey
	if ns := k.lastUsed.Load(); ns != 0 {
		rec.LastUsedAt = time.Unix(0, ns).UTC()
	}

	return rec
}

// keysFile is the format of keys.json.
type keysFile struct {
	Version int      `json:"version"`
	Keys    []APIKey `json:"keys"`
}

// OpenKeyStore opens or creates a key store at the given directory.
// The directory must already exist. The keys.json file is created on first write.
func OpenKeyStore(dir string) (*KeyStore, error) {
	path := filepath.Join(dir, "keys.json")

	ks := &KeyStore{path: path, byID: map[string]*storedKey{}}

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
	if f.Version != keysFileVersion {
		return nil, fmt.Errorf("auth.OpenKeyStore: %s has an unsupported format: "+
			"delete it, start the server with auth on to generate a new root key, and create the other keys again", path)
	}

	for _, rec := range f.Keys {
		k, err := loadKey(rec)
		if err != nil {
			return nil, fmt.Errorf("auth.OpenKeyStore: %s: %w", path, err)
		}
		if ks.byID[k.ID] != nil {
			return nil, fmt.Errorf("auth.OpenKeyStore: %s: duplicate key %q", path, k.ID)
		}
		ks.keys = append(ks.keys, k)
		ks.byID[k.ID] = k
	}

	return ks, nil
}

func loadKey(rec APIKey) (*storedKey, error) {
	if !validKeyID(rec.ID) {
		return nil, fmt.Errorf("invalid key id %q", rec.ID)
	}
	if rec.IsRoot != (KeyType(rec.ID[:2]) == KeyTypeRoot) {
		return nil, fmt.Errorf("key %q: is_root does not match its id", rec.ID)
	}
	if !ValidScope(string(rec.Scope)) {
		return nil, fmt.Errorf("key %q: invalid scope %q", rec.ID, rec.Scope)
	}
	k := &storedKey{APIKey: rec}
	if len(rec.SHA256) != hex.EncodedLen(sha256.Size) {
		return nil, fmt.Errorf("key %q: invalid sha256 digest", rec.ID)
	}
	if _, err := hex.Decode(k.digest[:], []byte(rec.SHA256)); err != nil {
		return nil, fmt.Errorf("key %q: invalid sha256 digest", rec.ID)
	}
	if !rec.LastUsedAt.IsZero() {
		k.lastUsed.Store(rec.LastUsedAt.UnixNano())
	}
	k.LastUsedAt = time.Time{}

	return k, nil
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

// CreateKey generates a new API key, stores its digest, and returns the
// plaintext token. The token is never stored and cannot be retrieved later.
func (ks *KeyStore) CreateKey(name string, isRoot bool) (*CreatedKey, error) {
	return ks.CreateKeyWithOpts(CreateKeyOpts{
		Name:   name,
		IsRoot: isRoot,
		Scope:  ScopeFull,
	})
}

// CreateKeyWithOpts generates a new API key with full configuration options.
// The plaintext token is returned once and never stored.
func (ks *KeyStore) CreateKeyWithOpts(opts CreateKeyOpts) (*CreatedKey, error) {
	scope := opts.Scope
	if scope == "" {
		scope = ScopeFull
	}
	k, token, err := newKey(opts.Name, opts.IsRoot, scope)
	if err != nil {
		return nil, fmt.Errorf("auth.CreateKey: %w", err)
	}
	k.Description = opts.Description
	k.ExpiresAt = opts.ExpiresAt

	ks.mu.Lock()
	defer ks.mu.Unlock()

	if ks.byID[k.ID] != nil {
		return nil, fmt.Errorf("auth.CreateKey: key id %q is taken", k.ID)
	}
	if err := ks.replaceLocked(append(slices.Clip(ks.keys), k)); err != nil {
		return nil, fmt.Errorf("auth.CreateKey: %w", err)
	}

	return &CreatedKey{
		KeyInfo:         k.info(),
		Token:           token,
		APIKeyComposite: k.ID + ":" + token,
	}, nil
}

// newKey generates a key and its token.
func newKey(name string, isRoot bool, scope Scope) (*storedKey, string, error) {
	keyType := KeyTypeRegular
	if isRoot {
		keyType = KeyTypeRoot
	}
	id, token, err := GenerateKey(keyType)
	if err != nil {
		return nil, "", err
	}
	k := &storedKey{
		APIKey: APIKey{
			ID:        id,
			Name:      name,
			Scope:     scope,
			CreatedAt: time.Now().UTC(),
			IsRoot:    isRoot,
		},
		digest: sha256.Sum256([]byte(token)),
	}
	k.SHA256 = hex.EncodeToString(k.digest[:])

	return k, token, nil
}

// Verify returns the key a token names, or nil when the token names no
// stored key, does not match it, or the key has expired.
func (ks *KeyStore) Verify(token string) *KeyInfo {
	id, ok := KeyIDFromToken(token)
	if !ok {
		return nil
	}
	digest := sha256.Sum256([]byte(token))

	ks.mu.RLock()
	defer ks.mu.RUnlock()

	k := ks.byID[id]
	if k == nil || subtle.ConstantTimeCompare(digest[:], k.digest[:]) != 1 {
		return nil
	}
	now := time.Now()
	if !k.ExpiresAt.IsZero() && now.After(k.ExpiresAt) {
		return nil
	}
	k.lastUsed.Store(now.UnixNano())
	if !ks.dirty.Swap(true) && !ks.closed {
		time.AfterFunc(lastUsedFlushInterval, ks.flushLastUsed)
	}
	info := k.info()

	return &info
}

// flushLastUsed writes the last use of the keys, unless the store is closed.
func (ks *KeyStore) flushLastUsed() {
	ks.mu.RLock()
	defer ks.mu.RUnlock()
	if ks.closed {
		return
	}
	ks.saveMu.Lock()
	defer ks.saveMu.Unlock()
	ks.dirty.Store(false)
	if err := writeKeys(ks.path, ks.keys); err != nil {
		slog.Error("keystore: failed to persist last_used_at", "path", ks.path, "error", err)
	}
}

// Close writes the last use of the keys if it changed since keys.json was
// last written; uses after Close are not written.
func (ks *KeyStore) Close() error {
	ks.mu.Lock()
	defer ks.mu.Unlock()
	if ks.closed {
		return nil
	}
	ks.closed = true
	if !ks.dirty.Swap(false) {
		return nil
	}
	if err := writeKeys(ks.path, ks.keys); err != nil {
		return fmt.Errorf("auth.KeyStore.Close: %w", err)
	}

	return nil
}

// List returns public info for all stored keys.
func (ks *KeyStore) List() []KeyInfo {
	ks.mu.RLock()
	defer ks.mu.RUnlock()

	infos := make([]KeyInfo, len(ks.keys))
	for i, k := range ks.keys {
		infos[i] = k.info()
	}

	return infos
}

// Revoke removes a key by ID. Returns an error if the key is the last root key.
func (ks *KeyStore) Revoke(id string) error {
	ks.mu.Lock()
	defer ks.mu.Unlock()

	k := ks.byID[id]
	if k == nil {
		return fmt.Errorf("auth.Revoke: key %q not found", id)
	}
	if k.IsRoot && ks.rootCountLocked() <= 1 {
		return ErrLastRootKey
	}
	if err := ks.replaceLocked(slices.DeleteFunc(slices.Clone(ks.keys), func(o *storedKey) bool { return o == k })); err != nil {
		return fmt.Errorf("auth.Revoke: %w", err)
	}

	return nil
}

func (ks *KeyStore) rootCountLocked() int {
	n := 0
	for _, k := range ks.keys {
		if k.IsRoot {
			n++
		}
	}

	return n
}

// RotateRoot creates a new root key and revokes the old one identified by oldID.
// Returns the new key with its plaintext token.
func (ks *KeyStore) RotateRoot(oldID string) (*CreatedKey, error) {
	k, token, err := newKey("root", true, ScopeFull)
	if err != nil {
		return nil, fmt.Errorf("auth.RotateRoot: %w", err)
	}

	ks.mu.Lock()
	defer ks.mu.Unlock()

	old := ks.byID[oldID]
	if old == nil {
		return nil, fmt.Errorf("auth.RotateRoot: key %q not found", oldID)
	}
	if !old.IsRoot {
		return nil, fmt.Errorf("auth.RotateRoot: key %q is not a root key", oldID)
	}
	if ks.byID[k.ID] != nil {
		return nil, fmt.Errorf("auth.RotateRoot: key id %q is taken", k.ID)
	}
	keys := slices.DeleteFunc(slices.Clone(ks.keys), func(o *storedKey) bool { return o == old })
	if err := ks.replaceLocked(append(keys, k)); err != nil {
		return nil, fmt.Errorf("auth.RotateRoot: %w", err)
	}

	return &CreatedKey{
		KeyInfo:         k.info(),
		Token:           token,
		APIKeyComposite: k.ID + ":" + token,
	}, nil
}

// replaceLocked writes keys to disk and, once they are written, makes them
// the stored keys. Caller must hold ks.mu exclusively.
func (ks *KeyStore) replaceLocked(keys []*storedKey) error {
	wasDirty := ks.dirty.Swap(false)
	if err := writeKeys(ks.path, keys); err != nil {
		ks.dirty.Store(wasDirty)
		return err
	}
	ks.keys = keys
	ks.byID = make(map[string]*storedKey, len(keys))
	for _, k := range keys {
		ks.byID[k.ID] = k
	}

	return nil
}

// writeKeys atomically replaces the file at path with keys.
func writeKeys(path string, keys []*storedKey) error {
	f := keysFile{Version: keysFileVersion, Keys: make([]APIKey, len(keys))}
	for i, k := range keys {
		f.Keys[i] = k.record()
	}

	data, err := json.MarshalIndent(f, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal keys: %w", err)
	}

	// Atomic write: write to temp file, then rename.
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".keys-*.tmp")
	if err != nil {
		return fmt.Errorf("create temp: %w", err)
	}

	tmpName := tmp.Name()

	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpName)

		return fmt.Errorf("write temp: %w", err)
	}

	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpName)

		return fmt.Errorf("close temp: %w", err)
	}

	if err := os.Chmod(tmpName, 0o600); err != nil {
		_ = os.Remove(tmpName)

		return fmt.Errorf("chmod temp: %w", err)
	}

	if err := os.Rename(tmpName, path); err != nil {
		_ = os.Remove(tmpName)

		return fmt.Errorf("rename temp: %w", err)
	}

	return nil
}
