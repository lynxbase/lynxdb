// Package auth implements API key authentication for LynxDB.
//
// Keys use the format: lynx_{type}_{32 alphanumeric chars}
// where type is "rk" (root key) or "ak" (regular API key).
// Root keys can manage other keys; regular keys can only query/ingest.
//
// Keys are stored server-side as argon2id hashes in {data_dir}/auth/keys.json.
// The plaintext key is shown only once at creation time.
package auth

import "time"

// KeyType distinguishes root keys from regular API keys.
type KeyType string

const (
	// KeyTypeRoot can create/revoke other keys and access auth management endpoints.
	KeyTypeRoot KeyType = "rk"
	// KeyTypeRegular can query and ingest but cannot manage keys.
	KeyTypeRegular KeyType = "ak"
)

// APIKey represents a stored API key with its argon2id hash.
type APIKey struct {
	// ID is a unique identifier for the key (e.g. "key_01JKNM3VXQP").
	ID string `json:"id"`
	// Prefix is the first 12 characters of the full key (for display).
	Prefix string `json:"prefix"`
	// Hash is the argon2id hash of the full key.
	Hash string `json:"hash"`
	// Name is a human-readable label for the key.
	Name string `json:"name"`
	// CreatedAt is when the key was created.
	CreatedAt time.Time `json:"created_at"`
	// LastUsedAt is the last time the key was used for authentication.
	// Zero value means never used.
	LastUsedAt time.Time `json:"last_used_at,omitempty"`
	// IsRoot indicates whether this is a root key that can manage other keys.
	IsRoot bool `json:"is_root"`
}

// KeyInfo is the public representation of an API key (no hash, no token).
// Used in list-keys and similar responses.
type KeyInfo struct {
	ID         string    `json:"id"`
	Name       string    `json:"name"`
	Prefix     string    `json:"prefix"`
	IsRoot     bool      `json:"is_root"`
	CreatedAt  time.Time `json:"created_at"`
	LastUsedAt time.Time `json:"last_used_at,omitempty"`
}

// CreatedKey is returned when a new key is created. It includes the plaintext
// token which is shown only once and never stored.
type CreatedKey struct {
	KeyInfo
	// Token is the full plaintext API key. Only returned at creation time.
	Token string `json:"token"`
}

// Info returns the public KeyInfo for an APIKey.
func (k *APIKey) Info() KeyInfo {
	return KeyInfo{
		ID:         k.ID,
		Name:       k.Name,
		Prefix:     k.Prefix,
		IsRoot:     k.IsRoot,
		CreatedAt:  k.CreatedAt,
		LastUsedAt: k.LastUsedAt,
	}
}
