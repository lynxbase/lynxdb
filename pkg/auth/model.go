// Package auth implements API key authentication for LynxDB.
//
// Keys use the format lynx_{type}_{12 alphanumeric chars}_{32 alphanumeric chars},
// where type is "rk" (root key) or "ak" (regular API key) and the part before
// the last "_" (without "lynx_") is the ID of the key.
// Root keys can manage other keys; regular keys can only query/ingest.
//
// Keys are stored server-side as SHA-256 digests in {data_dir}/auth/keys.json.
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

// Scope controls what operations a key can perform.
type Scope string

const (
	// ScopeIngest allows sending events: /ingest/*, the Elasticsearch bulk and
	// document routes, Splunk HEC and the OTLP listeners.
	ScopeIngest Scope = "ingest"
	// ScopeQuery allows running queries and reading their jobs.
	ScopeQuery Scope = "query"
	// ScopeAdmin allows managing partitions, reading the config and canceling jobs.
	ScopeAdmin Scope = "admin"
	// ScopeFull allows access to all endpoints (default).
	ScopeFull Scope = "full"
)

// ValidScope reports whether s is a recognized scope value.
func ValidScope(s string) bool {
	switch Scope(s) {
	case ScopeIngest, ScopeQuery, ScopeAdmin, ScopeFull:
		return true
	default:
		return false
	}
}

// APIKey is a stored API key.
type APIKey struct {
	// ID is a unique identifier for the key (e.g. "ak_7fA3mNpQ9xKv"), also part of its token.
	ID string `json:"id"`
	// SHA256 is the hex SHA-256 digest of the token.
	SHA256 string `json:"sha256"`
	// Name is a human-readable label for the key.
	Name string `json:"name"`
	// Scope controls what operations this key can perform.
	Scope Scope `json:"scope"`
	// Description is an optional human-readable description of the key's purpose.
	Description string `json:"description,omitempty"`
	// ExpiresAt is when the key expires. Zero value means never.
	ExpiresAt time.Time `json:"expires_at,omitzero"`
	// CreatedAt is when the key was created.
	CreatedAt time.Time `json:"created_at"`
	// LastUsedAt is the last time the key was used for authentication.
	// Zero value means never used.
	LastUsedAt time.Time `json:"last_used_at,omitzero"`
	// IsRoot indicates whether this is a root key that can manage other keys.
	IsRoot bool `json:"is_root"`
}

// KeyInfo is the public representation of an API key (no hash, no token).
// Used in key listings and similar responses.
type KeyInfo struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	Prefix      string    `json:"prefix"`
	Scope       Scope     `json:"scope"`
	Description string    `json:"description,omitempty"`
	ExpiresAt   time.Time `json:"expires_at,omitempty"`
	IsRoot      bool      `json:"is_root"`
	CreatedAt   time.Time `json:"created_at"`
	LastUsedAt  time.Time `json:"last_used_at,omitempty"`
}

// Allows reports whether the key may perform operations of the given scope.
// Root keys and full-scope keys may perform all of them.
func (k *KeyInfo) Allows(scope Scope) bool {
	return k.IsRoot || k.Scope == ScopeFull || k.Scope == scope
}

// CreatedKey is returned when a new key is created. It includes the plaintext
// token which is shown only once and never stored.
type CreatedKey struct {
	KeyInfo
	// Token is the full plaintext API key. Only returned at creation time.
	Token string `json:"token"`
	// APIKeyComposite is "id:token" for the Elasticsearch ApiKey header of shippers.
	APIKeyComposite string `json:"api_key"`
}

// CreateKeyOpts configures key creation parameters.
type CreateKeyOpts struct {
	Name        string
	IsRoot      bool
	Scope       Scope
	Description string
	ExpiresAt   time.Time // Zero value means never expires.
}

// Info returns the public KeyInfo for an APIKey.
func (k *APIKey) Info() KeyInfo {
	return KeyInfo{
		ID:          k.ID,
		Name:        k.Name,
		Prefix:      keyPrefix + k.ID,
		Scope:       k.Scope,
		Description: k.Description,
		ExpiresAt:   k.ExpiresAt,
		IsRoot:      k.IsRoot,
		CreatedAt:   k.CreatedAt,
		LastUsedAt:  k.LastUsedAt,
	}
}
