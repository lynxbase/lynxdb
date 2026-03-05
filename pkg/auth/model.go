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

// Scope controls what operations a key can perform.
type Scope string

const (
	// ScopeIngest allows POST to ingest endpoints (_bulk, /ingest/*, /otlp/*).
	ScopeIngest Scope = "ingest"
	// ScopeQuery allows GET/POST to query, fields, sources, histogram, tail.
	ScopeQuery Scope = "query"
	// ScopeAdmin allows access to auth management, config, cache, stats.
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

// APIKey represents a stored API key with its argon2id hash.
type APIKey struct {
	// ID is a unique identifier for the key (e.g. "ak_7fA3mNpQ9xKv").
	ID string `json:"id"`
	// Prefix is the first 12 characters of the full key (for display).
	Prefix string `json:"prefix"`
	// Hash is the argon2id hash of the full key.
	Hash string `json:"hash"`
	// Name is a human-readable label for the key.
	Name string `json:"name"`
	// Scope controls what operations this key can perform.
	// Empty or missing defaults to "full" for backward compatibility.
	Scope Scope `json:"scope,omitempty"`
	// Description is an optional human-readable description of the key's purpose.
	Description string `json:"description,omitempty"`
	// ExpiresAt is when the key expires. Zero value means never.
	ExpiresAt time.Time `json:"expires_at,omitempty"`
	// CreatedAt is when the key was created.
	CreatedAt time.Time `json:"created_at"`
	// LastUsedAt is the last time the key was used for authentication.
	// Zero value means never used.
	LastUsedAt time.Time `json:"last_used_at,omitempty"`
	// IsRoot indicates whether this is a root key that can manage other keys.
	IsRoot bool `json:"is_root"`
}

// EffectiveScope returns the key's scope, defaulting to ScopeFull when empty.
// This ensures backward compatibility with keys created before scopes existed.
func (k *APIKey) EffectiveScope() Scope {
	if k.Scope == "" {
		return ScopeFull
	}
	return k.Scope
}

// KeyInfo is the public representation of an API key (no hash, no token).
// Used in list-keys and similar responses.
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

// CreatedKey is returned when a new key is created. It includes the plaintext
// token which is shown only once and never stored.
type CreatedKey struct {
	KeyInfo
	// Token is the full plaintext API key. Only returned at creation time.
	Token string `json:"token"`
	// APIKeyComposite is "id:token" for Elasticsearch/Filebeat ApiKey header compatibility.
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
		Prefix:      k.Prefix,
		Scope:       k.EffectiveScope(),
		Description: k.Description,
		ExpiresAt:   k.ExpiresAt,
		IsRoot:      k.IsRoot,
		CreatedAt:   k.CreatedAt,
		LastUsedAt:  k.LastUsedAt,
	}
}
