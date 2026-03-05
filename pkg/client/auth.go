package client

import (
	"context"
	"time"
)

// AuthKeyInfo is the public representation of an API key (no token or hash).
type AuthKeyInfo struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	Prefix      string    `json:"prefix"`
	Scope       string    `json:"scope"`
	Description string    `json:"description,omitempty"`
	ExpiresAt   time.Time `json:"expires_at,omitempty"`
	IsRoot      bool      `json:"is_root"`
	CreatedAt   time.Time `json:"created_at"`
	LastUsedAt  time.Time `json:"last_used_at,omitempty"`
}

// AuthCreatedKey is returned when a new key is created, including the one-time token.
type AuthCreatedKey struct {
	AuthKeyInfo
	Token           string `json:"token"`
	APIKeyComposite string `json:"api_key"`
}

// AuthRotatedKey is returned from rotate-root, including the new token and revoked key ID.
type AuthRotatedKey struct {
	ID           string    `json:"id"`
	Name         string    `json:"name"`
	Prefix       string    `json:"prefix"`
	Token        string    `json:"token"`
	IsRoot       bool      `json:"is_root"`
	RevokedKeyID string    `json:"revoked_key_id"`
	CreatedAt    time.Time `json:"created_at"`
}

// CreateKeyInput holds parameters for creating a new API key.
type CreateKeyInput struct {
	Name        string `json:"name"`
	Scope       string `json:"scope,omitempty"`
	Description string `json:"description,omitempty"`
	ExpiresIn   string `json:"expires_in,omitempty"`
}

// AuthCreateKey creates a new API key with default options. Requires a root key.
func (c *Client) AuthCreateKey(ctx context.Context, name string) (*AuthCreatedKey, error) {
	return c.AuthCreateKeyWithOpts(ctx, CreateKeyInput{Name: name})
}

// AuthCreateKeyWithOpts creates a new API key with full options. Requires a root key.
func (c *Client) AuthCreateKeyWithOpts(ctx context.Context, input CreateKeyInput) (*AuthCreatedKey, error) {
	var result AuthCreatedKey

	_, err := c.doJSON(ctx, "POST", "/auth/keys", input, &result)
	if err != nil {
		return nil, err
	}

	return &result, nil
}

// AuthListKeys lists all API keys. Requires a root key.
func (c *Client) AuthListKeys(ctx context.Context) ([]AuthKeyInfo, error) {
	var result struct {
		Keys []AuthKeyInfo `json:"keys"`
	}

	_, err := c.doJSON(ctx, "GET", "/auth/keys", nil, &result)
	if err != nil {
		return nil, err
	}

	return result.Keys, nil
}

// AuthRevokeKey revokes an API key by ID. Requires a root key.
func (c *Client) AuthRevokeKey(ctx context.Context, id string) error {
	return c.doNoContent(ctx, "DELETE", "/auth/keys/"+id)
}

// AuthRotateRoot rotates the root key. Requires a root key.
func (c *Client) AuthRotateRoot(ctx context.Context) (*AuthRotatedKey, error) {
	var result AuthRotatedKey

	_, err := c.doJSON(ctx, "POST", "/auth/rotate-root", nil, &result)
	if err != nil {
		return nil, err
	}

	return &result, nil
}
