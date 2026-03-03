package auth

import (
	"context"
)

// ContextKey type for auth values stored in request context.
type contextKey struct{}

// WithKeyInfo returns a new context carrying the authenticated key info.
func WithKeyInfo(ctx context.Context, info *KeyInfo) context.Context {
	return context.WithValue(ctx, contextKey{}, info)
}

// KeyInfoFromContext extracts the authenticated key info from ctx.
// Returns nil if the request was not authenticated (auth disabled or no token).
func KeyInfoFromContext(ctx context.Context) *KeyInfo {
	v, _ := ctx.Value(contextKey{}).(*KeyInfo)

	return v
}

// IsRoot reports whether the request context carries a root key.
func IsRoot(ctx context.Context) bool {
	info := KeyInfoFromContext(ctx)

	return info != nil && info.IsRoot
}
