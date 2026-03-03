package auth

import "errors"

// ErrLastRootKey is returned when attempting to revoke the last remaining root key.
var ErrLastRootKey = errors.New("cannot revoke the last root key")
