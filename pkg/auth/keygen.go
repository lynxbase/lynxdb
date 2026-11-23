package auth

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"strings"
)

const (
	// keyPrefix is the common prefix for all LynxDB API keys.
	keyPrefix = "lynx_"
	// keyIDRandomLen is the number of random alphanumeric characters in a key ID.
	keyIDRandomLen = 12
	// keySecretLen is the number of random alphanumeric characters in the secret of a key.
	keySecretLen = 32
	// keyIDLen is the length of a key ID: the key type, "_" and the random part.
	keyIDLen = 2 + 1 + keyIDRandomLen
	// tokenLen is the length of a token: the prefix, the key ID, "_" and the secret.
	tokenLen = len(keyPrefix) + keyIDLen + 1 + keySecretLen
)

// alphabet used for random key generation (alphanumeric: a-zA-Z0-9).
const alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

// GenerateKey creates a new random API key of the given type and returns its
// ID and its token. The token carries the ID before its secret:
// "lynx_ak_7fA3mNpQ9xKv_d6GncfBAepfJBd0Kh8oOOL8dKLzdocJ2" has the ID
// "ak_7fA3mNpQ9xKv".
func GenerateKey(keyType KeyType) (id, token string, err error) {
	idPart, err := randomAlphanumeric(keyIDRandomLen)
	if err != nil {
		return "", "", fmt.Errorf("auth.GenerateKey: %w", err)
	}
	secret, err := randomAlphanumeric(keySecretLen)
	if err != nil {
		return "", "", fmt.Errorf("auth.GenerateKey: %w", err)
	}
	id = string(keyType) + "_" + idPart

	return id, keyPrefix + id + "_" + secret, nil
}

// KeyIDFromToken returns the ID of the key a token names. It reports false
// when the token does not have the form of a LynxDB API key.
func KeyIDFromToken(token string) (string, bool) {
	if len(token) != tokenLen || !strings.HasPrefix(token, keyPrefix) {
		return "", false
	}
	id := token[len(keyPrefix) : len(keyPrefix)+keyIDLen]
	if !validKeyID(id) || token[len(keyPrefix)+keyIDLen] != '_' {
		return "", false
	}

	return id, true
}

// validKeyID reports whether id has the form of a key ID.
func validKeyID(id string) bool {
	if len(id) != keyIDLen || id[2] != '_' {
		return false
	}
	switch KeyType(id[:2]) {
	case KeyTypeRoot, KeyTypeRegular:
		return true
	default:
		return false
	}
}

// KeyPrefix returns the public part of a token, the prefix and the key ID,
// which may be shown without disclosing the key.
func KeyPrefix(token string) string {
	if id, ok := KeyIDFromToken(token); ok {
		return keyPrefix + id
	}

	return token[:min(len(token), len(keyPrefix)+3)]
}

// randomAlphanumeric generates n cryptographically random alphanumeric characters.
func randomAlphanumeric(n int) (string, error) {
	alphabetLen := big.NewInt(int64(len(alphabet)))
	buf := make([]byte, n)

	for i := range buf {
		idx, err := rand.Int(rand.Reader, alphabetLen)
		if err != nil {
			return "", fmt.Errorf("crypto/rand: %w", err)
		}

		buf[i] = alphabet[idx.Int64()]
	}

	return string(buf), nil
}
