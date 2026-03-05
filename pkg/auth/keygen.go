package auth

import (
	"crypto/rand"
	"fmt"
	"math/big"
)

const (
	// KeyPrefix constant is the common prefix for all LynxDB API keys.
	keyPrefix = "lynx_"
	// KeyRandomLen constant is the number of random alphanumeric characters in a key.
	keyRandomLen = 32
	// KeyPrefixDisplayLen constant is how many characters of the full key to store as prefix.
	keyPrefixDisplayLen = 12
)

// alphabet used for random key generation (alphanumeric: a-zA-Z0-9).
const alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

// GenerateKey creates a new random API key of the given type.
// Returns the full plaintext token (e.g. "lynx_rk_7fA3mNpQ9xKvL2wB8cD4eF6gH1jR5tY").
func GenerateKey(keyType KeyType) (string, error) {
	random, err := randomAlphanumeric(keyRandomLen)
	if err != nil {
		return "", fmt.Errorf("auth.GenerateKey: %w", err)
	}

	token := keyPrefix + string(keyType) + "_" + random

	return token, nil
}

// KeyPrefix returns the display prefix (first keyPrefixDisplayLen chars) of a token.
func KeyPrefix(token string) string {
	if len(token) > keyPrefixDisplayLen {
		return token[:keyPrefixDisplayLen]
	}

	return token
}

// ParseKeyType extracts the key type from a token string.
// Returns an error if the token format is invalid.
func ParseKeyType(token string) (KeyType, error) {
	if len(token) < len(keyPrefix)+3 { // "lynx_" + "rk" + "_" minimum
		return "", fmt.Errorf("auth.ParseKeyType: token too short")
	}

	if token[:len(keyPrefix)] != keyPrefix {
		return "", fmt.Errorf("auth.ParseKeyType: missing lynx_ prefix")
	}

	rest := token[len(keyPrefix):]

	for i, ch := range rest {
		if ch == '_' {
			kt := KeyType(rest[:i])
			switch kt {
			case KeyTypeRoot, KeyTypeRegular:
				return kt, nil
			default:
				return "", fmt.Errorf("auth.ParseKeyType: unknown key type %q", kt)
			}
		}
	}

	return "", fmt.Errorf("auth.ParseKeyType: invalid token format")
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
