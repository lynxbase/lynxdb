package auth

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"
)

// CredentialsFile manages the client-side credentials store.
// Located at ~/.config/lynxdb/credentials.yaml (or OS-appropriate config dir).
type credentialsFile struct {
	Servers map[string]serverEntry `yaml:"servers"`
}

type serverEntry struct {
	Token       string `yaml:"token"`
	Fingerprint string `yaml:"fingerprint,omitempty"`
}

// CredentialsPath returns the path to the credentials file using os.UserConfigDir.
func CredentialsPath() (string, error) {
	dir, err := os.UserConfigDir()
	if err != nil {
		return "", fmt.Errorf("auth.CredentialsPath: %w", err)
	}

	return filepath.Join(dir, "lynxdb", "credentials.yaml"), nil
}

// LoadToken reads the stored token for a given server URL from the credentials file.
// Returns empty string if no credentials exist for this server.
func LoadToken(serverURL string) (string, error) {
	path, err := CredentialsPath()
	if err != nil {
		return "", err
	}

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return "", nil
		}

		return "", fmt.Errorf("auth.LoadToken: read credentials: %w", err)
	}

	var f credentialsFile
	if err := yaml.Unmarshal(data, &f); err != nil {
		return "", fmt.Errorf("auth.LoadToken: parse credentials: %w", err)
	}

	normalized := normalizeURL(serverURL)
	if entry, ok := f.Servers[normalized]; ok {
		return entry.Token, nil
	}

	return "", nil
}

// SaveToken writes a token for a server URL to the credentials file.
// Creates the file and parent directories if they don't exist.
func SaveToken(serverURL, token string) error {
	path, err := CredentialsPath()
	if err != nil {
		return err
	}

	// Load existing file.
	var f credentialsFile

	data, readErr := os.ReadFile(path)
	if readErr == nil {
		if err := yaml.Unmarshal(data, &f); err != nil {
			return fmt.Errorf("auth.SaveToken: parse existing credentials: %w", err)
		}
	}

	if f.Servers == nil {
		f.Servers = make(map[string]serverEntry)
	}

	f.Servers[normalizeURL(serverURL)] = serverEntry{Token: token}

	return writeCredentials(path, &f)
}

// SaveCredentials writes a token and optional TLS fingerprint for a server URL.
// Creates the file and parent directories if they don't exist.
func SaveCredentials(serverURL, token, fingerprint string) error {
	path, err := CredentialsPath()
	if err != nil {
		return err
	}

	f := loadCredentialsFile(path)

	entry := f.Servers[normalizeURL(serverURL)]
	if token != "" {
		entry.Token = token
	}
	entry.Fingerprint = fingerprint
	f.Servers[normalizeURL(serverURL)] = entry

	return writeCredentials(path, f)
}

// LoadCredentials reads the stored token and fingerprint for a server URL.
// Returns empty strings if no credentials exist for this server.
func LoadCredentials(serverURL string) (token, fingerprint string, err error) {
	path, pathErr := CredentialsPath()
	if pathErr != nil {
		return "", "", pathErr
	}

	data, readErr := os.ReadFile(path)
	if readErr != nil {
		if os.IsNotExist(readErr) {
			return "", "", nil
		}

		return "", "", fmt.Errorf("auth.LoadCredentials: read credentials: %w", readErr)
	}

	var f credentialsFile
	if unmarshalErr := yaml.Unmarshal(data, &f); unmarshalErr != nil {
		return "", "", fmt.Errorf("auth.LoadCredentials: parse credentials: %w", unmarshalErr)
	}

	normalized := normalizeURL(serverURL)
	if entry, ok := f.Servers[normalized]; ok {
		return entry.Token, entry.Fingerprint, nil
	}

	return "", "", nil
}

// SaveFingerprint saves just the TLS fingerprint for a server URL,
// preserving any existing token. Used when TLS is enabled but auth is not.
func SaveFingerprint(serverURL, fingerprint string) error {
	path, err := CredentialsPath()
	if err != nil {
		return err
	}

	f := loadCredentialsFile(path)

	normalized := normalizeURL(serverURL)
	entry := f.Servers[normalized]
	entry.Fingerprint = fingerprint
	f.Servers[normalized] = entry

	return writeCredentials(path, f)
}

// RemoveToken removes the stored token for a server URL.
// Returns true if a token was actually removed.
func RemoveToken(serverURL string) (bool, error) {
	path, err := CredentialsPath()
	if err != nil {
		return false, err
	}

	data, readErr := os.ReadFile(path)
	if readErr != nil {
		if os.IsNotExist(readErr) {
			return false, nil
		}

		return false, fmt.Errorf("auth.RemoveToken: read credentials: %w", readErr)
	}

	var f credentialsFile
	if err := yaml.Unmarshal(data, &f); err != nil {
		return false, fmt.Errorf("auth.RemoveToken: parse credentials: %w", err)
	}

	normalized := normalizeURL(serverURL)

	if _, ok := f.Servers[normalized]; !ok {
		return false, nil
	}

	delete(f.Servers, normalized)

	if err := writeCredentials(path, &f); err != nil {
		return false, err
	}

	return true, nil
}

// RemoveAll removes all stored credentials.
// Returns the number of server entries that were removed.
func RemoveAll() (int, error) {
	path, err := CredentialsPath()
	if err != nil {
		return 0, err
	}

	data, readErr := os.ReadFile(path)
	if readErr != nil {
		if os.IsNotExist(readErr) {
			return 0, nil
		}

		return 0, fmt.Errorf("auth.RemoveAll: read credentials: %w", readErr)
	}

	var f credentialsFile
	if err := yaml.Unmarshal(data, &f); err != nil {
		return 0, fmt.Errorf("auth.RemoveAll: parse credentials: %w", err)
	}

	count := len(f.Servers)
	if count == 0 {
		return 0, nil
	}

	f.Servers = make(map[string]serverEntry)

	if err := writeCredentials(path, &f); err != nil {
		return 0, err
	}

	return count, nil
}

// CheckPermissions warns if the credentials file has overly permissive permissions.
// Returns a warning message, or empty string if permissions are fine.
func CheckPermissions() string {
	path, err := CredentialsPath()
	if err != nil {
		return ""
	}

	info, err := os.Stat(path)
	if err != nil {
		return ""
	}

	mode := info.Mode().Perm()
	if mode&0o077 != 0 {
		return fmt.Sprintf("credentials file %s has permissions %o (expected 0600)", path, mode)
	}

	return ""
}

// loadCredentialsFile reads and parses the credentials file, returning an
// empty but initialized struct if the file does not exist or cannot be parsed.
func loadCredentialsFile(path string) *credentialsFile {
	var f credentialsFile

	data, err := os.ReadFile(path)
	if err == nil {
		_ = yaml.Unmarshal(data, &f)
	}

	if f.Servers == nil {
		f.Servers = make(map[string]serverEntry)
	}

	return &f
}

// writeCredentials writes the credentials file with secure permissions.
func writeCredentials(path string, f *credentialsFile) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("auth: create credentials dir: %w", err)
	}

	data, err := yaml.Marshal(f)
	if err != nil {
		return fmt.Errorf("auth: marshal credentials: %w", err)
	}

	// Prepend a comment header.
	header := []byte("# Managed by 'lynxdb login'. Do not edit manually.\n")
	content := append(header, data...)

	if err := os.WriteFile(path, content, 0o600); err != nil {
		return fmt.Errorf("auth: write credentials: %w", err)
	}

	return nil
}

// normalizeURL removes trailing slashes from a URL.
func normalizeURL(u string) string {
	return strings.TrimRight(u, "/")
}
