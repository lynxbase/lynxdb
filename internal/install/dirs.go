package install

import (
	"fmt"
	"os"
	"runtime"
	"strings"
)

// createDirectories creates the data, log, and (optionally) run directories
// with appropriate ownership and permissions.
func createDirectories(opts Options, paths Paths) (string, error) {
	type dirEntry struct {
		path  string
		perm  os.FileMode
		owner string // "user:group" for chown (system mode only)
	}

	var dirs []dirEntry

	if opts.Mode == ModeSystem {
		userGroup := opts.User + ":" + opts.Group
		dirs = []dirEntry{
			{paths.ConfigDir, 0o755, "root:root"},
			{paths.DataDir, 0o750, userGroup},
			{paths.LogDir, 0o750, userGroup},
		}
	} else {
		dirs = []dirEntry{
			{paths.ConfigDir, 0o755, ""},
			{paths.DataDir, 0o755, ""},
			{paths.LogDir, 0o755, ""},
		}
	}

	var created []string

	for _, d := range dirs {
		if err := os.MkdirAll(d.path, d.perm); err != nil {
			return "", fmt.Errorf("install.createDirectories: %s: %w", d.path, err)
		}

		// Ensure permissions are correct even if the directory already existed.
		if err := os.Chmod(d.path, d.perm); err != nil {
			return "", fmt.Errorf("install.createDirectories: chmod %s: %w", d.path, err)
		}

		// Set ownership on system installs (Linux/FreeBSD).
		if d.owner != "" && opts.Mode == ModeSystem && runtime.GOOS != "windows" && runtime.GOOS != "darwin" {
			parts := strings.SplitN(d.owner, ":", 2)
			if len(parts) == 2 {
				chownByName(d.path, parts[0], parts[1])
			}
		}

		created = append(created, d.path)
	}

	return strings.Join(created, ", "), nil
}
