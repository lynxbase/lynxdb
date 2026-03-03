//go:build linux

package install

import (
	"fmt"
	"os"
	"path/filepath"
)

const ulimitsContent = `# LynxDB file descriptor and process limits.
# Installed by 'lynxdb install'. See limits.conf(5) for syntax.
lynxdb  soft  nofile   1048576
lynxdb  hard  nofile   1048576
lynxdb  soft  nproc    65536
lynxdb  hard  nproc    65536
lynxdb  soft  memlock  unlimited
lynxdb  hard  memlock  unlimited
`

// configureUlimits writes a limits.d file for the lynxdb user.
// Only applies in system mode on Linux.
func configureUlimits(opts Options) (string, error) {
	if opts.Mode != ModeSystem {
		return "(skipped, user mode)", nil
	}

	limitsDir := "/etc/security/limits.d"
	if err := os.MkdirAll(limitsDir, 0o755); err != nil {
		return "", fmt.Errorf("install.configureUlimits: create limits.d: %w", err)
	}

	limitsFile := filepath.Join(limitsDir, "lynxdb.conf")
	if err := os.WriteFile(limitsFile, []byte(ulimitsContent), 0o644); err != nil {
		return "", fmt.Errorf("install.configureUlimits: write: %w", err)
	}

	return limitsFile, nil
}
