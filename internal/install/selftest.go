package install

import (
	"fmt"
	"os/exec"
	"strings"
)

// runSelfTest verifies the installed binary works by running
// `lynxdb version --short`. Returns the version string on success.
func runSelfTest(binaryPath string) (string, error) {
	cmd := exec.Command(binaryPath, "version", "--short") //nolint:gosec
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("install.runSelfTest: %s: %w", strings.TrimSpace(string(out)), err)
	}

	version := strings.TrimSpace(string(out))
	if version == "" {
		return "", fmt.Errorf("install.runSelfTest: empty version output")
	}

	return fmt.Sprintf("passed (%s)", version), nil
}
