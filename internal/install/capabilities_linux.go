//go:build linux

package install

import (
	"fmt"
	"os/exec"
)

// setCapabilities sets CAP_NET_BIND_SERVICE on the binary so it can
// bind to privileged ports (< 1024) without running as root.
// Returns a detail string. Warns and skips if setcap is not found.
func setCapabilities(binaryPath string) (string, error) {
	setcapPath, err := exec.LookPath("setcap")
	if err != nil {
		return "(skipped, setcap not found)", nil
	}

	cmd := exec.Command(setcapPath, "cap_net_bind_service=+ep", binaryPath) //nolint:gosec
	if out, err := cmd.CombinedOutput(); err != nil {
		return "", fmt.Errorf("install.setCapabilities: setcap: %s: %w", string(out), err)
	}

	return "cap_net_bind_service", nil
}
