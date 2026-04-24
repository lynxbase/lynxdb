package install

import (
	"fmt"
	"os/exec"
	"os/user"
	"runtime"
)

// ensureSystemUser creates the system user and group if they do not exist.
// On Linux: uses groupadd/useradd.
// Returns a detail string describing what happened.
func ensureSystemUser(userName, groupName, homeDir string) (string, error) {
	switch runtime.GOOS {
	case "linux":
		return ensureSystemUserLinux(userName, groupName, homeDir)
	default:
		return "(skipped, unsupported OS)", nil
	}
}

func ensureSystemUserLinux(userName, groupName, homeDir string) (string, error) {
	groupExists := userGroupExists(groupName)
	userExists := userExists(userName)

	if groupExists && userExists {
		return fmt.Sprintf("%s:%s (already exists)", userName, groupName), nil
	}

	if !groupExists {
		cmd := exec.Command("groupadd", "--system", groupName) //nolint:gosec
		if out, err := cmd.CombinedOutput(); err != nil {
			return "", fmt.Errorf("install.ensureSystemUser: groupadd: %s: %w", string(out), err)
		}
	}

	if !userExists {
		args := []string{
			"--system",
			"--gid", groupName,
			"--shell", "/usr/sbin/nologin",
			"--home-dir", homeDir,
			"--no-create-home",
			userName,
		}

		cmd := exec.Command("useradd", args...) //nolint:gosec
		if out, err := cmd.CombinedOutput(); err != nil {
			return "", fmt.Errorf("install.ensureSystemUser: useradd: %s: %w", string(out), err)
		}
	}

	return fmt.Sprintf("%s:%s", userName, groupName), nil
}

// userExists reports whether the given user exists on the system.
func userExists(name string) bool {
	_, err := user.Lookup(name)
	return err == nil
}

// userGroupExists reports whether the given group exists on the system.
func userGroupExists(name string) bool {
	_, err := user.LookupGroup(name)
	return err == nil
}
