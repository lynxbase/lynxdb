package install

import (
	"fmt"
	"os"
	"os/exec"
	"runtime"
)

// UninstallOptions configures the uninstall process.
type UninstallOptions struct {
	Mode  Mode
	Yes   bool // skip confirmation prompts
	Purge bool // also remove config files, ulimits, and system user (data preserved)
}

// UninstallResult captures what the uninstall actually did.
type UninstallResult struct {
	ServiceStopped  bool
	ServiceRemoved  bool
	BinaryRemoved   bool
	ConfigRemoved   bool
	UlimitsRemoved  bool
	UserRemoved     bool
	DataPreservedAt string
}

// DetectInstallMode inspects the filesystem to determine how LynxDB was installed.
// Checks for system-mode artifacts (systemd unit, launchd daemon) first, then
// falls back to user-mode artifacts (launchd agent).
func DetectInstallMode() (Mode, Paths) {
	sysPaths := resolveSystemPaths(Options{})
	if _, err := os.Stat(sysPaths.ServiceFile); err == nil {
		return ModeSystem, sysPaths
	}

	if _, err := os.Stat(sysPaths.Binary); err == nil {
		return ModeSystem, sysPaths
	}

	userPaths := resolveUserPaths(Options{})
	if userPaths.ServiceFile != "" {
		if _, err := os.Stat(userPaths.ServiceFile); err == nil {
			return ModeUser, userPaths
		}
	}

	if _, err := os.Stat(userPaths.Binary); err == nil {
		return ModeUser, userPaths
	}

	// Default: guess based on privileges.
	if os.Geteuid() == 0 {
		return ModeSystem, sysPaths
	}

	return ModeUser, userPaths
}

// UninstallSteps returns the ordered steps for uninstalling LynxDB.
func UninstallSteps(opts UninstallOptions, paths Paths) []Step {
	var steps []Step

	steps = append(steps, Step{
		Name: "Stop service",
		Fn: func() (string, error) {
			return stopService(opts.Mode, paths)
		},
	})

	steps = append(steps, Step{
		Name: "Remove service",
		Fn: func() (string, error) {
			return removeServiceFile(paths)
		},
	})

	steps = append(steps, Step{
		Name: "Remove binary",
		Fn: func() (string, error) {
			return removeFile(paths.Binary)
		},
	})

	if opts.Purge {
		steps = append(steps, Step{
			Name: "Remove config",
			Fn: func() (string, error) {
				return removeDir(paths.ConfigDir)
			},
		})

		if opts.Mode == ModeSystem && runtime.GOOS == "linux" {
			steps = append(steps, Step{
				Name: "Remove ulimits",
				Fn: func() (string, error) {
					return removeFile("/etc/security/limits.d/lynxdb.conf")
				},
			})
		}
	}

	return steps
}

// stopService stops the LynxDB service via systemctl or launchctl.
func stopService(mode Mode, paths Paths) (string, error) {
	switch runtime.GOOS {
	case "linux":
		return stopSystemdService(mode)
	case "darwin":
		return stopLaunchdService(mode, paths)
	default:
		return "(skipped)", nil
	}
}

func stopSystemdService(mode Mode) (string, error) {
	systemctl, err := exec.LookPath("systemctl")
	if err != nil {
		return "(systemctl not found)", nil
	}

	args := []string{"stop", "lynxdb"}
	if mode == ModeUser {
		args = []string{"--user", "stop", "lynxdb"}
	}

	// Stop — ignore errors if service is not running.
	cmd := exec.Command(systemctl, args...) //nolint:gosec
	_ = cmd.Run()

	// Disable.
	disableArgs := []string{"disable", "lynxdb"}
	if mode == ModeUser {
		disableArgs = []string{"--user", "disable", "lynxdb"}
	}

	cmd = exec.Command(systemctl, disableArgs...) //nolint:gosec
	_ = cmd.Run()

	return "stopped and disabled", nil
}

func stopLaunchdService(mode Mode, paths Paths) (string, error) {
	if paths.ServiceFile == "" {
		return "(no service file)", nil
	}

	launchctl, err := exec.LookPath("launchctl")
	if err != nil {
		return "(launchctl not found)", nil
	}

	// Unload — ignore errors if not loaded.
	if mode == ModeSystem {
		cmd := exec.Command(launchctl, "unload", paths.ServiceFile) //nolint:gosec
		_ = cmd.Run()
	} else {
		cmd := exec.Command(launchctl, "unload", paths.ServiceFile) //nolint:gosec
		_ = cmd.Run()
	}

	return "unloaded", nil
}

// removeServiceFile removes the service file and runs daemon-reload if needed.
func removeServiceFile(paths Paths) (string, error) {
	if paths.ServiceFile == "" {
		return "(no service file)", nil
	}

	if _, err := os.Stat(paths.ServiceFile); os.IsNotExist(err) {
		return "(not found)", nil
	}

	if err := os.Remove(paths.ServiceFile); err != nil {
		return "", fmt.Errorf("install.removeServiceFile: %w", err)
	}

	// Daemon-reload on Linux.
	if runtime.GOOS == "linux" {
		if systemctl, err := exec.LookPath("systemctl"); err == nil {
			cmd := exec.Command(systemctl, "daemon-reload")
			_ = cmd.Run()
		}
	}

	return paths.ServiceFile, nil
}

// removeFile removes a single file. Returns "(not found)" if it doesn't exist.
func removeFile(path string) (string, error) {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return "(not found)", nil
	}

	if err := os.Remove(path); err != nil {
		return "", fmt.Errorf("install.removeFile: %s: %w", path, err)
	}

	return path, nil
}

// removeDir removes a directory and all its contents.
// Returns "(not found)" if it doesn't exist.
func removeDir(path string) (string, error) {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return "(not found)", nil
	}

	if err := os.RemoveAll(path); err != nil {
		return "", fmt.Errorf("install.removeDir: %s: %w", path, err)
	}

	return path, nil
}
