// Package install implements the `lynxdb install` and `lynxdb uninstall`
// subcommands. It follows the ClickHouse self-install model: the binary
// copies itself to the target location, creates a system user, sets up
// directories, writes a config file, installs a systemd/launchd service,
// configures ulimits and capabilities, and runs a post-install self-test.
//
// Two installation modes are supported:
//   - ModeSystem (root): FHS-compliant paths, systemd service, dedicated user
//   - ModeUser (non-root): XDG paths, launchd agent (macOS), no user creation
package install

import (
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

// Mode selects between system-wide and user-local installation.
type Mode int

const (
	// ModeSystem is a root installation: /usr/local/bin, /etc/lynxdb,
	// /var/lib/lynxdb, systemd service, dedicated user.
	ModeSystem Mode = iota
	// ModeUser is a non-root installation: ~/.local/bin, ~/.config/lynxdb,
	// XDG data dir, launchd agent (macOS).
	ModeUser
)

// String returns a human-readable mode name.
func (m Mode) String() string {
	switch m {
	case ModeSystem:
		return "system"
	case ModeUser:
		return "user"
	default:
		return "unknown"
	}
}

// Options configures the installation process.
type Options struct {
	Mode             Mode
	Prefix           string // FHS prefix override (default "/" for system, "$HOME" for user)
	DataDir          string // data directory override
	User             string // system user name (default "lynxdb")
	Group            string // system group name (default "lynxdb")
	Yes              bool   // skip all confirmation prompts
	SkipService      bool   // skip systemd/launchd setup
	SkipConfig       bool   // skip config file creation
	SkipCapabilities bool   // skip Linux capabilities (setcap)
	SkipUlimits      bool   // skip ulimits configuration
	SkipSelfTest     bool   // skip post-install verification
}

// DefaultOptions returns Options with sensible defaults.
// Mode is auto-detected based on effective UID.
func DefaultOptions() Options {
	mode := ModeUser
	if os.Geteuid() == 0 {
		mode = ModeSystem
	}

	return Options{
		Mode:  mode,
		User:  "lynxdb",
		Group: "lynxdb",
	}
}

// Paths holds the resolved filesystem paths for the installation.
type Paths struct {
	Binary      string // e.g. /usr/local/bin/lynxdb
	ConfigDir   string // e.g. /etc/lynxdb
	ConfigFile  string // e.g. /etc/lynxdb/config.yaml
	DataDir     string // e.g. /var/lib/lynxdb
	LogDir      string // e.g. /var/log/lynxdb
	RunDir      string // e.g. /run/lynxdb (systemd RuntimeDirectory)
	ServiceFile string // e.g. /etc/systemd/system/lynxdb.service or ~/Library/LaunchAgents/...
}

// Result captures what the installation actually did.
type Result struct {
	Paths            Paths
	Mode             Mode
	UserCreated      bool
	ServiceInstalled bool
	ConfigCreated    bool
	SelfTestPassed   bool
}

// ResolvePaths computes concrete filesystem paths from the given options.
// This is a pure function (no I/O) and is fully unit-testable.
func ResolvePaths(opts Options) Paths {
	if opts.Mode == ModeSystem {
		return resolveSystemPaths(opts)
	}

	return resolveUserPaths(opts)
}

func resolveSystemPaths(opts Options) Paths {
	prefix := opts.Prefix
	if prefix == "" {
		prefix = "/"
	}

	dataDir := opts.DataDir
	if dataDir == "" {
		dataDir = filepath.Join(prefix, "var", "lib", "lynxdb")
	}

	serviceFile := filepath.Join(prefix, "etc", "systemd", "system", "lynxdb.service")
	if runtime.GOOS == "darwin" {
		serviceFile = "/Library/LaunchDaemons/org.lynxdb.lynxdb.plist"
	}

	return Paths{
		Binary:      filepath.Join(prefix, "usr", "local", "bin", "lynxdb"),
		ConfigDir:   filepath.Join(prefix, "etc", "lynxdb"),
		ConfigFile:  filepath.Join(prefix, "etc", "lynxdb", "config.yaml"),
		DataDir:     dataDir,
		LogDir:      filepath.Join(prefix, "var", "log", "lynxdb"),
		RunDir:      filepath.Join(prefix, "run", "lynxdb"),
		ServiceFile: serviceFile,
	}
}

func resolveUserPaths(opts Options) Paths {
	home, _ := os.UserHomeDir()
	if home == "" {
		home = "."
	}

	prefix := opts.Prefix
	if prefix == "" {
		prefix = home
	}

	binDir := filepath.Join(prefix, ".local", "bin")
	configDir := filepath.Join(prefix, ".config", "lynxdb")
	if xdg := os.Getenv("XDG_CONFIG_HOME"); xdg != "" {
		configDir = filepath.Join(xdg, "lynxdb")
	}

	dataDir := opts.DataDir
	if dataDir == "" {
		dataDir = filepath.Join(prefix, ".local", "share", "lynxdb")
		if xdg := os.Getenv("XDG_DATA_HOME"); xdg != "" {
			dataDir = filepath.Join(xdg, "lynxdb")
		}
	}

	logDir := filepath.Join(dataDir, "log")

	serviceFile := ""
	if runtime.GOOS == "darwin" {
		serviceFile = filepath.Join(home, "Library", "LaunchAgents", "org.lynxdb.lynxdb.plist")
	}

	return Paths{
		Binary:      filepath.Join(binDir, "lynxdb"),
		ConfigDir:   configDir,
		ConfigFile:  filepath.Join(configDir, "config.yaml"),
		DataDir:     dataDir,
		LogDir:      logDir,
		ServiceFile: serviceFile,
	}
}

// StepFunc is a function that performs one installation step.
// It returns a human-readable detail string on success.
type StepFunc func() (detail string, err error)

// Step describes a single installation step for progress reporting.
type Step struct {
	Name string
	Fn   StepFunc
}

// BuildSteps returns the ordered list of installation steps for the given options.
// The caller is responsible for executing them and rendering progress.
func BuildSteps(opts Options) ([]Step, Paths) {
	paths := ResolvePaths(opts)

	var steps []Step

	// 1. Install binary
	steps = append(steps, Step{
		Name: "Binary",
		Fn: func() (string, error) {
			return installBinary(paths.Binary)
		},
	})

	// 2. Create system user (system mode, Linux/FreeBSD only)
	if opts.Mode == ModeSystem && runtime.GOOS != "darwin" {
		steps = append(steps, Step{
			Name: "System user",
			Fn: func() (string, error) {
				return ensureSystemUser(opts.User, opts.Group, paths.DataDir)
			},
		})
	}

	// 3. Create directories
	steps = append(steps, Step{
		Name: "Directories",
		Fn: func() (string, error) {
			return createDirectories(opts, paths)
		},
	})

	// 4. Write config file
	if !opts.SkipConfig {
		steps = append(steps, Step{
			Name: "Config",
			Fn: func() (string, error) {
				return writeConfigFile(opts, paths)
			},
		})
	}

	// 5. Configure ulimits (system mode, Linux only)
	if opts.Mode == ModeSystem && !opts.SkipUlimits {
		steps = append(steps, Step{
			Name: "File limits",
			Fn: func() (string, error) {
				return configureUlimits(opts)
			},
		})
	}

	// 6. Set capabilities (system mode, Linux only)
	if opts.Mode == ModeSystem && !opts.SkipCapabilities {
		steps = append(steps, Step{
			Name: "Capabilities",
			Fn: func() (string, error) {
				return setCapabilities(paths.Binary)
			},
		})
	}

	// 7. Install service
	if !opts.SkipService {
		steps = append(steps, Step{
			Name: "Service",
			Fn: func() (string, error) {
				return installService(opts, paths)
			},
		})
	}

	// 8. Self-test
	if !opts.SkipSelfTest {
		steps = append(steps, Step{
			Name: "Self-test",
			Fn: func() (string, error) {
				return runSelfTest(paths.Binary)
			},
		})
	}

	return steps, paths
}

// installBinary copies the currently running executable to the target path
// using an atomic rename pattern. Skips if the binary at the target path
// is identical (same size + SHA-256).
func installBinary(target string) (string, error) {
	src, err := os.Executable()
	if err != nil {
		return "", fmt.Errorf("install.installBinary: resolve executable: %w", err)
	}

	src, err = filepath.EvalSymlinks(src)
	if err != nil {
		return "", fmt.Errorf("install.installBinary: resolve symlinks: %w", err)
	}

	// Check if target already exists and is identical.
	if sameFile(src, target) {
		return fmt.Sprintf("%s (already installed)", target), nil
	}

	// Ensure target directory exists.
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		return "", fmt.Errorf("install.installBinary: create directory: %w", err)
	}

	// Atomic copy: write to .tmp.<pid>, then rename.
	tmpPath := fmt.Sprintf("%s.tmp.%d", target, os.Getpid())

	if err := copyFile(src, tmpPath); err != nil {
		os.Remove(tmpPath) //nolint:errcheck
		return "", fmt.Errorf("install.installBinary: copy: %w", err)
	}

	if err := os.Chmod(tmpPath, 0o755); err != nil {
		os.Remove(tmpPath) //nolint:errcheck
		return "", fmt.Errorf("install.installBinary: chmod: %w", err)
	}

	if err := os.Rename(tmpPath, target); err != nil {
		os.Remove(tmpPath) //nolint:errcheck
		return "", fmt.Errorf("install.installBinary: rename: %w", err)
	}

	return target, nil
}

// sameFile reports whether src and dst are identical files
// (same size and SHA-256 hash).
func sameFile(src, dst string) bool {
	srcInfo, err := os.Stat(src)
	if err != nil {
		return false
	}

	dstInfo, err := os.Stat(dst)
	if err != nil {
		return false
	}

	if srcInfo.Size() != dstInfo.Size() {
		return false
	}

	srcHash, err := fileHash(src)
	if err != nil {
		return false
	}

	dstHash, err := fileHash(dst)
	if err != nil {
		return false
	}

	return srcHash == dstHash
}

func fileHash(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}

	return fmt.Sprintf("%x", h.Sum(nil)), nil
}

// copyFile copies src to dst, creating dst with 0644 permissions.
func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	defer out.Close()

	if _, err := io.Copy(out, in); err != nil {
		return err
	}

	return out.Close()
}

// writeConfigFile creates a production-ready config file if it does not
// already exist. Returns the path and a detail string.
func writeConfigFile(opts Options, paths Paths) (string, error) {
	// Don't overwrite existing config.
	if _, err := os.Stat(paths.ConfigFile); err == nil {
		return fmt.Sprintf("%s (already exists)", paths.ConfigFile), nil
	}

	// Ensure config directory exists.
	if err := os.MkdirAll(paths.ConfigDir, 0o755); err != nil {
		return "", fmt.Errorf("install.writeConfigFile: create config dir: %w", err)
	}

	listen := "0.0.0.0:3100"
	if opts.Mode == ModeUser {
		listen = "localhost:3100"
	}

	var buf strings.Builder
	buf.WriteString("# LynxDB configuration\n")
	buf.WriteString("# Generated by 'lynxdb install'. See 'lynxdb config' for all settings.\n\n")
	fmt.Fprintf(&buf, "listen: %s\n", listen)
	fmt.Fprintf(&buf, "data_dir: %s\n", paths.DataDir)
	buf.WriteString("retention: 7d\n")

	perm := os.FileMode(0o644)
	if opts.Mode == ModeSystem {
		// Readable by the lynxdb group but not world-readable.
		perm = 0o640
	}

	if err := os.WriteFile(paths.ConfigFile, []byte(buf.String()), perm); err != nil {
		return "", fmt.Errorf("install.writeConfigFile: write: %w", err)
	}

	// If system mode, chown config to root:<group> so the service can read it.
	if opts.Mode == ModeSystem && runtime.GOOS != "windows" {
		chownByName(paths.ConfigFile, "root", opts.Group)
	}

	return paths.ConfigFile, nil
}

// installService dispatches to the appropriate service installer
// based on OS and mode.
func installService(opts Options, paths Paths) (string, error) {
	switch runtime.GOOS {
	case "linux":
		return installSystemdService(opts, paths)
	case "darwin":
		return installLaunchdService(opts, paths)
	default:
		return "(skipped, unsupported OS)", nil
	}
}
