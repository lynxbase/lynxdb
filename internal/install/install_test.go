package install

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestResolvePathsSystem(t *testing.T) {
	paths := ResolvePaths(Options{Mode: ModeSystem})

	if paths.Binary != "/usr/local/bin/lynxdb" {
		t.Errorf("Binary = %q, want /usr/local/bin/lynxdb", paths.Binary)
	}

	if paths.ConfigDir != "/etc/lynxdb" {
		t.Errorf("ConfigDir = %q, want /etc/lynxdb", paths.ConfigDir)
	}

	if paths.ConfigFile != "/etc/lynxdb/config.yaml" {
		t.Errorf("ConfigFile = %q, want /etc/lynxdb/config.yaml", paths.ConfigFile)
	}

	if paths.DataDir != "/var/lib/lynxdb" {
		t.Errorf("DataDir = %q, want /var/lib/lynxdb", paths.DataDir)
	}

	if paths.LogDir != "/var/log/lynxdb" {
		t.Errorf("LogDir = %q, want /var/log/lynxdb", paths.LogDir)
	}
}

func TestResolvePathsSystemWithPrefix(t *testing.T) {
	paths := ResolvePaths(Options{Mode: ModeSystem, Prefix: "/opt"})

	if paths.Binary != "/opt/usr/local/bin/lynxdb" {
		t.Errorf("Binary = %q, want /opt/usr/local/bin/lynxdb", paths.Binary)
	}

	if paths.DataDir != "/opt/var/lib/lynxdb" {
		t.Errorf("DataDir = %q, want /opt/var/lib/lynxdb", paths.DataDir)
	}
}

func TestResolvePathsSystemWithDataDir(t *testing.T) {
	paths := ResolvePaths(Options{Mode: ModeSystem, DataDir: "/data/lynxdb"})

	if paths.DataDir != "/data/lynxdb" {
		t.Errorf("DataDir = %q, want /data/lynxdb", paths.DataDir)
	}

	// Other paths should use default prefix.
	if paths.Binary != "/usr/local/bin/lynxdb" {
		t.Errorf("Binary = %q, want /usr/local/bin/lynxdb", paths.Binary)
	}
}

func TestResolvePathsUser(t *testing.T) {
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatal("test requires home directory")
	}

	// Clear XDG vars for predictable test.
	t.Setenv("XDG_CONFIG_HOME", "")
	t.Setenv("XDG_DATA_HOME", "")

	paths := ResolvePaths(Options{Mode: ModeUser})

	expectedBin := filepath.Join(home, ".local", "bin", "lynxdb")
	if paths.Binary != expectedBin {
		t.Errorf("Binary = %q, want %q", paths.Binary, expectedBin)
	}

	expectedConfig := filepath.Join(home, ".config", "lynxdb", "config.yaml")
	if paths.ConfigFile != expectedConfig {
		t.Errorf("ConfigFile = %q, want %q", paths.ConfigFile, expectedConfig)
	}

	expectedData := filepath.Join(home, ".local", "share", "lynxdb")
	if paths.DataDir != expectedData {
		t.Errorf("DataDir = %q, want %q", paths.DataDir, expectedData)
	}
}

func TestResolvePathsUserWithXDG(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", "/tmp/xdg-config")
	t.Setenv("XDG_DATA_HOME", "/tmp/xdg-data")

	paths := ResolvePaths(Options{Mode: ModeUser})

	if paths.ConfigDir != "/tmp/xdg-config/lynxdb" {
		t.Errorf("ConfigDir = %q, want /tmp/xdg-config/lynxdb", paths.ConfigDir)
	}

	if paths.DataDir != "/tmp/xdg-data/lynxdb" {
		t.Errorf("DataDir = %q, want /tmp/xdg-data/lynxdb", paths.DataDir)
	}
}

func TestResolvePathsUserServiceFile(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", "")
	t.Setenv("XDG_DATA_HOME", "")

	paths := ResolvePaths(Options{Mode: ModeUser})

	switch runtime.GOOS {
	case "darwin":
		if !strings.Contains(paths.ServiceFile, "LaunchAgents") {
			t.Errorf("ServiceFile = %q, want to contain LaunchAgents", paths.ServiceFile)
		}
	default:
		// On Linux, user-mode service file is empty (systemd --user is optional).
		if paths.ServiceFile != "" {
			t.Errorf("ServiceFile = %q, want empty on %s", paths.ServiceFile, runtime.GOOS)
		}
	}
}

func TestModeString(t *testing.T) {
	tests := []struct {
		mode Mode
		want string
	}{
		{ModeSystem, "system"},
		{ModeUser, "user"},
		{Mode(99), "unknown"},
	}

	for _, tt := range tests {
		if got := tt.mode.String(); got != tt.want {
			t.Errorf("Mode(%d).String() = %q, want %q", tt.mode, got, tt.want)
		}
	}
}

func TestDefaultOptions(t *testing.T) {
	opts := DefaultOptions()

	if opts.User != "lynxdb" {
		t.Errorf("User = %q, want lynxdb", opts.User)
	}

	if opts.Group != "lynxdb" {
		t.Errorf("Group = %q, want lynxdb", opts.Group)
	}

	// Mode depends on euid; just verify it's one of the valid values.
	if opts.Mode != ModeSystem && opts.Mode != ModeUser {
		t.Errorf("Mode = %d, want ModeSystem or ModeUser", opts.Mode)
	}
}

func TestBuildSteps(t *testing.T) {
	opts := Options{
		Mode:  ModeUser,
		User:  "lynxdb",
		Group: "lynxdb",
	}

	steps, paths := BuildSteps(opts)

	if paths.Binary == "" {
		t.Error("paths.Binary is empty")
	}

	if len(steps) == 0 {
		t.Fatal("BuildSteps returned no steps")
	}

	// First step should always be Binary.
	if steps[0].Name != "Binary" {
		t.Errorf("first step = %q, want Binary", steps[0].Name)
	}

	// Should not have "System user" step in user mode.
	for _, s := range steps {
		if s.Name == "System user" {
			t.Error("user mode should not include System user step")
		}
	}
}

func TestBuildStepsSkipOptions(t *testing.T) {
	opts := Options{
		Mode:             ModeUser,
		User:             "lynxdb",
		Group:            "lynxdb",
		SkipService:      true,
		SkipConfig:       true,
		SkipCapabilities: true,
		SkipUlimits:      true,
		SkipSelfTest:     true,
	}

	steps, _ := BuildSteps(opts)

	for _, s := range steps {
		switch s.Name {
		case "Service", "Config", "Capabilities", "File limits", "Self-test":
			t.Errorf("step %q should be skipped", s.Name)
		}
	}
}

func TestRenderSystemdUnit(t *testing.T) {
	opts := Options{User: "lynxdb", Group: "lynxdb"}
	paths := Paths{
		Binary:     "/usr/local/bin/lynxdb",
		ConfigFile: "/etc/lynxdb/config.yaml",
		DataDir:    "/var/lib/lynxdb",
	}

	content, err := renderSystemdUnit(opts, paths)
	if err != nil {
		t.Fatalf("renderSystemdUnit: %v", err)
	}

	s := string(content)

	// Check key template substitutions.
	if !strings.Contains(s, "ExecStart=/usr/local/bin/lynxdb server --config /etc/lynxdb/config.yaml") {
		t.Error("ExecStart not rendered correctly")
	}

	if !strings.Contains(s, "User=lynxdb") {
		t.Error("User not rendered correctly")
	}

	if !strings.Contains(s, "Group=lynxdb") {
		t.Error("Group not rendered correctly")
	}

	if !strings.Contains(s, "ReadWritePaths=/var/lib/lynxdb") {
		t.Error("ReadWritePaths not rendered correctly")
	}

	// Check security hardening directives are present.
	for _, directive := range []string{
		"NoNewPrivileges=yes",
		"PrivateTmp=yes",
		"ProtectSystem=strict",
		"ProtectHome=yes",
		"LimitNOFILE=1048576",
	} {
		if !strings.Contains(s, directive) {
			t.Errorf("missing directive: %s", directive)
		}
	}
}

func TestRenderLaunchdPlist(t *testing.T) {
	paths := Paths{
		Binary:     "/usr/local/bin/lynxdb",
		ConfigFile: "/etc/lynxdb/config.yaml",
		DataDir:    "/var/lib/lynxdb",
		LogDir:     "/var/log/lynxdb",
	}

	content, err := renderLaunchdPlist(paths)
	if err != nil {
		t.Fatalf("renderLaunchdPlist: %v", err)
	}

	s := string(content)

	if !strings.Contains(s, "<string>/usr/local/bin/lynxdb</string>") {
		t.Error("BinaryPath not rendered correctly")
	}

	if !strings.Contains(s, "<string>/etc/lynxdb/config.yaml</string>") {
		t.Error("ConfigPath not rendered correctly")
	}

	if !strings.Contains(s, "org.lynxdb.lynxdb") {
		t.Error("Label not present")
	}

	if !strings.Contains(s, "<integer>1048576</integer>") {
		t.Error("NumberOfFiles limit not present")
	}
}

func TestWriteConfigFile(t *testing.T) {
	tmpDir := t.TempDir()

	paths := Paths{
		ConfigDir:  tmpDir,
		ConfigFile: filepath.Join(tmpDir, "config.yaml"),
		DataDir:    "/var/lib/lynxdb",
	}

	// System mode.
	detail, err := writeConfigFile(Options{Mode: ModeSystem}, paths)
	if err != nil {
		t.Fatalf("writeConfigFile: %v", err)
	}

	if detail != paths.ConfigFile {
		t.Errorf("detail = %q, want %q", detail, paths.ConfigFile)
	}

	content, err := os.ReadFile(paths.ConfigFile)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}

	s := string(content)
	if !strings.Contains(s, "listen: 0.0.0.0:3100") {
		t.Error("system mode should bind to 0.0.0.0:3100")
	}

	if !strings.Contains(s, "data_dir: /var/lib/lynxdb") {
		t.Error("data_dir not written")
	}

	// Second call should skip (already exists).
	detail2, err := writeConfigFile(Options{Mode: ModeSystem}, paths)
	if err != nil {
		t.Fatalf("writeConfigFile (second): %v", err)
	}

	if !strings.Contains(detail2, "already exists") {
		t.Errorf("second call detail = %q, want 'already exists'", detail2)
	}
}

func TestWriteConfigFileUserMode(t *testing.T) {
	tmpDir := t.TempDir()

	paths := Paths{
		ConfigDir:  tmpDir,
		ConfigFile: filepath.Join(tmpDir, "config.yaml"),
		DataDir:    "/home/user/.local/share/lynxdb",
	}

	detail, err := writeConfigFile(Options{Mode: ModeUser}, paths)
	if err != nil {
		t.Fatalf("writeConfigFile: %v", err)
	}

	if detail != paths.ConfigFile {
		t.Errorf("detail = %q, want %q", detail, paths.ConfigFile)
	}

	content, err := os.ReadFile(paths.ConfigFile)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}

	if !strings.Contains(string(content), "listen: localhost:3100") {
		t.Error("user mode should bind to localhost:3100")
	}
}

func TestSameFileIdentical(t *testing.T) {
	tmpDir := t.TempDir()
	f := filepath.Join(tmpDir, "a")

	if err := os.WriteFile(f, []byte("hello"), 0o644); err != nil {
		t.Fatal(err)
	}

	// Same file compared to itself should be true.
	if !sameFile(f, f) {
		t.Error("sameFile(f, f) = false, want true")
	}
}

func TestSameFileDifferent(t *testing.T) {
	tmpDir := t.TempDir()
	a := filepath.Join(tmpDir, "a")
	b := filepath.Join(tmpDir, "b")

	if err := os.WriteFile(a, []byte("hello"), 0o644); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(b, []byte("world"), 0o644); err != nil {
		t.Fatal(err)
	}

	if sameFile(a, b) {
		t.Error("sameFile(a, b) = true, want false")
	}
}

func TestSameFileNonexistent(t *testing.T) {
	tmpDir := t.TempDir()
	a := filepath.Join(tmpDir, "a")

	if err := os.WriteFile(a, []byte("hello"), 0o644); err != nil {
		t.Fatal(err)
	}

	if sameFile(a, filepath.Join(tmpDir, "nonexistent")) {
		t.Error("sameFile with nonexistent = true, want false")
	}
}

func TestCreateDirectories(t *testing.T) {
	tmpDir := t.TempDir()

	paths := Paths{
		ConfigDir: filepath.Join(tmpDir, "config"),
		DataDir:   filepath.Join(tmpDir, "data"),
		LogDir:    filepath.Join(tmpDir, "log"),
	}

	detail, err := createDirectories(Options{Mode: ModeUser}, paths)
	if err != nil {
		t.Fatalf("createDirectories: %v", err)
	}

	if detail == "" {
		t.Error("detail is empty")
	}

	for _, dir := range []string{paths.ConfigDir, paths.DataDir, paths.LogDir} {
		info, err := os.Stat(dir)
		if err != nil {
			t.Errorf("directory %s not created: %v", dir, err)
			continue
		}

		if !info.IsDir() {
			t.Errorf("%s is not a directory", dir)
		}
	}
}

func TestDetectInstallMode(t *testing.T) {
	// Should not panic regardless of environment.
	mode, paths := DetectInstallMode()

	if mode != ModeSystem && mode != ModeUser {
		t.Errorf("mode = %d, want ModeSystem or ModeUser", mode)
	}

	if paths.Binary == "" {
		t.Error("Binary path is empty")
	}
}
