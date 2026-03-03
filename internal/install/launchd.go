package install

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"text/template"
)

// launchdTemplate is the macOS launchd plist for LynxDB.
// Template variables: .BinaryPath, .ConfigPath, .DataDir, .LogDir.
const launchdTemplate = `<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
  "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>org.lynxdb.lynxdb</string>
    <key>ProgramArguments</key>
    <array>
        <string>{{.BinaryPath}}</string>
        <string>server</string>
        <string>--config</string>
        <string>{{.ConfigPath}}</string>
    </array>
    <key>RunAtLoad</key>
    <false/>
    <key>KeepAlive</key>
    <true/>
    <key>WorkingDirectory</key>
    <string>{{.DataDir}}</string>
    <key>StandardOutPath</key>
    <string>{{.LogDir}}/lynxdb.log</string>
    <key>StandardErrorPath</key>
    <string>{{.LogDir}}/lynxdb.err.log</string>
    <key>SoftResourceLimits</key>
    <dict>
        <key>NumberOfFiles</key>
        <integer>1048576</integer>
    </dict>
</dict>
</plist>
`

type launchdTemplateData struct {
	BinaryPath string
	ConfigPath string
	DataDir    string
	LogDir     string
}

// renderLaunchdPlist renders the macOS launchd plist file content.
func renderLaunchdPlist(paths Paths) ([]byte, error) {
	tmpl, err := template.New("launchd").Parse(launchdTemplate)
	if err != nil {
		return nil, fmt.Errorf("install.renderLaunchdPlist: parse template: %w", err)
	}

	data := launchdTemplateData{
		BinaryPath: paths.Binary,
		ConfigPath: paths.ConfigFile,
		DataDir:    paths.DataDir,
		LogDir:     paths.LogDir,
	}

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, data); err != nil {
		return nil, fmt.Errorf("install.renderLaunchdPlist: execute template: %w", err)
	}

	return buf.Bytes(), nil
}

// installLaunchdService writes the launchd plist file.
// Does NOT load the agent — that's left to the user.
func installLaunchdService(_ Options, paths Paths) (string, error) {
	if runtime.GOOS != "darwin" {
		return "(skipped, not macOS)", nil
	}

	if paths.ServiceFile == "" {
		return "(skipped, no service file path)", nil
	}

	content, err := renderLaunchdPlist(paths)
	if err != nil {
		return "", err
	}

	// Ensure the parent directory exists.
	plistDir := filepath.Dir(paths.ServiceFile)
	if err := os.MkdirAll(plistDir, 0o755); err != nil {
		return "", fmt.Errorf("install.installLaunchdService: create dir: %w", err)
	}

	if err := os.WriteFile(paths.ServiceFile, content, 0o644); err != nil {
		return "", fmt.Errorf("install.installLaunchdService: write: %w", err)
	}

	return paths.ServiceFile, nil
}
