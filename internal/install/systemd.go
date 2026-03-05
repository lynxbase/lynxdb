package install

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"text/template"
)

// systemdTemplate is the hardened systemd service unit for LynxDB.
// Template variables: .BinaryPath, .ConfigPath, .User, .Group, .DataDir.
const systemdTemplate = `[Unit]
Description=LynxDB - open-source log analytics engine
Documentation=https://docs.lynxdb.org/
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User={{.User}}
Group={{.Group}}
ExecStart={{.BinaryPath}} server --config {{.ConfigPath}}
Restart=always
RestartSec=10
TimeoutStartSec=0
TimeoutStopSec=90

# Directories (systemd creates and manages these)
RuntimeDirectory=lynxdb
RuntimeDirectoryMode=0755
StateDirectory=lynxdb
StateDirectoryMode=0750
LogsDirectory=lynxdb
LogsDirectoryMode=0750

# Resource limits
LimitNOFILE=1048576
LimitNPROC=65536
LimitMEMLOCK=infinity
LimitCORE=infinity

# Capabilities
CapabilityBoundingSet=CAP_NET_BIND_SERVICE
AmbientCapabilities=CAP_NET_BIND_SERVICE

# Security hardening
NoNewPrivileges=yes
PrivateTmp=yes
PrivateDevices=yes
ProtectSystem=strict
ProtectHome=yes
ProtectKernelModules=yes
ProtectKernelTunables=yes
ProtectControlGroups=yes
ReadWritePaths={{.DataDir}} /var/log/lynxdb /run/lynxdb
RestrictAddressFamilies=AF_INET AF_INET6 AF_UNIX
RestrictNamespaces=yes
RestrictRealtime=yes
RestrictSUIDSGID=yes
SystemCallArchitectures=native
SystemCallFilter=@system-service

# Logging
StandardOutput=journal
StandardError=journal
SyslogIdentifier=lynxdb

[Install]
WantedBy=multi-user.target
`

type systemdTemplateData struct {
	BinaryPath string
	ConfigPath string
	User       string
	Group      string
	DataDir    string
}

// renderSystemdUnit renders the systemd service unit file content.
func renderSystemdUnit(opts Options, paths Paths) ([]byte, error) {
	tmpl, err := template.New("systemd").Parse(systemdTemplate)
	if err != nil {
		return nil, fmt.Errorf("install.renderSystemdUnit: parse template: %w", err)
	}

	data := systemdTemplateData{
		BinaryPath: paths.Binary,
		ConfigPath: paths.ConfigFile,
		User:       opts.User,
		Group:      opts.Group,
		DataDir:    paths.DataDir,
	}

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, data); err != nil {
		return nil, fmt.Errorf("install.renderSystemdUnit: execute template: %w", err)
	}

	return buf.Bytes(), nil
}

// installSystemdService writes the systemd service file and runs daemon-reload.
// Does NOT enable or start the service — that's left to the user.
func installSystemdService(opts Options, paths Paths) (string, error) {
	if runtime.GOOS != "linux" {
		return "(skipped, not Linux)", nil
	}

	content, err := renderSystemdUnit(opts, paths)
	if err != nil {
		return "", err
	}

	serviceDir := filepath.Dir(paths.ServiceFile)
	if err := os.MkdirAll(serviceDir, 0o755); err != nil {
		return "", fmt.Errorf("install.installSystemdService: create dir: %w", err)
	}

	if err := os.WriteFile(paths.ServiceFile, content, 0o644); err != nil {
		return "", fmt.Errorf("install.installSystemdService: write: %w", err)
	}

	// Run systemctl daemon-reload so systemd picks up the new unit.
	if _, err := exec.LookPath("systemctl"); err == nil {
		cmd := exec.Command("systemctl", "daemon-reload")
		if out, err := cmd.CombinedOutput(); err != nil {
			return "", fmt.Errorf("install.installSystemdService: daemon-reload: %s: %w", string(out), err)
		}
	}

	return paths.ServiceFile, nil
}
