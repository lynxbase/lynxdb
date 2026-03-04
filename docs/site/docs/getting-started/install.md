---
sidebar_position: 2
title: Installation
description: Install LynxDB on Linux, macOS, Windows, or Docker.
---

# Installation

LynxDB ships as a single static binary with zero runtime dependencies. Choose your preferred installation method.

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

## Pre-built Binary

<Tabs>
<TabItem value="linux" label="Linux" default>

```bash
curl -fsSL https://lynxdb.org/install.sh | sh
```

The install script auto-detects your architecture (amd64/arm64) and libc (glibc/musl for Alpine). It verifies SHA256 checksums and installs to `/usr/local/bin` (or `~/.local/bin` if not root).

</TabItem>
<TabItem value="macos" label="macOS">

```bash
# Install script (works on Intel and Apple Silicon)
curl -fsSL https://lynxdb.org/install.sh | sh

# Or via Homebrew
brew install lynxbase/tap/lynxdb
```

</TabItem>
<TabItem value="windows" label="Windows">

```powershell
# Download from GitHub Releases
# https://github.com/lynxbase/lynxdb/releases

# Or use Go
go install github.com/lynxbase/lynxdb/cmd/lynxdb@latest
```

On Windows, LynxDB works best under WSL2 or Git Bash.

</TabItem>
</Tabs>

### Install Script Options

The install script supports customization via environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `LYNXDB_VERSION` | latest | Install a specific version |
| `LYNXDB_INSTALL_DIR` | auto-detect | Custom installation directory |
| `LYNXDB_NO_MODIFY_PATH` | unset | Skip PATH modification |
| `LYNXDB_FORCE` | unset | Skip confirmation prompts |

```bash
# Install a specific version
LYNXDB_VERSION=v0.5.0 curl -fsSL https://lynxdb.org/install.sh | sh

# Install to a custom directory
LYNXDB_INSTALL_DIR=/opt/bin curl -fsSL https://lynxdb.org/install.sh | sh
```

## Docker

```bash
# Run server mode
docker run -d --name lynxdb \
  -p 3100:3100 \
  -v lynxdb-data:/data \
  ghcr.io/lynxbase/lynxdb server --data-dir /data

# Run a one-off query
echo '{"level":"error","msg":"test"}' | \
  docker run -i ghcr.io/lynxbase/lynxdb query '| stats count by level'
```

### Docker Compose

```yaml
services:
  lynxdb:
    image: ghcr.io/lynxbase/lynxdb:latest
    command: server --data-dir /data
    ports:
      - "3100:3100"
    volumes:
      - lynxdb-data:/data

volumes:
  lynxdb-data:
```

## From Source

Requires Go 1.25+:

```bash
# Via go install
go install github.com/lynxbase/lynxdb/cmd/lynxdb@latest

# Or clone and build
git clone https://github.com/lynxbase/lynxdb.git
cd lynxdb
go build -o lynxdb ./cmd/lynxdb/
```

## Verify Installation

```bash
lynxdb version
# LynxDB v0.5.0 (abc1234) built 2026-03-01T10:00:00Z
# Go: go1.25.4 darwin/arm64
```

## Set Up as a Service

After installing the binary, set up LynxDB as a production service with a single command:

```bash
# System-wide (creates user, dirs, config, hardened systemd service)
sudo lynxdb install

# User-local (no root required, uses ~/.local and user systemd/launchd)
lynxdb install
```

This replaces the manual steps of creating a dedicated user, directories, config file, and systemd/launchd unit. See the full [`install` reference](/docs/cli/install) for all flags and details.

## Self-Update

LynxDB can update itself:

```bash
# Check for updates
lynxdb upgrade --check

# Upgrade to latest
lynxdb upgrade

# Upgrade to a specific version
lynxdb upgrade --version v0.6.0
```

## Shell Completion

Enable tab completion for your shell:

```bash
# Bash
lynxdb completion bash >> ~/.bashrc

# Zsh
lynxdb completion zsh >> ~/.zshrc

# Fish
lynxdb completion fish > ~/.config/fish/completions/lynxdb.fish
```

## Supported Platforms

| OS | Architecture | Notes |
|----|-------------|-------|
| Linux | x86_64 (amd64) | glibc and musl (Alpine) |
| Linux | ARM64 | glibc and musl |
| Linux | ARMv7 | Raspberry Pi 3+ |
| macOS | Intel (amd64) | macOS 12+ |
| macOS | Apple Silicon (arm64) | M1/M2/M3/M4 |
| Windows | x86_64 | Best with WSL2 |

All binaries are statically linked (`CGO_ENABLED=0`) with zero runtime dependencies.

## Next Steps

- **[Quick Start](/docs/getting-started/quickstart)** -- Run your first query
- **[Pipe Mode](/docs/getting-started/pipe-mode)** -- Query without a server
- **[Server Mode](/docs/getting-started/server-mode)** -- Start a persistent server
