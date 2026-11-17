---
sidebar_position: 2
title: Installation
description: Install LynxDB on Linux, macOS, Windows, or Docker.
---

# Installation

LynxDB ships as one statically linked binary (`CGO_ENABLED=0`) with no
runtime dependencies.

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

## Pre-built binary

<Tabs>
<TabItem value="linux" label="Linux" default>

```bash
curl -fsSL https://lynxdb.org/install.sh | sh
```

The script detects the architecture (amd64, arm64, armv7) and the C library
(glibc or musl), downloads the matching release archive, checks it against
the release's SHA-256 checksums when `sha256sum` or `shasum` is available,
and installs to `/usr/local/bin`, or to `~/.local/bin` when `/usr/local/bin`
is not writable.

</TabItem>
<TabItem value="macos" label="macOS">

```bash
# Install script (Intel and Apple Silicon)
curl -fsSL https://lynxdb.org/install.sh | sh

# Or Homebrew
brew install lynxbase/tap/lynxdb
```

</TabItem>
<TabItem value="windows" label="Windows">

Download the `windows-amd64` zip archive from
[GitHub Releases](https://github.com/lynxbase/lynxdb/releases), or build it
with Go:

```powershell
go install github.com/lynxbase/lynxdb/cmd/lynxdb@latest
```

The install script also runs under Git Bash and MSYS2.

</TabItem>
</Tabs>

### Install script options

Pass options after `sh -s --`, or set the matching environment variable:

| Option | Variable | Default | Description |
|--------|----------|---------|-------------|
| `--version <v>` | `LYNXDB_VERSION` | latest | Install a specific version, such as `v0.5.0` |
| `--channel <c>` | `LYNXDB_CHANNEL` | `stable` | Release channel: `stable` or `nightly` |
| `--allow-prerelease` | `LYNXDB_ALLOW_PRERELEASE` | unset | Required for nightly and prerelease versions |
| `--dir <path>` | `LYNXDB_INSTALL_DIR` | auto | Installation directory |
| `--no-modify-path` | `LYNXDB_NO_MODIFY_PATH` | unset | Do not add the directory to `PATH` |
| `--force` | `LYNXDB_FORCE` | unset | Skip confirmation prompts |
| | `LYNXDB_BASE_URL` | `https://dl.lynxdb.org` | Download from another base URL (GitHub Releases is the fallback) |

```bash
# A specific version
curl -fsSL https://lynxdb.org/install.sh | sh -s -- --version v0.5.0

# A custom directory
curl -fsSL https://lynxdb.org/install.sh | LYNXDB_INSTALL_DIR=~/bin sh
```

### Nightly builds

Nightly builds are prerelease builds of `main` for testing. They need an
explicit opt-in.

```bash
# Install script
curl -fsSL https://lynxdb.org/install.sh | sh -s -- --channel nightly --allow-prerelease

# Homebrew
brew install lynxbase/tap/lynxdb-nightly
```

To return to the stable channel, run the plain install script again, or
swap the Homebrew formula:

```bash
brew uninstall lynxdb-nightly
brew install lynxbase/tap/lynxdb
```

## Docker

Images are published to `ghcr.io/lynxbase/lynxdb` for `linux/amd64` and
`linux/arm64`. The image runs `lynxdb server` by default. Inside a container,
listen on `0.0.0.0` and keep the data directory on a volume:

```bash
docker run -d --name lynxdb \
  -p 3100:3100 \
  -v lynxdb-data:/data \
  ghcr.io/lynxbase/lynxdb server --addr 0.0.0.0:3100 --data-dir /data
```

Run a one-off query over standard input:

```bash
echo '{"level":"error","msg":"test"}' | \
  docker run -i --rm ghcr.io/lynxbase/lynxdb query 'source=main | stats count() by level'
```

The `nightly` tag follows the nightly builds:

```bash
docker pull ghcr.io/lynxbase/lynxdb:nightly
```

### Docker Compose

```yaml
services:
  lynxdb:
    image: ghcr.io/lynxbase/lynxdb:latest
    command: server --addr 0.0.0.0:3100 --data-dir /data
    ports:
      - "3100:3100"
    volumes:
      - lynxdb-data:/data

volumes:
  lynxdb-data:
```

See [Docker deployment](../deployment/docker.md) for more examples.

## From source

Requires Go 1.26 or later:

```bash
# go install
go install github.com/lynxbase/lynxdb/cmd/lynxdb@latest

# Or clone and build
git clone https://github.com/lynxbase/lynxdb.git
cd lynxdb
go build -o lynxdb ./cmd/lynxdb/
```

## Verify the installation

```bash
lynxdb version
```

```text
  Version        v0.5.0
  Commit         abc1234
  Built          2026-03-01T10:00:00Z
  Go             go1.26.3 linux/amd64
```

`lynxdb version --short` prints only the version; `--json` prints the same
information as JSON.

## Run as a service

The binary does not install a service unit. To run the server under systemd,
see [Server Mode](./server-mode.md#run-as-a-service) and
[Single-node deployment](../deployment/single-node.md).

## Shell completion

```bash
# Bash
lynxdb completion bash >> ~/.bashrc

# Zsh
lynxdb completion zsh >> ~/.zshrc

# Fish
lynxdb completion fish > ~/.config/fish/completions/lynxdb.fish
```

PowerShell is supported too: `lynxdb completion powershell`.

## Supported platforms

| OS | Architecture | Archives |
|----|-------------|----------|
| Linux | x86_64 (amd64) | glibc and musl |
| Linux | arm64 | glibc and musl |
| Linux | armv7 | glibc |
| macOS | Intel (amd64) | |
| macOS | Apple Silicon (arm64) | |
| Windows | x86_64 (amd64) | zip |

## Next steps

- [Quick Start](./quickstart.md): run a first query
- [Pipe Mode](./pipe-mode.md): query files without a server
- [Server Mode](./server-mode.md): start a persistent server
