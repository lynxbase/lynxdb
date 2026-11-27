---
sidebar_position: 2
title: Install LynxDB
sidebar_label: Installation
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

The script detects the architecture (amd64, arm64, armv7), downloads the
matching release archive, checks it against the release's `SHA256SUMS` with
`sha256sum` or `shasum`, and installs to `/usr/local/bin`, or to
`~/.local/bin` when `/usr/local/bin` is not writable. The binary is static,
so the same archive runs on glibc and musl systems.

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
[GitHub Releases](https://github.com/lynxbase/lynxdb/releases), or build the
tip of `main` with Go:

```powershell
go install github.com/lynxbase/lynxdb/cmd/lynxdb@main
```

The install script also runs under Git Bash and MSYS2.

</TabItem>
</Tabs>

### Install script options

Pass options after `sh -s --`, or set the matching environment variable:

| Option | Variable | Default | Description |
|--------|----------|---------|-------------|
| `--version <v>` | `LYNXDB_VERSION` | newest of the channel | Install a specific version, such as `26.11.1.80` |
| `--channel <c>` | `LYNXDB_CHANNEL` | `stable` | Release channel: `stable` or `lts` |
| `--dir <path>` | `LYNXDB_INSTALL_DIR` | auto | Installation directory |
| `--no-modify-path` | `LYNXDB_NO_MODIFY_PATH` | unset | Do not add the directory to `PATH` |
| `--force` | `LYNXDB_FORCE` | unset | Skip confirmation prompts |
| | `LYNXDB_BASE_URL` | `https://dl.lynxdb.org` | Download from a mirror of the download site |

```bash
# A specific version
curl -fsSL https://lynxdb.org/install.sh | sh -s -- --version 26.11.1.80

# A custom directory
curl -fsSL https://lynxdb.org/install.sh | LYNXDB_INSTALL_DIR=~/bin sh
```

### Versions and channels

A version reads `year.month.patch.build`: `26.11.1.80` is the first release
of the line cut in November 2026. The `stable` channel follows the newest
release. The `lts` channel follows the newest release of a line that keeps
receiving patches.

A release candidate is published as a pre-release on
[GitHub Releases](https://github.com/lynxbase/lynxdb/releases) and belongs to
no channel. Install one by its version:

```bash
curl -fsSL https://lynxdb.org/install.sh | sh -s -- --version 26.12.1.45
```

To return to the stable channel, run the plain install script again.

Homebrew also keeps one formula per line, for staying on it:

```bash
brew install lynxbase/tap/lynxdb@26.11
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

`latest` follows the newest release. A version tag such as `26.11.1.80`
names one release, and a line tag such as `26.11` follows the patches of that
line:

```bash
docker pull ghcr.io/lynxbase/lynxdb:26.11
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
# The tip of main
go install github.com/lynxbase/lynxdb/cmd/lynxdb@main

# Or clone and build
git clone https://github.com/lynxbase/lynxdb.git
cd lynxdb
go build -o lynxdb ./cmd/lynxdb/
```

Release tags are not Go module versions, so `@latest` does not name the
newest release. A binary built this way reports the version `dev`;
`mise run build` in a clone stamps it with the version the tags give.

## Verify the installation

```bash
lynxdb version
```

```text
  Version        26.11.1.80
  Commit         abc1234def56
  Built          2026-11-24T10:00:00Z
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

| OS | Architecture | Archive |
|----|-------------|---------|
| Linux | x86_64 (amd64) | `linux-amd64.tar.gz` |
| Linux | arm64 | `linux-arm64.tar.gz` |
| Linux | armv7 | `linux-armv7.tar.gz` |
| macOS | Intel (amd64) | `darwin-amd64.tar.gz` |
| macOS | Apple Silicon (arm64) | `darwin-arm64.tar.gz` |
| Windows | x86_64 (amd64) | `windows-amd64.zip` |

The Linux binaries are static and run on glibc and musl systems alike.

## Next steps

- [Quick Start](./quickstart.md): run a first query
- [Pipe Mode](./pipe-mode.md): query files without a server
- [Server Mode](./server-mode.md): start a persistent server
