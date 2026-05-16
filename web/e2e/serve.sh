#!/usr/bin/env bash
# Builds the web UI, embeds it into a throwaway lynxdb binary, and serves it
# in-memory so Playwright drives the real shipped artifact.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

make webui
go build -o "$ROOT/.e2e-lynxdb" ./cmd/lynxdb/

exec "$ROOT/.e2e-lynxdb" server --addr 127.0.0.1:3100
