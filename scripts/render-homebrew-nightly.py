#!/usr/bin/env python3
import argparse
import re
import sys
from pathlib import Path


ARCHIVE_PATTERN = re.compile(
    r"^lynxdb-v(?P<version>[0-9]+\.[0-9]+\.[0-9]+-nightly\.[0-9]{8}\.g[0-9a-fA-F]+)-"
    r"(?P<os>darwin|linux)-(?P<arch>amd64|arm64)\.tar\.gz$"
)

REQUIRED_PLATFORMS = (
    ("darwin", "amd64"),
    ("darwin", "arm64"),
    ("linux", "amd64"),
    ("linux", "arm64"),
)


def parse_checksums(path: Path) -> dict[str, str]:
    checksums = {}
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split()
        if len(parts) >= 2:
            checksums[parts[1].lstrip("*")] = parts[0]
    return checksums


def collect_artifacts(checksums: dict[str, str], version: str) -> dict[tuple[str, str], tuple[str, str]]:
    artifacts = {}
    for filename, sha256 in checksums.items():
        match = ARCHIVE_PATTERN.match(filename)
        if not match:
            continue
        if match.group("version") != version.lstrip("v"):
            continue
        key = (match.group("os"), match.group("arch"))
        artifacts[key] = (filename, sha256)

    missing = [f"{os_name}-{arch}" for os_name, arch in REQUIRED_PLATFORMS if (os_name, arch) not in artifacts]
    if missing:
        raise ValueError("missing required artifacts: " + ", ".join(missing))

    return artifacts


def url_for(base_url: str, version: str, filename: str) -> str:
    return f"{base_url.rstrip('/')}/{version}/{filename}"


def render_formula(version: str, base_url: str, artifacts: dict[tuple[str, str], tuple[str, str]]) -> str:
    version_no_v = version.lstrip("v")

    def entry(os_name: str, arch: str) -> tuple[str, str]:
        filename, sha256 = artifacts[(os_name, arch)]
        return url_for(base_url, version, filename), sha256

    darwin_amd64_url, darwin_amd64_sha = entry("darwin", "amd64")
    darwin_arm64_url, darwin_arm64_sha = entry("darwin", "arm64")
    linux_amd64_url, linux_amd64_sha = entry("linux", "amd64")
    linux_arm64_url, linux_arm64_sha = entry("linux", "arm64")

    return f'''class LynxdbNightly < Formula
  desc "Open-source log analytics database in a single binary, queried with PPL"
  homepage "https://lynxdb.org"
  version "{version_no_v}"
  license "Apache-2.0"

  conflicts_with "lynxdb", because: "both install bin/lynxdb"

  on_macos do
    if Hardware::CPU.arm?
      url "{darwin_arm64_url}"
      sha256 "{darwin_arm64_sha}"
    else
      url "{darwin_amd64_url}"
      sha256 "{darwin_amd64_sha}"
    end
  end

  on_linux do
    if Hardware::CPU.arm?
      url "{linux_arm64_url}"
      sha256 "{linux_arm64_sha}"
    else
      url "{linux_amd64_url}"
      sha256 "{linux_amd64_sha}"
    end
  end

  def install
    bin.install "lynxdb"
  end

  def caveats
    <<~EOS
      This is a nightly prerelease build. It may contain regressions.
    EOS
  end

  test do
    system bin/"lynxdb", "version"
  end
end
'''


def main() -> int:
    parser = argparse.ArgumentParser(description="Render the Homebrew formula for lynxdb-nightly")
    parser.add_argument("--version", required=True, help="Nightly version tag, e.g. v0.7.0-nightly.20260509.g1a2b3c4")
    parser.add_argument("--checksums", required=True, type=Path, help="Path to GoReleaser checksums.txt")
    parser.add_argument("--base-url", default="https://dl.lynxdb.org", help="Release CDN base URL")
    parser.add_argument("--output", required=True, type=Path, help="Output formula path")
    args = parser.parse_args()

    if "-nightly." not in args.version:
        print("ERROR: --version must be a nightly prerelease tag", file=sys.stderr)
        return 2

    try:
        artifacts = collect_artifacts(parse_checksums(args.checksums), args.version)
    except ValueError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(render_formula(args.version, args.base_url, artifacts))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
