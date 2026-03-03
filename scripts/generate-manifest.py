#!/usr/bin/env python3
"""
Generate manifest.json for LynxDB release distribution.

This script runs in CI after GoReleaser builds all artifacts. It reads the
checksums.txt file and artifact directory to produce a structured manifest
that the install.sh script and `lynxdb upgrade` command can consume.

Usage:
    python3 generate-manifest.py \
        --version v0.5.0 \
        --checksums dist/checksums.txt \
        --artifacts-dir dist/ \
        --base-url https://dl.lynxdb.org \
        --output manifest.json
"""

import argparse
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path


# Map GoReleaser naming conventions to our platform keys
# Archive format: lynxdb-v{version}-{os}-{arch}[-musl].tar.gz
ARTIFACT_PATTERN = re.compile(
    r"lynxdb-v[\d.]+(?:-(?:rc|alpha|beta|dev)\.\d+)?-"
    r"(?P<os>linux|darwin|freebsd|windows)-"
    r"(?P<arch>amd64|arm64|armv7)"
    r"(?P<variant>-musl)?"
    r"\.(?P<ext>tar\.gz|zip)$"
)


def parse_checksums(checksums_path: str) -> dict[str, str]:
    """Parse GoReleaser checksums.txt into {filename: sha256} dict."""
    checksums = {}
    with open(checksums_path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split()
            if len(parts) >= 2:
                sha256 = parts[0]
                filename = parts[1].lstrip("*")  # Handle BSD-style checksums
                checksums[filename] = sha256
    return checksums


def scan_artifacts(artifacts_dir: str) -> dict[str, int]:
    """Scan directory for release artifacts and return {filename: size}."""
    sizes = {}
    for entry in Path(artifacts_dir).iterdir():
        if entry.is_file():
            sizes[entry.name] = entry.stat().st_size
    return sizes


def build_manifest(
    version: str,
    checksums: dict[str, str],
    sizes: dict[str, int],
    base_url: str,
    channel: str = "stable",
) -> dict:
    """Build the manifest.json structure."""
    artifacts = {}

    for filename, sha256 in sorted(checksums.items()):
        match = ARTIFACT_PATTERN.match(filename)
        if not match:
            continue

        os_name = match.group("os")
        arch = match.group("arch")
        variant = match.group("variant") or ""

        # Build platform key: {os}-{arch}[-musl]
        platform_key = f"{os_name}-{arch}{variant}"

        artifacts[platform_key] = {
            "url": f"{base_url}/{version}/{filename}",
            "sha256": sha256,
            "size": sizes.get(filename, 0),
            "filename": filename,
        }

    # Determine if this is a pre-release
    is_prerelease = bool(re.search(r"-(rc|alpha|beta|dev)\.", version))

    manifest = {
        "version": version,
        "channel": "prerelease" if is_prerelease else channel,
        "released_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "changelog_url": f"https://github.com/OrlovEvgeny/Lynxdb/releases/tag/{version}",
        "artifacts": artifacts,
        "notices": [],
    }

    return manifest


def validate_manifest(manifest: dict) -> list[str]:
    """Validate manifest completeness. Returns list of warnings."""
    warnings = []

    expected_platforms = [
        "linux-amd64",
        "linux-arm64",
        "darwin-amd64",
        "darwin-arm64",
    ]

    for platform in expected_platforms:
        if platform not in manifest["artifacts"]:
            warnings.append(f"Missing expected platform: {platform}")

    if not manifest["artifacts"]:
        warnings.append("No artifacts found!")

    return warnings


def main():
    parser = argparse.ArgumentParser(description="Generate LynxDB release manifest")
    parser.add_argument("--version", required=True, help="Release version (e.g. v0.5.0)")
    parser.add_argument("--checksums", required=True, help="Path to checksums.txt")
    parser.add_argument("--artifacts-dir", required=True, help="Directory containing artifacts")
    parser.add_argument("--base-url", default="https://dl.lynxdb.org", help="CDN base URL")
    parser.add_argument("--channel", default="stable", help="Release channel")
    parser.add_argument("--output", default="manifest.json", help="Output file path")
    args = parser.parse_args()

    # Parse inputs
    print(f"Generating manifest for {args.version}...")
    checksums = parse_checksums(args.checksums)
    sizes = scan_artifacts(args.artifacts_dir)

    print(f"  Found {len(checksums)} checksums, {len(sizes)} artifacts")

    # Build manifest
    manifest = build_manifest(
        version=args.version,
        checksums=checksums,
        sizes=sizes,
        base_url=args.base_url,
        channel=args.channel,
    )

    # Validate
    warnings = validate_manifest(manifest)
    for w in warnings:
        print(f"  WARNING: {w}", file=sys.stderr)

    platforms = list(manifest["artifacts"].keys())
    print(f"  Platforms: {', '.join(platforms)}")

    # Write output
    with open(args.output, "w") as f:
        json.dump(manifest, f, indent=2)
        f.write("\n")

    print(f"  Manifest written to {args.output}")

    # Exit with warning code if validation issues
    if warnings and not manifest["artifacts"]:
        sys.exit(1)


if __name__ == "__main__":
    main()
