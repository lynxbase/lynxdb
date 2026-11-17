#!/usr/bin/env python3
"""
Generate manifest.json for LynxDB release distribution.

This script runs in CI after GoReleaser builds all artifacts. It reads the
checksums.txt file and artifact directory to produce a structured manifest
that the install.sh script can consume.

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
    r"lynxdb-v(?P<version>[0-9]+\.[0-9]+\.[0-9]+(?:-[0-9A-Za-z.-]+)?)-"
    r"(?P<os>linux|darwin|windows)-"
    r"(?P<arch>amd64|arm64|armv7)"
    r"(?P<variant>-musl)?"
    r"\.(?P<ext>tar\.gz|zip)$"
)
VERSION_PATTERN = re.compile(r"^v[0-9]+\.[0-9]+\.[0-9]+(?:-[0-9A-Za-z.-]+)?$")
NIGHTLY_PATTERN = re.compile(r"^v[0-9]+\.[0-9]+\.[0-9]+-nightly\.[0-9]{8}\.g[0-9a-fA-F]+$")
SOURCE_COMMIT_PATTERN = re.compile(r"-nightly\.[0-9]{8}\.g(?P<sha>[0-9a-fA-F]+)$")


def is_prerelease(version: str) -> bool:
    """Return whether a version has a SemVer prerelease suffix."""
    return "-" in version


def source_commit(version: str) -> str | None:
    """Extract the short source commit from a nightly version."""
    match = SOURCE_COMMIT_PATTERN.search(version)
    if not match:
        return None
    return match.group("sha")


def validate_release_channel(version: str, channel: str, allow_stable_prerelease: bool = False) -> None:
    """Reject channel/version combinations that can overwrite stable pointers."""
    if not VERSION_PATTERN.match(version):
        raise ValueError("version must match vX.Y.Z or vX.Y.Z-prerelease")

    if channel not in ("stable", "nightly"):
        raise ValueError("channel must be stable or nightly")

    if channel == "stable" and is_prerelease(version) and not allow_stable_prerelease:
        raise ValueError("stable channel cannot be used with a prerelease version")

    if channel == "nightly" and not NIGHTLY_PATTERN.match(version):
        raise ValueError("nightly channel requires vX.Y.Z-nightly.YYYYMMDD.gSHA")


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

        artifact_version = f"v{match.group('version')}"
        if artifact_version != version:
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

    manifest = {
        "version": version,
        "channel": channel,
        "released_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "changelog_url": f"https://github.com/lynxbase/lynxdb/releases/tag/{version}",
        "artifacts": artifacts,
        "notices": [],
    }

    commit = source_commit(version)
    if commit:
        manifest["source_commit"] = commit

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
    parser.add_argument("--channel", choices=["stable", "nightly"], default="stable", help="Release channel")
    parser.add_argument(
        "--allow-stable-prerelease",
        action="store_true",
        help="Allow a prerelease version with --channel stable",
    )
    parser.add_argument("--output", default="manifest.json", help="Output file path")
    args = parser.parse_args()

    try:
        validate_release_channel(
            args.version,
            args.channel,
            allow_stable_prerelease=args.allow_stable_prerelease,
        )
    except ValueError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(2)

    print(f"Generating manifest for {args.version}...")
    checksums = parse_checksums(args.checksums)
    sizes = scan_artifacts(args.artifacts_dir)

    print(f"  Found {len(checksums)} checksums, {len(sizes)} artifacts")

    manifest = build_manifest(
        version=args.version,
        checksums=checksums,
        sizes=sizes,
        base_url=args.base_url,
        channel=args.channel,
    )

    warnings = validate_manifest(manifest)
    for w in warnings:
        print(f"  WARNING: {w}", file=sys.stderr)

    if not manifest["artifacts"]:
        print("ERROR: no release artifacts matched the requested version", file=sys.stderr)
        sys.exit(1)

    platforms = list(manifest["artifacts"].keys())
    print(f"  Platforms: {', '.join(platforms)}")

    # Write output
    with open(args.output, "w") as f:
        json.dump(manifest, f, indent=2)
        f.write("\n")

    print(f"  Manifest written to {args.output}")

if __name__ == "__main__":
    main()
