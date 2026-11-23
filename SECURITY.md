# Security

## Reporting a vulnerability

Report it privately through
[a security advisory](https://github.com/lynxbase/lynxdb/security/advisories/new),
not in a public issue.

## Supported versions

Fixes go into the newest release line and into the lts lines. `RELEASING.md`
explains how to tell them apart.

## Verifying a download

Every release lists the SHA-256 checksum of its files in `SHA256SUMS` and
carries a build attestation that ties each file to the workflow run and the
commit it came from:

```sh
gh attestation verify <file> --repo lynxbase/lynxdb
```

The container image carries one too:

```sh
gh attestation verify oci://ghcr.io/lynxbase/lynxdb:<version> --repo lynxbase/lynxdb
```

The install script checks the archive it downloads against `SHA256SUMS` and
stops when they differ.
