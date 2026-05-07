# Releasing

## Sigma / rsigma Compatibility

Before publishing a release, regenerate the compatibility manifest:

```bash
make compat-manifest VERSION=vX.Y.Z
```

The release workflow attaches `pkg/sigmaqueries/compat_manifest.json` to the
GitHub release. Release notes must include one line in a `Sigma / rsigma
compatibility` section:

```text
Still compatible with rsigma 0.9.x.
```

If the supported rsigma range changes, use:

```text
Extends compatibility to rsigma 0.10.x.
```
