# Releasing

## Versions

A version reads `year.month.patch.build`, and a release is tagged with it and
its channel: `v26.11.1.80-stable`.

| Part | Meaning |
|---|---|
| `26.11` | The line, named after the month (UTC) it was cut from `main`. It lives on the branch `26.11`. |
| `1` | The patch: 1 for the release that opens the line, one more for each release of the line after it. |
| `80` | The build: the first-parent commits since the release this one follows. |
| `-stable`, `-lts` | The channel of the line. An lts line is one that keeps receiving patches. |
| `-prestable` | A candidate. It is published as a pre-release, enters no channel and no formula, and the release that follows counts its build as if the candidate were not there. |

The tags are the only record of a version. A build without a tag on its
commit reports the release before it and the distance: `26.11.1.80+5.gabc1234`.
`mise run version` prints what the current tree builds as.

One line opens per month. A second release in the same month is a patch of
that line.

The releases up to `v0.2.5` were numbered `major.minor.patch`, and nightly
builds were tagged `v0.2.6-nightly.<date>.g<commit>`. Those tags stay as they
are. The first release numbered by date counts its build from the first
commit, and its notes start after `v0.2.5`.

Release tags are not Go module versions:
`go install github.com/lynxbase/lynxdb/cmd/lynxdb@latest` resolves to
`v0.2.5`, and `@main` builds the tip.

## Cutting a release

```sh
mise run release -- new                      # a new line from main
mise run release -- new --lts                # the same, as an lts line
mise run release -- patch --ref 26.11        # the next patch of a line
mise run release -- new --prestable          # a candidate
mise run release -- new --dry-run            # everything short of publishing
```

Each prints the version it will produce and starts the `Release` workflow,
which can also be started from the Actions tab. The workflow

1. computes the version and refuses a commit that is not on `main` (for a
   new line) or on the branch of the line (for a patch);
2. runs every check of continuous integration on that commit, with the
   binaries and archives built as that version;
3. waits for approval of the `release` environment;
4. attests the archives and the installer, tags the commit, pushes the
   container image `ghcr.io/lynxbase/lynxdb` for amd64 and arm64 and attests
   it, and publishes the GitHub release with notes generated from the commit
   subjects;
5. opens the branch of a new line;
6. rebuilds the download site from every published release and uploads it;
7. writes the formula `lynxdb@<line>` in the Homebrew tap
   `lynxbase/homebrew-tap`, and `lynxdb` too when the release is the newest
   one;
8. opens a pull request that adds the notes to `CHANGELOG.md`.

A release carries these files, the ones the checks installed:

```
lynxdb-<version>-linux-amd64.tar.gz
lynxdb-<version>-linux-arm64.tar.gz
lynxdb-<version>-linux-armv7.tar.gz
lynxdb-<version>-darwin-amd64.tar.gz
lynxdb-<version>-darwin-arm64.tar.gz
lynxdb-<version>-windows-amd64.zip
install.sh
SHA256SUMS
```

The image is tagged with the version, with the line, and with `latest` when
the release is the newest one.

A candidate is published with its files, and its image is tagged with its
version alone. It opens no branch, enters no channel and no formula, and adds
nothing to the changelog. `install.sh --version <version>` installs it.

## Patching a line

Bring the fixes to the branch of the line, then release with
`patch --ref <line>`. While the line has received nothing of its own, moving
its branch forward to `main` and releasing a patch is how a second release in
a month is made.

The workflows always come from `main`; the tasks and scripts they run come
from the released commit. A task that continuous integration calls therefore
has to exist on every line that still gets patches.

## When a release stops halfway

Run the failed jobs again from the workflow run: an existing tag is accepted
if it names the same commit, a draft release is filled and published, a
public one is left as it is, and the site is rebuilt from what is published.
Running all jobs again stops at the plan, which refuses a version that is
already tagged. `Download site` rebuilds and uploads the site on its own.

A published release cannot be changed. A bad one is followed by a patch.

## The download site

`https://dl.lynxdb.org` is the R2 bucket `lynxdb-releases`, and
`https://lynxdb.org/install.sh` redirects to its `install.sh`.

| Path | Holds |
|---|---|
| `install.sh` | the installer of the newest stable or lts release |
| `dist/<tag>/` | the files of a release, the same as its GitHub assets |
| `channels/stable`, `channels/lts` | the newest version of the channel |
| `versions/<version>` | the tag that holds a version |

The site is built from the GitHub releases every time, so the bucket holds
nothing that cannot be made again. `tools/release/deploy.sh` uploads the
release files first, to be cached for good, and the files that name a release
after, to be checked on every request. A file under `dist/` that differs from
the bucket's stops the upload. Nothing is deleted, so the files of the
releases numbered `major.minor.patch` stay where the installer of that time
put them: `<tag>/` and `manifest.json` at the root.

`install.sh` and `channels/` are written once a stable or lts release
numbered by date exists. Until then the bucket serves the installer of the
releases before it, which installs `v0.2.5` and cannot install a candidate.

`mise run site` builds the site from releases downloaded with
`tools/release/fetch.sh dist/releases`.

## Setting the repository up

`tools/release/github-setup.sh` applies the settings below and is safe to run
again:

- immutable releases and private vulnerability reports;
- the `release` environment, with whoever runs the script as its reviewer,
  open to workflow runs from `main` alone;
- rules: `main` and the line branches can be neither rewritten nor deleted;
  the tags of the releases numbered by date can be neither moved nor deleted;
- permission for workflows to open pull requests;
- a deploy key with write access to `lynxbase/homebrew-tap`, stored as the
  secret `HOMEBREW_TAP_KEY` of the `release` environment.

Two of these need the organization to allow them first, and a new
organization allows neither. An owner sets both in the organization's
settings: under Actions, General, Workflow permissions, "Allow GitHub Actions
to create and approve pull requests"; and under Member privileges, "Deploy
keys", Enabled. The script stops with the link to the setting when one is
off.

These secrets, of the repository or of its `release` environment, are what
the workflow publishes with:

| Secret | Holds | Without it |
|---|---|---|
| `CF_ACCOUNT_ID` | the Cloudflare account of the bucket | the download site is left as it is |
| `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY` | an R2 API token that may write the objects of `lynxdb-releases` | the same |
| `HOMEBREW_TAP_KEY` | the SSH deploy key that `github-setup.sh` adds to `lynxbase/homebrew-tap` | the formulae are not updated |

The container image is pushed with the token of the workflow run. The
documentation site is published by the `Docs` workflow, with `CF_PAGES_TOKEN`
and `CF_ACCOUNT_ID`.

## Tools

`mise.toml` names the tools and `mise.lock` records the exact version and
checksum of each. The `Tools` workflow runs weekly, moves the lockfile to the
newest versions `mise.toml` allows, and opens a pull request. Dependabot does
the same for the GitHub Actions, the Go modules and the container images.
