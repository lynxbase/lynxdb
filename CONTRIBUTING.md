# Contributing to LynxDB

## Build and test locally

[mise](https://mise.jdx.dev/) installs the tools at the versions
[`mise.lock`](mise.lock) records and names the commands used day to day.

```bash
git clone https://github.com/lynxbase/lynxdb.git
cd lynxdb
curl https://mise.run | sh
mise trust
mise install
mise tasks
mise run build
mise run test
```

For quick local Go-only checks, with Go 1.26 or later:

```bash
go build -o lynxdb ./cmd/lynxdb/
go test ./...
```

See the [development setup guide](docs/site/docs/contributing/development-setup.md)
for running a local server, golden files and benchmarks.

## Branches

Branch off `main` and name the branch `<type>/<short-description>`.

| Prefix      | When to use                                          |
|-------------|------------------------------------------------------|
| `feat/`     | New feature                                          |
| `fix/`      | Bug fix                                              |
| `chore/`    | Build, CI, deps, cleanup                             |
| `refactor/` | Code change that doesn't fix a bug or add a feature  |
| `docs/`     | Documentation only                                   |
| `test/`     | Adding or fixing tests                               |
| `perf/`     | Performance improvement                              |

Examples: `feat/streaming-join`, `fix/flush-retry`, `chore/bump-go-version`.

## Commits

Use the same prefixes for commit messages, `<type>: <subject>`, imperative and
lowercase after the prefix, under 72 characters:

```
feat: add trendline command
fix: retry a flush that a full disk interrupted
chore: update zstd to v1.18
```

One logical change per commit. Keep commits small and reviewable.

## Pull requests

- Keep each PR to one logical change and use `type: description` in the title.
- Run `mise run ci`, which is the linters and the unit tests. Add tests for
  new code.
- For a PPL command or function, update its catalog entry in `pkg/ppl`. Add
  compile goldens under `pkg/ppl/testdata/compile/` and execution tests, then
  regenerate the reference pages with `mise run gen`.
- A change to the installer, the archives or the container image is checked
  by `mise run dist`, `mise run package`, then `mise run test:package` and
  `mise run test:image`, which need Docker.

The subjects are what users read: the notes of a release are the `feat`,
`perf` and `fix` subjects since the release before, and the changes marked
with `!` after the type. `mise run changelog` shows what the next release
would say.

## Continuous integration

Every pull request and every push to `main` runs the linters, the unit tests
on Linux for x86-64 and arm64 and on macOS, the unit tests under the race
detector, the end-to-end, command line and shipper compatibility tests, and
the documentation build. It also builds the release binaries of every
platform, packs the archives, installs from them with the installer on the
runner and in containers, and builds and starts the container image. The
check named `CI` passes when all of that did. The shipper conformance matrix
runs nightly.

## Code style

- `mise run lint` must pass: golangci-lint with `.golangci.yml`, `gofmt`
  formatting (`mise run fmt` fixes it), a tidy `go.mod`, shellcheck, the
  workflow linters and the commit subjects.
- Exported symbols have doc comments.
- Errors are wrapped with the failed operation: `fmt.Errorf("columnar: index %s: %w", index, err)`.
- `context.Context` is the first parameter of any I/O or blocking function.
- Avoid mutable package-level state; no `panic` for control flow.
- The query language stays in `pkg/ppl`; the optimizer, the columnar executor
  and the row operators work on `pkg/ir` only.

See [coding guidelines](docs/site/docs/contributing/coding-guidelines.md).

## Testing

```bash
mise run test                               # Go unit tests
mise run test:race                          # the same under the race detector
mise run test:e2e                           # end-to-end server tests
mise run test:cli                           # tests against the built binary
go test ./pkg/ppl/ -run TestCompileGolden   # one package and test
go test ./pkg/ppl/ -run TestCompileGolden -update   # rewrite compile goldens
go test -run '^$' -bench . -benchmem ./pkg/vec/...  # benchmarks
```

Run tests from the repository root with package paths; tests read testdata
relative to their package.

Test locations:

- Unit tests: next to the code (`*_test.go` in the same package).
- E2E tests: `test/e2e/` (shipper compatibility in `test/e2e/shippers/`).
- CLI tests: `test/cli/`.
- Regression tests: `test/regression/`.
- Installer and container image tests: `test/packaging/`.

## Common tasks

```bash
mise run build          # build ./lynxdb, stamped with the version git reports
mise run ci             # every linter and the unit tests
mise run fmt            # format the Go sources
mise run bench          # micro benchmarks into artifacts/bench-micro.txt
mise run gen            # regenerate the PPL reference pages from the catalogs
mise run version        # the version this tree builds as
```

`mise tasks` lists the rest. [RELEASING.md](RELEASING.md) describes versions
and how a release is cut.

## Project structure

- `cmd/lynxdb/` -- CLI entry point.
- `pkg/` -- Core packages: PPL frontend (`pkg/ppl`), IR and optimizer
  (`pkg/ir`), execution (`pkg/engine`, `pkg/colstore/exec`), storage, ingest, API.
- `internal/` -- Packages not meant for import outside the module.

See the [project structure guide](docs/site/docs/contributing/project-structure.md).

## Documentation

Edit user-facing pages under [`docs/site/docs/`](docs/site/docs/) and build
the Docusaurus site before submitting changes.

```bash
mise run docs
```

Start a guide with the task and its prerequisites. Show a command the reader
can run and how to recognize a successful result. Link to the CLI, API, or
PPL reference for flags and options. Give each page a descriptive title and
summary in its front matter. The build uses them in search metadata and
`llms.txt`. It also writes `robots.txt`, `sitemap.xml`, and Markdown copies
of the published pages into `docs/site/build`. The Worker serves Markdown
at the canonical page URL when a client sends `Accept: text/markdown`.
The build also publishes `/openapi.json` and `/openapi.yaml` from
`docs/site/static/api/swagger.yaml`.

The PPL command, function, and aggregation references come from the catalogs
in `pkg/ppl`. Run `mise run gen` after editing the catalogs.
[CHANGELOG.md](CHANGELOG.md) is written from the commit subjects when a
release is cut.

### Documentation deployment

The [Docs workflow](.github/workflows/docs.yml) publishes the build to the
`lynxdb-docs` Worker. Its `CF_DOCS_WORKERS_TOKEN` secret needs Workers
Scripts Write on the Cloudflare account plus Zone Read and Workers Routes
Write on `lynxdb.org`. The account ID comes from `CF_ACCOUNT_ID`.

To verify Cloudflare routing against a local build without updating the
production Worker, deploy to the preview Worker. Sign in with `cf auth login`
or set `CLOUDFLARE_API_TOKEN` and `CLOUDFLARE_ACCOUNT_ID` for the deployment.

```bash
mise run docs
cd docs/site
mise x node@26.12.1 -- node worker/build.mjs preview
mise x node@26.12.1 -- npx --yes cf@1.0.0-rc.6 deploy --prebuilt --mode preview
```

Production uses the `docs.lynxdb.org/*` Worker route. Keep the proxied
`docs.lynxdb.org` CNAME pointing to `lynxdb-docs.pages.dev` so removing the
Worker route returns traffic to Pages. Pause the Docs workflow before
removing the route during a rollback so the next deploy cannot recreate it.

## Report an issue

Open a [GitHub issue](https://github.com/lynxbase/lynxdb/issues/new) and include
the following details.

- What you did (query, config, input data).
- What you expected.
- What happened instead.
- LynxDB version (`lynxdb version --json`).
