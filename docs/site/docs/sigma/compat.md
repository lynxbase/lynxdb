# Sigma compatibility

[Back to Sigma docs](index.md)

## Contract

For every Sigma rule that `rsigma convert -t lynxdb` v0.9.0 produces a
non-error output for, the resulting SPL2 string must:

1. Parse through `pkg/spl2.ParseProgram` without error.
2. Plan through `pkg/engine/pipeline.NewPipeline` without error.
3. Execute with the semantics specified here: match the same set of events that
   rsigma's own `eval` engine would match on identical input.

LynxDB pins rsigma v0.9.0 for v1 of this contract. A future LynxDB release may
extend the supported rsigma tag range, but must not narrow it for the same
contract version.

## Supported output shapes

| Sigma construct | rsigma LynxDB SPL2 output |
|---|---|
| `CommandLine: whoami` | `FROM main | search CommandLine="whoami"` |
| `CommandLine|contains: whoami` | `FROM main | search CommandLine=*"whoami"*` |
| `CommandLine|startswith: cmd` | `FROM main | search CommandLine="cmd"*` |
| `Image|endswith: '.exe'` | `FROM main | search Image=*".exe"` |
| `(sel and not filter) or extra` | `FROM main | search (FieldA="val1" AND NOT FieldB="val2") OR FieldC="val3"` |
| `EventCount|gte: 10` | `FROM main | search EventCount>=10` |
| Status range from 400 through 499 | `FROM main | search` with a bounded numeric status predicate |
| `CommandLine|re: '.*whoami.*'` | `FROM main | search * | where CommandLine =~ ".*whoami.*"` |
| `SourceIP|cidr: '10.0.0.0/8'` | `FROM main | search * | where cidrmatch("10.0.0.0/8", SourceIP)` |
| `FieldA|exists: true` | `FROM main | search FieldA=*` |
| `FieldA: null` | `FROM main | search NOT FieldA=*` |
| `keywords: [error, timeout, refused]` | `FROM main | search "error" OR "timeout" OR "refused"` |
| `CommandLine|cased: Whoami` | `FROM main | search CommandLine=CASE("Whoami")` |
| OR-list collapse | `FROM main | search source IN ("nginx", "postgres", "redis")` |

## Known limitations

The compatibility contract only covers query strings that rsigma emits without
error for the LynxDB backend. Unsupported Sigma constructs remain upstream
rsigma errors. See [limitations](limitations.md) for the current list.

`lynxdb query --queries-file` and `lynxdb saved import` accept any SPL2 file,
including rsigma output. They do not call rsigma.

## Provenance header

Clients that submit rsigma-produced queries over REST may send:

```http
Sigma-Source: rsigma/0.9.0
```

The header is informational. LynxDB records it in request logs and does not
change query parsing, planning, or execution behavior.

## Embedded manifest

Every LynxDB release carries `pkg/sigmaqueries/compat_manifest.json` as a
release artifact and embeds the same manifest into the binary.

Print the embedded summary:

```bash
lynxdb sigma compat-check
```

Check a specific rsigma version:

```bash
lynxdb sigma compat-check --rsigma-version 0.9.0
```

Export the full manifest:

```bash
lynxdb sigma compat-check --json
```

## Drift policy

Nightly drift checks compare LynxDB's pinned corpus against rsigma `main`.
Triage follows the [drift runbook](drift-runbook.md): bump the supported rsigma
range, extend LynxDB SPL2, or document the shape as unsupported.
