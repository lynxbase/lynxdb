# rsigma drift triage

[Back to Sigma docs](index.md)

The nightly drift workflow compares LynxDB's pinned rsigma corpus with rsigma
`main`. When it opens an issue, the db-expert owner triages the issue and
records the decision in the issue thread.

## Inputs

Open the latest workflow run and download the `drift.patch` artifact. The
artifact contains the generated corpus diff from rsigma `main` compared with
the pinned LynxDB corpus.

Tag the issue with `area/sigma` and `rsigma-drift`. If the change affects SPL2
parsing, planning, or execution, also tag the owning query-engine area.

## Decision tree

1. If the new output is compatible with LynxDB and the upstream rsigma change is
   intentional, bump the supported rsigma range.
2. If the new output is valid rsigma output but LynxDB cannot parse, plan, or
   execute it, file an SPL2 compatibility issue and keep the drift issue open.
3. If rsigma emits a shape LynxDB does not intend to support yet, document it in
   [limitations](limitations.md) and close the drift issue with that link.

## Bump supported rsigma

Run the sync script against the new tag:

```bash
scripts/sync_rsigma_golden.sh --rsigma-ref v0.9.1
```

Commit the corpus diff and update [compat](compat.md) with the new supported
range. Keep the patch artifact attached to the issue for review history.

## Extend LynxDB SPL2

File a separate issue that includes:

- The failing Sigma rule.
- The rsigma SPL2 output.
- The parser, planner, or execution error.
- A link to the drift issue and `drift.patch` artifact.

Do not widen the compatibility range until the SPL2 work lands and the corpus
tests pass.

## Document unsupported output

Update [limitations](limitations.md) with the unsupported shape and link to the
upstream rsigma issue if one exists. Close the drift issue only after the docs
change lands.
