# RFC-001 Query Syntax Gap Map

Status as of the current `feat/lynxflow_rfc` branch.

Source contract: `docs/grammar/RFC.md`.

## Implemented

| Area | Evidence |
|---|---|
| SEARCH precedence uses Splunk-compatible `OR` before `AND` and rejects SEARCH `XOR` | `pkg/spl2/search_parser.go`, `pkg/spl2/search_lexer.go`, `pkg/spl2/search_test.go` |
| WHERE/EVAL `XOR` parses and executes with lower precedence than `OR` | `pkg/spl2/parser.go`, `pkg/vm/compiler.go`, `pkg/vm/vm.go`, `pkg/spl2/parser_test.go`, `pkg/vm/compiler_test.go` |
| Single-quoted identifiers parse for sources, fields, aliases, and SEARCH field comparisons | `pkg/spl2/lexer.go`, `pkg/spl2/search_lexer.go`, `pkg/spl2/parser_test.go`, `pkg/spl2/parser_lynxflow_test.go`, `pkg/spl2/search_test.go` |
| Core LynxFlow projection/filter/aggregation/output sugar | `pkg/spl2/parser_lynxflow_test.go` |
| Aggregate aliases `mean`, `median`, `distinct_count`, `estdc`, and supported percentile forms normalize to executable aggregate functions | `pkg/spl2/parser.go`, `pkg/spl2/parser_test.go`, `pkg/api/rest/server_test.go` |
| Aggregate `range(field)` executes for numeric fields | `pkg/engine/pipeline/aggregate.go`, `pkg/engine/pipeline/partial_agg.go`, `pkg/api/rest/server_test.go` |
| Aggregate `sumsq(field)` executes for numeric fields | `pkg/engine/pipeline/aggregate.go`, `pkg/engine/pipeline/partial_agg.go`, `pkg/api/rest/server_test.go` |
| Aggregate `stdevp(field)`, `var(field)`, and `varp(field)` execute for numeric fields | `pkg/engine/pipeline/aggregate.go`, `pkg/engine/pipeline/partial_agg.go`, `pkg/api/rest/server_test.go` |
| Aggregate `list(field)` executes and preserves duplicate values separately from `values(field)` | `pkg/engine/pipeline/aggregate.go`, `pkg/engine/pipeline/eventstats.go`, `pkg/engine/pipeline/streamstats.go`, `pkg/api/rest/server_test.go` |
| Aggregate `estdc_error(field)` executes with exact-path zero and HLL standard error reporting | `pkg/engine/pipeline/aggregate.go`, `pkg/engine/pipeline/partial_agg.go`, `pkg/engine/pipeline/streamstats.go`, `pkg/api/rest/server_test.go` |
| Aggregate `mode(field)` executes as the most frequent string value | `pkg/engine/pipeline/aggregate.go`, `pkg/engine/pipeline/partial_agg.go`, `pkg/engine/pipeline/eventstats.go`, `pkg/engine/pipeline/streamstats.go`, `pkg/api/rest/server_test.go` |
| Timechart aggregates `per_second(field)`, `per_minute(field)`, `per_hour(field)`, and `per_day(field)` scale numeric bucket totals by span | `pkg/engine/pipeline/aggregate.go`, `pkg/engine/pipeline/pipeline.go`, `pkg/api/rest/server_test.go` |
| Time aggregates `earliest_time(field)`, `latest_time(field)`, and `rate(field)` execute from event-time order | `pkg/engine/pipeline/aggregate.go`, `pkg/api/rest/server_test.go` |
| SPL2 `reverse` command reverses current row order without changing row contents | `pkg/spl2/parser.go`, `pkg/engine/pipeline/pipeline.go`, `pkg/engine/pipeline/pipeline_test.go` |
| Unsupported Splunk commands in the RFC profile reject with `L021` and compatibility hints | `pkg/spl2/parser.go`, `pkg/spl2/compat_hints.go`, `pkg/spl2/parser_test.go`, `pkg/spl2/compat_hints_test.go` |
| LynxFlow `proportion`, `impact`, `baseline`, `changes`, and `exemplars` deterministic desugaring | `pkg/spl2/parser.go`, `pkg/spl2/parser_lynxflow_test.go` |
| Web autocomplete and highlighting share one editor catalog | `web/src/editor/lynxflow-catalog.ts`, `web/src/editor/autocomplete.ts`, `web/src/editor/lynxflow-lang.ts` |
| EBNF includes currently implemented LynxFlow sugar | `docs/grammar/spl2.ebnf`, `cmd/lynxdb/grammar_data/spl2.ebnf` |

Official Splunk compatibility checked:

| Topic | Result |
|---|---|
| Boolean operator precedence and `XOR` support | Splunk docs say `search` evaluates `OR` before `AND` and does not support `XOR`; `where` and `eval` evaluate `AND`, then `OR`, then `XOR`. |
| Time aggregate functions | Splunk docs limit `per_second`, `per_minute`, `per_hour`, and `per_day` to `timechart`; `rate` uses `latest`, `earliest`, `latest_time`, and `earliest_time` semantics. |
| Reverse command | Splunk docs define `reverse` as reversing result row order without changing which rows are returned. |

## Partial

| RFC area | Current state | Gap |
|---|---|---|
| Source selectors | `FROM`, `INDEX`, lists, globs, `*`, CTE refs, and compact time ranges parse | Negated source globs such as `FROM logs*,!logs-debug*` are not represented in `SourceClause` or planner selectors. |
| Time modifiers | `earliest`, `latest`, `_index_earliest`, `_index_latest` compatibility is partly normalized | `_index_*` planning and diagnostics need coverage against the RFC rewrite contract. |
| Rewrite transparency | `NormalizeQuery` rewrites source-less and Splunk-style forms | Rewrites are not yet recorded as structured `Rewrite{Before, After, Reason}` through CLI/TUI/REST metadata. |
| Lints | Compatibility hints, parse suggestions, and post-parse `L001`/`L002`/`L003`/`L005`/`L010`/`L012`/`L013`/`L022`/`L030`/`L031`/`L034`/`L036` exist | Most RFC lint catalog entries `L001` through `L039` are not implemented yet. |
| Quoted identifier canon | Single-quoted identifiers now parse as canonical names and double-quoted names remain accepted in legacy positions with `L012` | Some less-common double-quoted legacy name positions may still need coverage. |
| Function catalog | Many eval and aggregate functions parse and execute; common aggregate aliases and time aggregates now normalize before planning | RFC aggregate/eval catalog needs a full parser, VM, and editor cross-check for missing functions and aliases. |
| Command catalog | Native SPL2/LynxFlow commands plus several helpers parse; profile-excluded Splunk commands reject with `L021` | SPL compatibility commands such as `chart`, `fieldformat`, `regex`, `replace`, `mvexpand`, `makeresults`, `union`, and optional capability commands remain incomplete. |
| Editor assistance | Autocomplete covers commands, fields, values, regex snippets, time values, and templates | Ranking reasons and disable switches are not surfaced as RFC `meta.suggestions` behavior. |
| REST lint metadata | Sync, completed hybrid, async handles, and job completion responses expose `meta.lints` for implemented lints; `lint: false` disables them | Full lint output controls are not wired yet. |
| CLI/TUI assistance | Shell autocomplete exists; `lynxdb query --no-lint` passes `lint:false`; server-mode CLI/TUI results render returned lints on stderr | Query-context autocomplete and rewrite preview are not aligned with the web catalog yet. |

## Missing Or Deferred

| RFC requirement | Status | Reason |
|---|---|---|
| Full glob syntax including `**`, character classes, alternatives, and quoted glob escapes | Deferred | Requires selector AST and matcher updates beyond the current token-level glob detection. |
| SEARCH `L030` mixed `AND`/`OR` lint with parsed shape | Partial | The lint is implemented for explicit SEARCH commands and surfaces through REST metadata plus CLI/TUI stderr; bare-search normalization paths still need coverage. |
| Broad-search lints and explain blocks `L032`, `L037`, source counts, skipped segments | Deferred | Requires planner and API metadata integration. |
| Regex engine selection, PCRE2 diagnostics, and `L038`/`L039` | Deferred | Requires runtime regex engine configuration and planner literal-extraction diagnostics. |
| `facets` fan-out normalization | Deferred | Requires prefix-aware normalizer support for command suffixes that expand the prior pipeline into `multisearch`. |
| `compare previous <dur>` previous-window replay | Partial | Command parses, but RFC replay semantics need verification and tests. |
| `use <fragment>` expansion | Partial | Command parses, but fragment resolution and missing-fragment diagnostics need full RFC tests. |
| REST `meta.rewrites`, `meta.suggestions`, `meta.explain` | Deferred | Requires API contract expansion without changing result row shape. |
| CLI `--show-rewritten` and TUI rewrite blocks | Deferred | `--no-lint` now disables server-side advisory lints and returned lints render on stderr; rewrite rendering still requires structured rewrite data. |
| Grammar source sharing between Go parser and web/CLI catalogs | Partial | Web now shares an editor catalog, but Go parser catalogs are still manually mirrored. |
