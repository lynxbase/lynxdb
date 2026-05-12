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
| SPL `chart` executes grouped aggregation and pivots one aggregate with a column split | `pkg/spl2/parser.go`, `pkg/engine/pipeline/pipeline.go`, `pkg/engine/pipeline/pipeline_test.go` |
| Time aggregates `earliest_time(field)`, `latest_time(field)`, and `rate(field)` execute from event-time order | `pkg/engine/pipeline/aggregate.go`, `pkg/api/rest/server_test.go` |
| SPL2 `reverse` command reverses current row order without changing row contents | `pkg/spl2/parser.go`, `pkg/engine/pipeline/pipeline.go`, `pkg/engine/pipeline/pipeline_test.go` |
| SPL `regex` command filters `_raw` by default and supports field `=` / `!=` patterns | `pkg/spl2/parser.go`, `pkg/engine/pipeline/pipeline.go`, `pkg/engine/pipeline/pipeline_test.go` |
| SPL `replace` command replaces exact and wildcard field values across selected fields | `pkg/spl2/parser.go`, `pkg/engine/pipeline/replace.go`, `pkg/engine/pipeline/pipeline_test.go` |
| SPL `fieldformat` parses one field/eval-expression pair and preserves underlying row values | `pkg/spl2/parser.go`, `pkg/engine/pipeline/fieldformat.go`, `pkg/engine/pipeline/pipeline_test.go` |
| SPL/SPL2 `mvexpand` and SPL2 `expand` expand one multivalue/array field into separate rows, including `limit` | `pkg/spl2/parser.go`, `pkg/engine/pipeline/unroll.go`, `pkg/engine/pipeline/unroll_test.go` |
| SPL/SPL2 `makeresults` generates temporary rows with `_time`, default/positional/`count=<n>` counts, and `annotate=true` metadata fields | `pkg/spl2/parser.go`, `pkg/engine/pipeline/pipeline.go`, `pkg/engine/pipeline/pipeline_test.go` |
| SPL/SPL2 `untable` converts wide rows into name/value rows for every field except the x-field | `pkg/spl2/parser.go`, `pkg/engine/pipeline/untable.go`, `pkg/engine/pipeline/pipeline_test.go` |
| SPL/SPL2 `union` merges incoming rows with dataset or subsearch branches and enforces branch `maxout` | `pkg/spl2/parser.go`, `pkg/engine/pipeline/pipeline.go`, `pkg/engine/pipeline/pipeline_test.go` |
| SPL/SPL2 `appendcols` appends subsearch fields to current rows by row position and enforces `maxout` | `pkg/spl2/parser.go`, `pkg/engine/pipeline/appendcols.go`, `pkg/engine/pipeline/pipeline_test.go` |
| SPL/SPL2 `appendpipe` appends a subpipe result computed from the current rows and parses `run_in_preview` as preview metadata | `pkg/spl2/parser.go`, `pkg/engine/pipeline/appendpipe.go`, `pkg/engine/pipeline/pipeline_test.go` |
| LynxFlow `compare previous <dur>` parses RFC positive durations, replays the prior source time window, and adds `previous_*` numeric fields with absolute `change_*` deltas | `pkg/spl2/parser.go`, `pkg/spl2/parser_lynxflow_test.go`, `pkg/engine/pipeline/pipeline.go`, `pkg/engine/pipeline/compare.go`, `pkg/engine/pipeline/compare_test.go` |
| SPL/SPL2 `makemv` converts single-value fields into multivalue fields with delimiter or tokenizer splitting | `pkg/spl2/parser.go`, `pkg/engine/pipeline/makemv.go`, `pkg/engine/pipeline/pipeline_test.go` |
| SPL/SPL2 `mvcombine` merges rows that differ only by one field into a single row with multivalue field values | `pkg/spl2/parser.go`, `pkg/engine/pipeline/mvcombine.go`, `pkg/engine/pipeline/pipeline_test.go` |
| SPL/SPL2 `nomv` converts multivalue fields into one newline-delimited value | `pkg/spl2/parser.go`, `pkg/engine/pipeline/nomv.go`, `pkg/engine/pipeline/pipeline_test.go` |
| Optional capability commands parse and report capability-required execution errors | `pkg/spl2/parser.go`, `pkg/engine/pipeline/pipeline.go`, `pkg/spl2/parser_test.go`, `pkg/engine/pipeline/pipeline_test.go` |
| `use <fragment>` parses and expands named pipeline fragments with missing-fragment and cycle diagnostics | `pkg/spl2/parser.go`, `pkg/spl2/fragment.go`, `pkg/spl2/fragment_test.go` |
| SEARCH `L030` mixed `AND`/`OR` lint covers explicit `search` and normalized free-hand search inputs | `pkg/spl2/lints.go`, `pkg/spl2/lints_test.go` |
| Unsupported Splunk commands in the RFC profile reject with `L021` and compatibility hints | `pkg/spl2/parser.go`, `pkg/spl2/compat_hints.go`, `pkg/spl2/parser_test.go`, `pkg/spl2/compat_hints_test.go` |
| LynxFlow `proportion`, `impact`, `baseline`, `changes`, and `exemplars` deterministic desugaring | `pkg/spl2/parser.go`, `pkg/spl2/parser_lynxflow_test.go` |
| Web autocomplete and highlighting share one editor catalog | `web/src/editor/lynxflow-catalog.ts`, `web/src/editor/autocomplete.ts`, `web/src/editor/lynxflow-lang.ts` |
| EBNF includes currently implemented LynxFlow sugar | `docs/grammar/spl2.ebnf`, `cmd/lynxdb/grammar_data/spl2.ebnf` |

Official Splunk compatibility checked:

| Topic | Result |
|---|---|
| Boolean operator precedence and `XOR` support | Splunk docs say `search` evaluates `OR` before `AND` and does not support `XOR`; `where` and `eval` evaluate `AND`, then `OR`, then `XOR`. |
| Time aggregate functions | Splunk docs limit `per_second`, `per_minute`, `per_hour`, and `per_day` to `timechart`; `rate` uses `latest`, `earliest`, `latest_time`, and `earliest_time` semantics. |
| Chart command | Splunk docs define `chart` as a transforming command requiring a statistical function, with `OVER <row-split> BY <column-split>` equivalent to `BY <row-split> <column-split>` for row/column splits. LynxDB implements grouped aggregation and one-aggregate split pivots; advanced chart options are deferred. |
| Reverse command | Splunk docs define `reverse` as reversing result row order without changing which rows are returned. |
| Regex command | Splunk docs define `regex` as a streaming filter over `_raw` by default, with `field="pattern"` retaining matches and `field!="pattern"` retaining non-matches plus null field values. LynxDB uses the default linear regex engine unless PCRE2 is explicitly added later. |
| Replace command | Splunk docs define `replace (<wc-string> WITH <wc-string>)... [IN <field-list>]` as a streaming value replacement command. Wildcards match value text, replacement wildcards reuse captures, and internal fields require explicit `IN`. |
| Fieldformat command | Splunk docs define `fieldformat <field>=<eval-expression>` as changing rendered appearance without changing the underlying field value. Only one eval expression is accepted per command; exported data keeps original values. |
| Multivalue expansion | Splunk docs define `mvexpand` as expanding one multivalue field into separate rows while keeping other fields unchanged; SPL2 also defines `expand` for arrays. SPL2 places `limit=<int>` before the field, while SPL allows it after the field. |
| Makeresults command | Splunk docs define `makeresults` default row generation, `count=<num>`, `annotate=<bool>`, server targeting options, and `format`/`data` inline CSV or JSON. SPL2 examples also use positional counts. LynxDB implements generated rows with `_time` and `annotate`; inline `format`/`data` execution is deferred. |
| Untable command | Splunk docs define `untable <x-field> <y-name-field> <y-data-field>` as the inverse of `xyseries`, emitting field names other than the x-field into the y-name field and their values into the y-data field. |
| Union command | Splunk docs define `union` as merging two or more datasets, with operands as dataset names or subsearches and optional `maxout`, `maxtime`, and `timeout` subsearch options. LynxDB parses those options, enforces branch `maxout`, and merges incoming rows with branch rows; time-budget enforcement and `_time` interleaving are deferred. |
| Appendcols command | Splunk docs define `appendcols [override=<bool> | <subsearch-options>...] <subsearch>` as merging subsearch fields into current rows by row position while excluding internal fields; `maxout` caps subsearch result rows. |
| Appendpipe command | Splunk docs define `appendpipe` as appending the result of a subpipe run when the search reaches the command, unlike subsearch commands that run first. `run_in_preview` defaults to true and controls Splunk preview display only. |
| Makemv command | Splunk docs define `makemv [delim=<string> | tokenizer=<string>] [allowempty=<bool>] [setsv=<bool>] <field>` as splitting a single-value field into multivalue values. LynxDB supports delimiter, tokenizer, and `allowempty`; `setsv` is parsed but not separately observable in the current value model. |
| Mvcombine command | Splunk docs define `mvcombine [delim=<string>] <field>` as merging rows where all fields except the specified field match, turning the specified field into a multivalue field. LynxDB implements grouping and multivalue output; delimiter-specific single-value display metadata is deferred. |
| Nomv command | Splunk docs define `nomv <field>` as converting multivalue field values into one single value separated with a newline delimiter. |

## Partial

| RFC area | Current state | Gap |
|---|---|---|
| Source selectors | `FROM`, `INDEX`, lists, RFC glob matching, source exclude globs, `*`, CTE refs, and compact time ranges parse | Some source-scope diagnostics still need coverage against the RFC rewrite contract. |
| Time modifiers | Source-prefix `earliest`/`latest` normalizes to compact source time ranges, including `now`/`now()` latest values; `_index_earliest` and `_index_latest` normalize to explicit `_indextime` predicates; lossless deprecated `daysago`, `hoursago`, `minutesago`, matching `end*` forms, `starttime`, `endtime`, and supported `timeformat` values normalize to explicit ranges | Unsupported legacy `timeformat` directives still need rejection with structured suggestions. |
| Rewrite transparency | `NormalizeQuery` rewrites source-less and Splunk-style forms | Rewrites are not yet recorded as structured `Rewrite{Before, After, Reason}` through CLI/TUI/REST metadata. |
| Lints | Compatibility hints, parse suggestions, and post-parse `L001`/`L002`/`L003`/`L005`/`L010`/`L012`/`L013`/`L022`/`L030`/`L031`/`L034`/`L036` exist | Most RFC lint catalog entries `L001` through `L039` are not implemented yet. |
| Quoted identifier canon | Single-quoted identifiers now parse as canonical names and double-quoted names remain accepted in legacy positions with `L012` | Some less-common double-quoted legacy name positions may still need coverage. |
| Function catalog | Many eval and aggregate functions parse and execute; common aggregate aliases and time aggregates now normalize before planning | RFC aggregate/eval catalog needs a full parser, VM, and editor cross-check for missing functions and aliases. |
| Command catalog | Native SPL2/LynxFlow commands, several helpers, and optional capability command syntax parse; profile-excluded Splunk commands reject with `L021` | Optional capability command execution semantics remain deferred. |
| Editor assistance | Autocomplete covers commands, fields, values, regex snippets, time values, and templates | Ranking reasons and disable switches are not surfaced as RFC `meta.suggestions` behavior. |
| REST lint metadata | Sync, completed hybrid, async handles, and job completion responses expose `meta.lints` for implemented lints; `lint: false` disables them | Full lint output controls are not wired yet. |
| CLI/TUI assistance | Shell autocomplete command vocabulary matches the parser-supported command catalog; `lynxdb query --no-lint` passes `lint:false`; server-mode CLI/TUI results render returned lints on stderr | Query-context autocomplete and rewrite preview are not fully aligned with the web catalog yet. |

## Missing Or Deferred

| RFC requirement | Status | Reason |
|---|---|---|
| Full duration grammar including calendar `M`/`y` units | Deferred | Current parser and runtime cover signed `s`/`m`/`h`/`d`/`w` relative ranges, snap suffixes, and week-start snap variants; calendar-aware units need a time arithmetic model beyond `time.Duration`. |
| Broad-search lints and explain blocks `L032`, `L037`, source counts, skipped segments | Deferred | Requires planner and API metadata integration. |
| Regex engine selection, PCRE2 diagnostics, and `L038`/`L039` | Deferred | Requires runtime regex engine configuration and planner literal-extraction diagnostics. |
| `chart` advanced options and multi-aggregate split pivots | Deferred | Current execution covers grouped aggregation and one-aggregate row/column pivots; Splunk options such as `limit`, `format`, `sep`, `cont`, and split-series filtering need chart metadata and option parsing. |
| `union` time limits and `_time` interleaving | Deferred | Branch `maxout` is implemented; `maxtime` and `timeout` parse for compatibility but need branch execution cancellation. Splunk-style time interleaving needs a merge iterator keyed by `_time`. |
| `appendcols` `maxtime` and `timeout` limits | Deferred | Row-wise merge, `override`, and `maxout` are implemented; time-budget options parse for compatibility but need branch execution cancellation. |
| `appendpipe` preview-mode display effect | Deferred | `run_in_preview` parses and is retained in the AST; Splunk's preview-only display behavior depends on preview-mode execution metadata that LynxDB does not expose. |
| `makeresults` `format` and `data` inline datasets | Deferred | Options parse and return an explicit execution error; CSV and JSON inline dataset execution requires a richer generator parser. |
| `fieldformat` render metadata | Deferred | Current `event.Value` has one representation per field; expressions parse and rows keep original values, but alternate display strings are not represented. |
| `makemv` `setsv` dual representation | Deferred | Current `event.Value` has one representation per field; delimiter and tokenizer multivalue splitting are implemented, but parallel single-value display metadata is not represented. |
| `mvcombine` delimiter display metadata | Deferred | Current `event.Value` has one representation per field; row grouping and multivalue values are implemented, but delimiter-specific alternate display strings are not represented. |
| Optional capability command execution semantics | Deferred | `addinfo`, `convert`, `fieldsummary`, `flatten`, `iplocation`, `tags`, `typer`, `thru`, `timewrap`, `tstats`, and `mstats` parse for compatibility, but execution depends on deployment-specific metadata, GeoIP data, accelerated indexes, or metrics stores. |
| `facets` fan-out normalization | Deferred | Requires prefix-aware normalizer support for command suffixes that expand the prior pipeline into `multisearch`. |
| REST `meta.rewrites`, `meta.suggestions`, `meta.explain` | Deferred | Requires API contract expansion without changing result row shape. |
| CLI `--show-rewritten` and TUI rewrite blocks | Deferred | `--no-lint` now disables server-side advisory lints and returned lints render on stderr; rewrite rendering still requires structured rewrite data. |
| Grammar source sharing between Go parser and web/CLI catalogs | Partial | Web now shares an editor catalog, but Go parser catalogs are still manually mirrored. |
