# RFC-001 Query Syntax Gap Map

Status as of the current `feat/lynxflow_rfc` branch.

Source contract: `tmp/researches/opus_ux_syntax/RFC.md`.

## Implemented

| Area | Evidence |
|---|---|
| SEARCH precedence uses Splunk-compatible `OR` before `AND` and rejects SEARCH `XOR` | `pkg/spl2/search_parser.go`, `pkg/spl2/search_lexer.go`, `pkg/spl2/search_test.go` |
| WHERE/EVAL `XOR` parses and executes with lower precedence than `OR` | `pkg/spl2/parser.go`, `pkg/vm/compiler.go`, `pkg/vm/vm.go`, `pkg/spl2/parser_test.go`, `pkg/vm/compiler_test.go` |
| Single-quoted identifiers parse for sources, fields, aliases, and SEARCH field comparisons | `pkg/spl2/lexer.go`, `pkg/spl2/search_lexer.go`, `pkg/spl2/parser_test.go`, `pkg/spl2/parser_lynxflow_test.go`, `pkg/spl2/search_test.go` |
| Core LynxFlow projection/filter/aggregation/output sugar | `pkg/spl2/parser_lynxflow_test.go` |
| LynxFlow `proportion`, `impact`, `baseline`, `changes`, and `exemplars` deterministic desugaring | `pkg/spl2/parser.go`, `pkg/spl2/parser_lynxflow_test.go` |
| Web autocomplete and highlighting share one editor catalog | `web/src/editor/lynxflow-catalog.ts`, `web/src/editor/autocomplete.ts`, `web/src/editor/lynxflow-lang.ts` |
| EBNF includes currently implemented LynxFlow sugar | `docs/grammar/spl2.ebnf`, `cmd/lynxdb/grammar_data/spl2.ebnf` |

Official Splunk compatibility checked:

| Topic | Result |
|---|---|
| Boolean operator precedence and `XOR` support | Splunk docs say `search` evaluates `OR` before `AND` and does not support `XOR`; `where` and `eval` evaluate `AND`, then `OR`, then `XOR`. |

## Partial

| RFC area | Current state | Gap |
|---|---|---|
| Source selectors | `FROM`, `INDEX`, lists, globs, `*`, CTE refs, and compact time ranges parse | Negated source globs such as `FROM logs*,!logs-debug*` are not represented in `SourceClause` or planner selectors. |
| Time modifiers | `earliest`, `latest`, `_index_earliest`, `_index_latest` compatibility is partly normalized | `_index_*` planning and diagnostics need coverage against the RFC rewrite contract. |
| Rewrite transparency | `NormalizeQuery` rewrites source-less and Splunk-style forms | Rewrites are not yet recorded as structured `Rewrite{Before, After, Reason}` through CLI/TUI/REST metadata. |
| Lints | Compatibility hints and parse suggestions exist | RFC lint catalog `L001` through `L039` is not implemented as a post-parse lint stage. |
| Quoted identifier canon | Single-quoted identifiers now parse as canonical names and double-quoted names remain accepted in legacy positions | Compatibility lint `L012` for double-quoted field names is not implemented yet. |
| Function catalog | Many eval and aggregate functions parse and execute | RFC aggregate/eval catalog needs a parser, VM, and editor cross-check for missing functions and aliases. |
| Command catalog | Native SPL2/LynxFlow commands plus several helpers parse | SPL compatibility commands such as `chart`, `fieldformat`, `regex`, `replace`, `reverse`, `mvexpand`, `makeresults`, `union`, and optional capability commands remain incomplete. |
| Editor assistance | Autocomplete covers commands, fields, values, regex snippets, time values, and templates | Ranking reasons and disable switches are not surfaced as RFC `meta.suggestions` behavior. |
| CLI/TUI assistance | Shell autocomplete exists | Query-context autocomplete and lint/rewrite preview are not aligned with the web catalog yet. |

## Missing Or Deferred

| RFC requirement | Status | Reason |
|---|---|---|
| Full glob syntax including `**`, character classes, alternatives, and quoted glob escapes | Deferred | Requires selector AST and matcher updates beyond the current token-level glob detection. |
| SEARCH `L030` mixed `AND`/`OR` lint with parsed shape | Deferred | Needs post-parse lint framework and stable diagnostic metadata. |
| Broad-search lints and explain blocks `L032`, `L037`, source counts, skipped segments | Deferred | Requires planner and API metadata integration. |
| Regex engine selection, PCRE2 diagnostics, and `L038`/`L039` | Deferred | Requires runtime regex engine configuration and planner literal-extraction diagnostics. |
| `facets` fan-out normalization | Deferred | Requires prefix-aware normalizer support for command suffixes that expand the prior pipeline into `multisearch`. |
| `compare previous <dur>` previous-window replay | Partial | Command parses, but RFC replay semantics need verification and tests. |
| `use <fragment>` expansion | Partial | Command parses, but fragment resolution and missing-fragment diagnostics need full RFC tests. |
| REST `meta.lints`, `meta.rewrites`, `meta.suggestions`, `meta.explain` | Deferred | Requires API contract expansion without changing result row shape. |
| CLI `--show-rewritten`, `--no-lint`, and TUI rewrite/lint blocks | Deferred | Requires CLI/TUI option and rendering changes after structured rewrite/lint data exists. |
| Grammar source sharing between Go parser and web/CLI catalogs | Partial | Web now shares an editor catalog, but Go parser catalogs are still manually mirrored. |
