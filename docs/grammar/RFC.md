# RFC-001: LynxDB Query Syntax

| Field | Value |
|---|---|
| Status | Draft |
| Author | Evgenii Orlov (evgenii@eorlov.org) |
| Last updated | 2026-05-12 |
| Supersedes | — |
| Related | `pkg/spl2/`, `pkg/sigmaqueries/`, README, CLAUDE.md |

---

## Abstract

The LynxDB query language consists of two layers:

1. **SPL2** — the canonical language, based on Splunk SPL2. Full semantics; any query can be expressed in it. The AST, optimizer, and VM operate exclusively on SPL2.
2. **LynxFlow** — a sugar layer on top of SPL2. Additive keywords oriented toward typical SRE/DevOps tasks (latency, errors, rate, proportion, impact, facets, compare, trace). Each LynxFlow construct has exactly one SPL2 expansion; the normalizer expands it before the optimization stage.

This document fixes the lexis, grammar of both layers, the command and function catalog, and the LynxFlow → SPL2 desugaring rules. It is a reference specification, not a tutorial.

---

## 1. Motivation

Existing alternatives sit at one extreme:
- **Splunk SPL2** — powerful but verbose and engine-centric (`bin _time span=5m | stats count() by status, _time` for a basic time-series operation).
- **KQL** — more compact than SPL, but its syntax is ML-oriented (`make-series`, `summarize`), not log-oriented.
- **LogQL / Loki** — simple, but without proper analytics.

LynxDB sits in the middle. SPL2 provides compatibility; LynxFlow gives "one query — one task" for typical cases (`from app | errors by service compute count()` instead of `index=app level=error | stats count() by service`).

Both layers live inside one parser, one pipeline, one AST. The user is free to mix them within a single query.

---

## 2. Design principles

**P1. SPL2 is the single source of truth.** After normalization the AST contains only SPL2 nodes. The optimizer, VM, ingest pipeline, and materialized views know nothing about LynxFlow.

**P2. LynxFlow is pure sugar.** Each LynxFlow construct has a short, explicit SPL2 expansion. That expansion is stable across releases.

**P3. One way to express each idea per layer.** If SPL2 already has `stats count by host`, LynxFlow has `group by host compute count()` — and nothing more. We do not introduce a third variant.

**P4. Full backward compatibility for SPL2.** Existing queries — the `pkg/sigmaqueries/` corpus, `parser_test.go`, README, user documents — must parse and execute identically.

**P5. No hidden heuristics in semantics.** Hints (`L00x`) are emitted after successful parsing but never change query behavior. If a LynxFlow command uses a documented default (for example `slowest` without a metric field uses `duration_ms`), the default is visible through lint and rewrite metadata.

**P6. Additive only.** Every new token is a new keyword. Existing tokens never change semantics.

**P7. UX aids are advisory.** Autocomplete, lint, examples, query previews, no-result guidance, and formatting never mutate the executed query unless the user explicitly accepts an edit. The parser and normalizer remain the only authority for execution.

**P8. Bare search is useful by default.** A query that is just a user term or phrase (`errors`, `"connection reset"`) searches the configured default source scope unless the user narrows it. The selected scope is explicit in the rewrite (`FROM <default>`) and visible in diagnostics.

**P9. Investigation helpers are deterministic.** LynxFlow may add shortcuts for common observability investigations, but they must be mechanical query rewrites: no AI, LLMs, probabilistic explanations, or hidden "root cause" inference. If a helper ranks, buckets, compares, or samples data, the exact fields, formulas, thresholds, and limits are visible in the SPL2 expansion.

---

## 3. Lexical structure

### 3.1 Encoding and whitespace
- Source text is UTF-8.
- Whitespace characters (` `, `\t`, `\n`, `\r`) separate tokens but are otherwise insignificant (except inside string literals).
- Line breaks are permitted between tokens within a query.

### 3.2 Comments
- Single-line: `-- comment until end of line`.
- No multi-line comments.

### 3.3 Identifiers
- Bare identifier: `[a-zA-Z_][a-zA-Z0-9_.:-]*` in canonical examples. The lexer accepts Unicode letters, digits, `_`, `.`, `-`, `:`, `*`, and `?` in identifier-like tokens; `*` or `?` makes the token a glob.
- Quoted identifier: `"my-field"` or `'my-field'` — for names with special characters, spaces, or names that collide with keywords.
- **Canon (RFC):** single quotes `'...'` for quoted identifiers everywhere; double quotes `"..."` are reserved for string values. Double quotes around names continue to work (legacy) with lint hint `L012`.

### 3.4 String literals
- Double-quoted: `"hello\nworld"` with escapes `\"`, `\\`, `\n`, `\t`, `\r`, `\u{NNNN}`.
- F-strings: `f"user={user_id} dur={duration_ms}ms"`. Inside `{...}` is an expression. Brace escape: `{{` and `}}` produce literal `{` and `}`.

### 3.5 Numeric literals
- Integers: `42`, `-42`.
- Floating-point: `3.14`, `1.5e-3`.
- Hexadecimal: not supported (no log-domain use case).

### 3.6 Durations and time literals
- Relative durations: `[+|-]<int><unit>` where `unit` ∈ {`s`, `m`, `h`, `d`, `w`, `M`, `y`}.
  - Examples: `-1h`, `-7d`, `+30m`, `-1w`.
- Snap-to: `<duration>@<unit>`.
  - `-1d@d` — start of yesterday.
  - `-1h@h` — start of the previous hour.
  - `@w0` — start of this week (Sunday), `@w1` — Monday, etc.
- Range: `<duration>..<duration>` — for example, `-7d..-1d` (between 7 and 1 days ago).
- Absolute time literals — ISO-8601 strings: `"2026-05-12T14:00:00Z"`.

### 3.7 Glob patterns
- `*` and `?` inside a bare identifier or value automatically make it a glob: `host=web-*`, `FROM logs*`.
- Supported glob syntax:
  - `*` — zero or more characters except the source hierarchy separator `/`.
  - `?` — exactly one character except `/`.
  - `**` — zero or more hierarchy components when matching source names or path-like fields.
  - `[abc]`, `[a-z]`, `[!a-z]` — character classes.
  - `{api,web,worker}` — alternatives.
  - `\*`, `\?`, `\[`, `\{`, `\\` — literal escapes inside quoted glob strings.
- Source lists support include and exclude globs: `FROM logs*,!logs-debug*`. Excludes apply after includes and never create sources by themselves.
- Leading wildcards (`*error*`) work but trigger lint `L001` ("slow").
- Glob matching is case-sensitive by default.

### 3.8 Regex patterns
- Regex is written as a double-quoted string after regex operators: `field =~ "panic|fatal"`, `field !~ "(?i)debug"`.
- SEARCH context does not have standalone regex literals. Use `where _raw =~ "panic|fatal"` when regex behavior is required; use `search panic OR fatal` when term search is enough.
- Regex flags are expressed inside the regex pattern when supported by the selected engine, for example `(?i)` for case-insensitive matching.
- Default regex engine is a guaranteed-linear automata engine. Look-around and backreferences are not accepted by default. Builds may expose PCRE2-compatible matching via `--regex-engine=pcre2` or REST `regex_engine: "pcre2"`, but only with query budgets and explicit diagnostics.
- Regex patterns over `_raw` or all fields participate in literal extraction for prefiltering. Patterns with no extractable literal still run, but trigger `L038`.

### 3.9 Operators

| Category | Tokens |
|---|---|
| Comparison | `=`, `==`, `!=`, `<`, `<=`, `>`, `>=` |
| Logical | `AND`, `OR`, `NOT`, `XOR` in WHERE/EVAL contexts; SEARCH context supports `AND`, `OR`, `NOT` only |
| Arithmetic | `+`, `-`, `*`, `/`, `%` |
| Concatenation | `+` (as in SPL2, unlike SPL1's `.`) |
| Regex | `=~`, `!~` |
| Membership | `IN (a, b, c)`, `NOT IN (...)` |
| Range | `BETWEEN x AND y` |
| Null | `IS NULL`, `IS NOT NULL` |
| Pattern | `LIKE "pattern"` (SQL-style with `%` and `_`) |
| Optional chaining | `?.` (for `user?.profile?.name`) |
| Coalesce | `??` (for `host ?? "unknown"`) |

### 3.10 Other tokens
- Pipe: `|`.
- Comma: `,`.
- Brackets: `(`, `)`, `[`, `]` (the latter pair for subsearches).
- CTE prefix: `$`.
- Statement terminator: `;` (separates CTE definitions from the final query).
- `@` — for snap-to and time aliases.
- `..` — for ranges.
- `=~`, `!~` — regex operators; the pattern is a string literal.

### 3.11 Keywords

All keywords are case-insensitive (`STATS` = `stats` = `Stats`). The canonical form is lowercase.

**SPL2 canon command keywords + modifiers:**

```
FROM, INDEX, WHERE, SEARCH, STATS, EVAL, SORT, HEAD, TAIL,
TIMECHART, REX, FIELDS, TABLE, DEDUP, RENAME, BIN,
STREAMSTATS, EVENTSTATS, JOIN, APPEND, MULTISEARCH,
TRANSACTION, XYSERIES, TOP, RARE, FILLNULL,
ADDINFO, APPENDCOLS, APPENDPIPE, CHART, CONVERT, EXPAND,
FIELDFORMAT, FIELDSUMMARY, FLATTEN, IPLOCATION, LOOKUP,
MAKEMV, MAKERESULTS, MVCOMBINE, MVEXPAND, NOMV, REGEX,
REPLACE, REVERSE, SPL1, TAGS, THRU, TIMEWRAP, TSTATS, MSTATS,
UNION, UNTABLE,
MATERIALIZE, VIEWS, DROPVIEW,
UNPACK_JSON, UNPACK_LOGFMT, UNPACK_SYSLOG, UNPACK_COMBINED,
UNPACK_CLF, UNPACK_NGINX_ERROR, UNPACK_CEF, UNPACK_KV,
UNPACK_DOCKER, UNPACK_REDIS, UNPACK_APACHE_ERROR,
UNPACK_POSTGRES, UNPACK_MYSQL_SLOW, UNPACK_HAPROXY,
UNPACK_LEEF, UNPACK_W3C, UNPACK_PATTERN,
JSON, UNROLL, PACK_JSON, TEE
```

Modifiers / contextual words: `BY`, `AS`, `AND`, `OR`, `NOT`, `XOR`, `IN`, `SPAN`, `LIKE`, `BETWEEN`, `IS`, `NULL`, `TRUE`, `FALSE`, `WINDOW`, `CURRENT`, `MAXSPAN`, `STARTSWITH`, `ENDSWITH`, `TYPE`, `OVER`, `LIMIT`, `CONT`, `USENULL`, `USEOTHER`, `EARLIEST`, `LATEST`, `_INDEX_EARLIEST`, `_INDEX_LATEST`, `NOW`, `TIME`, `INTO`.

**LynxFlow extension keywords:**

```
LET, KEEP, OMIT, SELECT, GROUP, COMPUTE, EVERY, BUCKET,
ORDER, TAKE, RANK, TOPBY, BOTTOMBY, BOTTOM, RUNNING,
ENRICH, PARSE, EXPLODE, PACK, LOOKUP, USING, EXTRACT,
IF_MISSING, PER, ON, INTO, ASC, DESC,
LATENCY, ERRORS, RATE, PROPORTION, IMPACT, PERCENTILES, SLOWEST,
ROLLUP, GLIMPSE, DESCRIBE, USE, OUTLIERS,
COMPARE, BASELINE, CHANGES, FACETS, EXEMPLARS,
PATTERNS, TRACE, CORRELATE, SESSIONIZE, TOPOLOGY
```

The complete list is registered in `pkg/spl2/token.go:keywords`.

---

## 4. SPL2 — canonical layer

### 4.1 Query structure

```ebnf
query          ::= (cte_def ';')* pipeline
cte_def        ::= '$' name '=' pipeline
pipeline       ::= source_stage ('|' command)*
source_stage   ::= ('FROM' | 'INDEX') source_list [time_range] [time_modifier]* [where_clause]
                 | freehand_stage          -- free-hand search, see §4.2.1
                 | command                 -- implicit source, see §4.2.2
freehand_stage ::= search_expr
```

**CTEs:** `$threats = FROM idx_alerts WHERE type = "sqli";` — a variable that can later be referenced as `FROM $threats`. Terminated by `;`. Multiple CTEs may appear in sequence.

### 4.2 Sources (FROM)

```ebnf
source_list ::= source (',' source)*
source      ::= ident | glob | negated_glob | quoted_ident | cte_ref | '*'
cte_ref     ::= '$' name
time_range  ::= '[' duration ('..' duration)? ']'
time_modifier ::= ('earliest' | 'latest' | '_index_earliest' | '_index_latest') '=' time_value
time_value  ::= duration | 'now' | 'now()' | string | number
```

Examples:
```
FROM nginx
FROM nginx, postgres, redis
FROM logs*                    -- glob
FROM 'my-logs'                -- special characters
FROM *                        -- all sources
FROM $previous_query          -- CTE
FROM nginx[-1h]               -- source + range
FROM nginx[-7d..-1d]          -- source + range
INDEX nginx                   -- exact alias for FROM nginx
INDEX logs*                   -- exact alias for FROM logs*
```

**INDEX alias:** `INDEX` in source position is an exact alias for `FROM`. It supports a single source, comma-separated sources, globs, `*`, and CTE references.

**Splunk compatibility:** `index=nginx status>=500` is normalized to `FROM nginx | search status>=500`. `index IN ("a","b")` is normalized to `FROM a, b`. `index NOT IN (...)` and `index!=x` cannot be represented as a positive source list, so they normalize to `FROM * | where _source NOT IN (...)` or `FROM * | where _source!="x"`. Lint `L003` shows the rewrite to the user.

**`source=` compatibility:** `source=<name>` is a logical field filter, not a physical source selector. It normalizes to `FROM * | where _source="<name>"`. `source IN (...)` normalizes to `FROM main | where _source IN (...)` for compatibility with the default-source pipeline behavior. This distinction is intentional: `index` selects storage scope; `source` filters event metadata.

**Splunk search-time modifiers:** SPL/SPL2 modifiers `earliest=<time>`, `latest=<time>`, `_index_earliest=<time>`, and `_index_latest=<time>` are accepted in SEARCH/source-prefix position and normalized into explicit `_time` or `_indextime` range predicates before planning.

| Input | Canonical rewrite |
|---|---|
| `index=nginx earliest=-5m latest=now` | `FROM nginx[-5m..now]` |
| `index=nginx earliest=-4h latest=-2h status=500` | `FROM nginx[-4h..-2h] | search status=500` |
| `index=nginx _index_earliest=-1h _index_latest=now` | `FROM nginx | where _indextime BETWEEN "<resolved>" AND "<resolved>"` |

`now` and `now()` are aliases in time-modifier positions. Deprecated SPL time modifiers such as `daysago`, `hoursago`, `starttime`, `endtime`, and `timeformat` are parsed only as compatibility hints and normalized to `earliest`/`latest` when lossless; otherwise the query is rejected with an explicit suggestion.

#### 4.2.1 Free-hand search

If the submitted query starts with a SEARCH expression and has no explicit source, LynxDB treats it as a free-hand search over the configured default source scope. The compatibility default is `main`; installations may configure the default as `*` for exploratory shells, but the selected scope is always shown in rewrites and explain output.

| Input | Canonical rewrite |
|---|---|
| `errors` | `FROM <default> | search errors` |
| `"connection reset"` | `FROM <default> | search "connection reset"` |
| `errors timeout` | `FROM <default> | search errors timeout` |
| `host=web-* errors` | `FROM <default> | search host=web-* errors` |
| `errors | stats count() by source` | `FROM <default> | search errors | stats count() BY source` |

This is the CLI contract for:

```
lynxdb query 'errors'
lynxdb query '"connection reset"'
```

Rules:
- The default free-hand source is explicit in `meta.rewrites`; users must not have to guess whether `main` or `*` was used.
- The rewrite is recorded in `meta.rewrites` and shown by `--show-rewritten`.
- `FROM *` uses source authorization and retention filters before planning; it means "all sources the caller can search", not all physical segments.
- If source count or estimated scanned data exceeds configured thresholds, the query still parses but returns lint `L037` and an `explain.source_scope` block with the selected source count, estimated segment count, and suggested narrowing filters.
- A free-hand query may be narrowed after the fact with field predicates (`source=api`, `_source=api`, `service=checkout`) or by rewriting it explicitly as `FROM api | search errors`.

#### 4.2.2 Implicit source for pipeline commands

If the query starts with a pipeline command that is not a SEARCH expression (for example `stats count()` or `| stats count()`), the normalizer prepends `FROM main`. Lint `L002` suggests adding an explicit `FROM`.

`search <expr>` without an explicit source is treated as free-hand search and normalizes to `FROM <default> | search <expr>`.

### 4.3 Expression contexts

SPL2 has **three** contexts with different expression grammars. This is a historical inheritance from Splunk, preserved for compatibility.

#### 4.3.1 SEARCH context

Used in the `search` command and in the implicit form after `FROM` (`FROM nginx error timeout`).

```ebnf
search_expr ::= term (search_op term)*
term        ::= bare_word              -- _raw contains substring
              | quoted_phrase          -- _raw contains exact phrase
              | glob_word              -- _raw matches glob
              | field '=' value
              | field '!=' value
              | field op value         -- <, <=, >, >=
              | field 'IN' '(' value_list ')'
              | 'NOT' term
              | '(' search_expr ')'
              | 'CASE' '(' value ')'   -- case-sensitive
              | 'TERM' '(' value ')'   -- exact token match
search_op   ::= 'AND' | 'OR' | <implicit>     -- whitespace = implicit AND
```

**Precedence (Splunk-compatible canon):** parentheses > `NOT` > `OR` > `AND`. Implicit AND has the same precedence as explicit `AND`. This is intentionally different from WHERE/EVAL contexts and matches Splunk's search-command behavior. `XOR` is not accepted in SEARCH context.

Because this precedence is surprising to users coming from SQL/KQL/boolean algebra, mixed `AND`/`OR` SEARCH expressions without parentheses must emit `L030` with the parsed shape. Installations may expose a non-default `--sql-search-precedence` feature flag for experimentation, but full SPL compatibility requires the default above.

#### 4.3.2 WHERE context

Used in the `WHERE` clause (`FROM x WHERE ...`) and in the `| where ...` command.

```ebnf
where_expr ::= comparison (logical_op comparison)*
logical_op  ::= 'AND' | 'OR' | 'XOR'
comparison ::= expr op expr
             | expr 'IN' '(' value_list ')'
             | expr 'BETWEEN' expr 'AND' expr
             | expr 'IS' ['NOT'] 'NULL'
             | expr 'LIKE' string
             | expr '=~' regex          -- regex match
             | expr '!~' regex          -- regex not match
             | 'NOT' where_expr
             | '(' where_expr ')'
             | func_call
```

Field names are unquoted if they consist of `[a-zA-Z0-9_.]`. Otherwise single quotes are required: `where 'user-id' = 42`. String values are always double-quoted: `where level = "ERROR"`.

`regex` is a double-quoted regex pattern. Slash-delimited regex literals are not part of this RFC because `/` is also the arithmetic division token in eval context. `glob_word` follows §3.7; when applied to `_raw`, the planner uses literal extraction and n-gram/trigram candidates before evaluating the full glob.

#### 4.3.3 EVAL context

Used in `eval` and inside aggregate functions `stats count(...)`.

```ebnf
eval_expr ::= coalesce
coalesce  ::= xor ('??' xor)*
xor       ::= or_expr ('XOR' or_expr)*
or_expr   ::= and_expr ('OR' and_expr)*
and_expr  ::= comparison ('AND' comparison)*
comparison::= sum (cmp_op sum)*
sum       ::= term (('+'|'-') term)*
term      ::= factor (('*'|'/'|'%') factor)*
factor    ::= '-' factor
            | '+' factor
            | 'NOT' factor
            | primary
primary   ::= number | string | f_string | ident | field_ref
            | func_call | '(' eval_expr ')'
field_ref ::= ident ('.' ident | '?.' ident)*
```

Conditional logic uses `if(cond, then, else)` and `case(...)`. C-style ternary is not part of the canonical eval grammar. Eval functions (see §4.5) are listed in `pkg/vm/compiler.go`. Function names are case-insensitive.

### 4.4 Command catalog (SPL2)

Canonical form: command after the pipe, options (`key=value`) before positional arguments, separators are commas.

#### Filters

| Command | Syntax | Description |
|---|---|---|
| `search` | `search <search_expr>` | Filter in SEARCH context. |
| `where` | `where <where_expr>` | Filter in WHERE context. |

#### Field transformation

| Command | Syntax | Description |
|---|---|---|
| `eval` | `eval <name> = <eval_expr> [, ...]` | Compute new fields. |
| `rename` | `rename <old> AS <new> [, ...]` | Rename. |
| `fields` | `fields [+\|-] <name-or-glob> [, ...]` or `fields *` | `+` (default) keep, `-` remove. |
| `table` | `table <name-or-glob> [, ...]` or `table *` | Only the listed fields, in the given order. |
| `fillnull` | `fillnull [value=<v>] [<field> [, ...]]` | Fill nulls. |
| `rex` | `rex [field=<f>] "<pattern>"` | Regex extraction from `_raw` by default. |
| `unpack_<format>` | `unpack_json [from <field>] [fields (...)] [prefix <p>] [keep_original]` | Parse known formats (JSON, logfmt, syslog, combined, CLF, nginx_error, CEF, KV, docker, redis, apache_error, postgres, mysql_slow, haproxy, leef, w3c). |
| `unpack_kv` | `unpack_kv [from <field>] [delim=<s>] [assign=<s>] [quote=<s>] ...` | KV parser with optional delimiters. |
| `unpack_w3c` | `unpack_w3c [from <field>] [header="<#Fields ...>"] ...` | W3C parser with optional header override. |
| `unpack_pattern` | `unpack_pattern "<pattern>" [from <field>] [fields (...)] [prefix <p>] [keep_original]` | User pattern extraction using `%{name}` placeholders. |
| `json` | `json [field=<field>] [path="<path>" AS alias] [paths="<p> AS a, ..."]` or `json <path> [AS alias], ...` | Lightweight JSON path extraction from `_raw` by default. |
| `unroll` | `unroll [field=]<field>` | Unroll one JSON array / multi-value field into rows. |
| `pack_json` | `pack_json [<field_list>] INTO <target>` | Combine selected fields, or all non-internal fields when omitted, into JSON. |

#### Aggregation

| Command | Syntax | Description |
|---|---|---|
| `stats` | `stats <agg_func> [AS <name>] [, ...] [BY <field_list>]` | Group and aggregate. |
| `eventstats` | `eventstats ...` (like `stats`) | Aggregation without row collapse. |
| `streamstats` | `streamstats [current=<bool>] [window=<n>] <agg_list> [BY <field_list>]` | Sliding aggregates. |
| `timechart` | `timechart [span=<dur>] <agg_list> [span=<dur>] [BY <split>]` | Time-series aggregation; output is sorted by `_time`. |
| `top` | `top [N] <field> [BY <field>]` | Top-N frequent values. |
| `rare` | `rare [N] <field> [BY <field>]` | Bottom-N frequent values. |
| `xyseries` | `xyseries <x> <y> <value>` | Transpose into a matrix. |

#### Sort and limit

| Command | Syntax | Description |
|---|---|---|
| `sort` | `sort [-\|+]<field> [, ...]` or `sort by <field> [ASC\|DESC] [, ...]` | Canon — prefix form `-field` (DESC), `+field` (ASC, default). |
| `head` | `head [<n>]` | First N (default 10). |
| `tail` | `tail [<n>]` | Last N. |
| `reverse` | `reverse` | Reverse current row order without sorting. |
| `dedup` | `dedup [N] <field_list>` | Keep one row per key by default; optional `N` keeps up to N rows per key. |

#### Joins

| Command | Syntax | Description |
|---|---|---|
| `join` | `join [type=inner\|left] <field> [<subsearch>]` | Join with a subsearch. |
| `append` | `append [<subsearch>]` | Concatenate results. |
| `multisearch` | `multisearch [<subsearch1>] [<subsearch2>] ...` | Parallel subsearches. |
| `transaction` | `transaction [maxspan=<dur>] [startswith=<cond>] [endswith=<cond>] <field_list>` | Group events into transactions. |

#### Materialized views

| Command | Syntax | Description |
|---|---|---|
| `materialize` | `materialize "<name>" [retention=<dur>] [partition_by=<field>[,<field>...]]` (last command in the pipeline) | Create an MV from the current query. |
| `from` | `from <view-or-system-table>` (pipeline command) | Read a materialized view or system table inside a pipeline. |
| `views` | `views ["name"] [retention=<dur>]` | List views, inspect one view, or alter retention. |
| `dropview` | `dropview "<name>"` | Drop an MV. |

#### Other

| Command | Syntax | Description |
|---|---|---|
| `tee` | `tee "<sink>"` | Side effect: additionally send JSON rows to a sink without interrupting the stream. |

### 4.4.1 Splunk/SPL2 compatibility surface

The native LynxDB command set is intentionally smaller than Splunk Enterprise. For SPL compatibility the parser recognizes the following additional SPL2/SPL commands and maps them into one of three buckets.

**Native or direct alias:**

| Command | Status | Canonical LynxDB form |
|---|---|---|
| `chart` | Parse and execute as grouped aggregation. | `stats <agg> BY <row>[, <column>]` plus chart metadata. |
| `fieldformat` | Parse and execute as display-only formatting metadata. | Does not mutate exported field values. |
| `regex` | Parse and execute as regex filter. | `where <field> =~ "<pattern>"` or `where _raw =~ "<pattern>"`. |
| `replace` | Parse and execute for field-value replacement. | `eval <field> = replace(<field>, ...)` when expressible. |
| `reverse` | Parse and execute. | Reverse current row order. |
| `lookup` | Parse and execute against configured datasets. | Left join/enrichment against an external dataset. |
| `mvexpand` / `expand` | Parse and execute. | `unroll <field>`; `expand` is the SPL2 spelling for array/object rows. |
| `makeresults` | Parse and execute for tests/examples. | `FROM repeat({}, count)` where available. |
| `union` | Parse and execute for compatible subsearches. | `multisearch ...` followed by schema union. |

**Parse-compatible, execution optional by deployment capability:**

| Command | Status | Notes |
|---|---|---|
| `addinfo` | Adds search metadata fields. | Requires planner to expose time range/search id. |
| `appendcols` / `appendpipe` | Result-composition commands. | Useful for Splunk imports; may be disabled under streaming-only profiles. |
| `convert` | Type/format conversion. | Prefer explicit `eval to*()` functions in canonical examples. |
| `fieldsummary` | Field profiling. | Equivalent to `describe`/`glimpse` style metadata work. |
| `flatten` | Object-field expansion. | Equivalent to first-level JSON/object unpacking. |
| `iplocation` | Geo-IP enrichment. | Requires a configured GeoIP database; otherwise capability error. |
| `makemv`, `mvcombine`, `nomv` | Multivalue field reshaping. | Backed by multivalue eval/runtime support. |
| `tags`, `typer` | Knowledge-object metadata. | Capability error unless tag/eventtype stores are configured. |
| `thru` | Writeable dataset side effect. | Equivalent in spirit to `tee`, but mutates a named dataset. |
| `timewrap` | Time-series comparison. | Prefer LynxFlow `compare previous <dur>` for new queries. |
| `untable` | Inverse of `xyseries`. | Useful for chart/table reshaping. |
| `tstats` / `mstats` | Indexed/metric acceleration. | Accepted only if LynxDB exposes an accelerated/metric profile. |

**Unsupported Splunk commands with hints:** destructive/admin/search-head commands (`delete`, `collect`, `stash`, `sendemail`, `sendalert`, `localop`, `redistribute`, `loadjob`, `savedsearch`) are not part of the query language profile. The parser should reject them with `L021` and a specific reason.

**Embedded SPL:** Splunk SPL2 has a `spl1` command for embedded SPL. LynxDB does not execute arbitrary embedded SPL; `spl1 "<text>"` may be parsed only to produce compatibility diagnostics and suggested native rewrites.

### 4.5 Function catalog

Functions are available in `eval`, `where`, `from ... WHERE`, and in `stats`/`eventstats`/`streamstats`/`timechart` aggregate contexts where appropriate. Names are case-insensitive.

#### Aggregate / statistical and charting functions
`avg`, `count`, `distinct_count` / `dc`, `estdc`, `estdc_error`, `first`, `last`, `list`, `max`, `mean`, `median`, `min`, `mode`, `range`, `stdev`, `stdevp`, `sum`, `sumsq`, `var`, `varp`, `values`.

Time/statistical functions: `earliest`, `earliest_time`, `latest`, `latest_time`, `per_second`, `per_minute`, `per_hour`, `per_day`, `rate`, `span`, `sparkline`.

Percentiles:
- Generic SPL2 form: `perc(field, percentile)`, `percentile(field, percentile)`, `exactperc(field, percentile)`, `upperperc(field, percentile)`.
- SPL-style suffix form: `perc95(field)`, `percentile95(field)`, `exactperc95(field)`, `upperperc95(field)`.
- LynxDB aliases: `p50`, `p75`, `p90`, `p95`, and `p99` parse as `perc50`, `perc75`, `perc90`, `perc95`, and `perc99` in aggregate shorthand.

Multivalue/array aggregations: `dataset()`, `pivot(key, value)`, `list(value)`, `values(value)`.

No-argument aggregations such as `count` may be written without parentheses for Splunk compatibility; canonical examples use `count()`.

> Conditional aggregates use `eval(...)` as the argument: `stats count(eval(method="GET")) AS gets, count(eval(method="POST")) AS posts BY host`.

#### Conditional and comparison
`if(cond, then, else)`, `case(cond1, val1, cond2, val2, ..., true, default)`, `validate(cond1, err1, ...)`, `coalesce(a, b, c, ...)`, `null()`, `nullif(a, b)`, `in(value, ...)`, `searchmatch(search_str)`, `match(str, "regex")`, `like(str, "pattern")`, `cidrmatch("subnet/mask", ip)`.

Informational predicates: `isnull`, `isnotnull`, `isnum`, `isnumeric`, `isint`, `isstr`, `isbool`, `isarray`, `isobject`, `typeof`.

LynxDB-only convenience predicates: `ilike(x, "pattern")`, `startswith(x, prefix)`, `endswith(x, suffix)`, `contains(x, substring)`.

#### Conversion
`tonumber(str[, base])`, `tostring(value[, format])`, `toint(value[, base])`, `todouble(value[, base])`, `tobool(value)`, `toarray(value)`, `tomv(value)`, `toobject(value)`, `tojson([internal_fields])`, `printf(format, ...)`, `ipmask(mask, ip)`.

#### Numeric, trig, and random
`abs`, `ceil`, `ceiling`, `floor`, `round(x[, digits])`, `sigfig`, `ln`, `log(x[, base])`, `exp`, `pow`, `sqrt`, `pi`, `random`, `max(a, b, ...)`, `min(a, b, ...)`.

Trig/hyperbolic: `acos`, `acosh`, `asin`, `asinh`, `atan`, `atan2`, `atanh`, `cos`, `cosh`, `hypot`, `sin`, `sinh`, `tan`, `tanh`.

#### String and regex
`upper`, `lower`, `len`, `substr(s, start[, len])`, `replace(s, re, with)`, `trim(s[, chars])`, `ltrim(s[, chars])`, `rtrim(s[, chars])`, `urldecode(url)`, `spath(value, path)`.

#### Multivalue
`split(s, sep)`, `mvappend(...)`, `mvcount(mv)`, `mvdedup(mv)`, `mvfilter(predicate)`, `mvfind(mv, regex)`, `mvindex(mv, start[, end])`, `mvjoin(mv, sep)`, `mvmap(mv, expr)`, `mvrange(start, end[, step])`, `mvsort(mv)`, `mvzip(left, right[, sep])`, `mv_to_json_array(mv[, infer_types])`, `json_array_to_mv(json[, infer_types])`.

#### Time
`now()`, `time()`, `relative_time(epoch, "specifier")`, `strftime(epoch, "fmt")`, `strptime(str, "fmt")`.

`now` without parentheses is accepted only in time-modifier positions such as `earliest=now`; eval context uses `now()`.

#### Cryptographic
`md5(str)`, `sha1(str)`, `sha256(str)`, `sha512(str)`.

#### JSON / structured data
`json_extract(s, "$.path")`, `json_extract_exact(s, key, ...)`, `json_valid(s)`, `json_keys(s[, "$.path"])`, `json_array_length(s[, "$.path"])`, `json_object(k1, v1, ...)`, `json_array(...)`, `json_type(s[, "$.path"])`, `json_set(s, "$.path", value)`, `json_set_exact(s, key, value, ...)`, `json_append(s, "$.path", value, ...)`, `json_extend(s, "$.path", value, ...)`, `json_delete(s, key, ...)`, `json_remove(s, "$.path")`, `json_merge(a, b)`.

SPL2 JSON higher-order functions `all`, `filter`, `map`, and `reduce` are reserved for compatibility. They require lambda syntax support and are optional until lambda grammar is specified.

Full execution catalog — `pkg/vm/compiler.go`. Parse-time hint catalog — `pkg/spl2/error_hints.go`.

---

## 5. LynxFlow — sugar layer

### 5.1 Contract

LynxFlow is a set of additive keywords and forms, each of which has exactly one SPL2 expansion. Expansion happens inside the normalizer (`pkg/spl2/normalize.go`) before the optimization stage. After normalization the AST contains only SPL2 nodes.

Properties:
1. **Pure sugar.** LynxFlow adds no computational power. Anything expressed in LynxFlow can be expressed in SPL2.
2. **Stable desugaring.** The LynxFlow → SPL2 mapping does not change between minor releases without deprecation.
3. **Mixing allowed.** A single pipeline can interleave SPL2 and LynxFlow: `from x | where status >= 500 | group by service compute count() | sort -count`.
4. **Bidirectional lint.** When parsing an SPL2 query, lint suggests a LynxFlow equivalent if it is shorter. When parsing LynxFlow, lint can show the SPL2 equivalent on request (`--show-rewritten`).

### 5.2 Sources and time shortcuts

| LynxFlow | SPL2 equivalent | What it improves |
|---|---|---|
| `from x` | `FROM x` | Lowercase canonical examples are accepted because keywords are case-insensitive. |
| `index x` | `FROM x` | Splunk muscle memory without changing AST semantics. |
| `from x[-1h]` | `FROM x[-1h]` | Compact CLI time range. |
| `from x[-7d..-1d]` | `FROM x[-7d..-1d]` | Compact bounded relative range. |
| `where _time >= -1h` | `where _time >= "<resolved RFC3339 time>"` | Relative time in expression context is normalized before parse. |
| `where _time BETWEEN -7d AND -1d` | `where _time BETWEEN "<resolved>" AND "<resolved>"` | Relative bounded range in expression context. |

Phrase forms such as `from x last 1h`, `today`, or `yesterday` are not separate grammar forms in this RFC. The canonical compact source-time form is `FROM x[-1h]`; UIs may offer phrase templates that insert the canonical form.

### 5.3 Projection: `select`

| LynxFlow | SPL2 equivalent |
|---|---|
| `select host, status` | `table host, status` |
| `select host AS h, status` | `table host, status \| rename host AS h` |
| `select *` | `table *` |

`select` is a pipeline projection command with optional inline aliases. Top-level SQL-style `SELECT ... FROM ... WHERE ...` is not part of this RFC; adding it would introduce a third query shape and should live in a separate compatibility RFC.

### 5.4 Filters

| LynxFlow | SPL2 equivalent | What it improves |
|---|---|---|
| `from x \| status >= 500` (no `where`) | `from x \| where status >= 500` | Implicit WHERE for a single predicate. |
| `from x \| errors` | `from x \| where lower(level) IN ("error","fatal") \| stats count()` | Conservative common error view. |
| `from x \| errors by service` | `from x \| where lower(level) IN ("error","fatal") \| stats count() BY service` | Shortcut for the typical "errors by something" pattern. |
| `from x \| errors by service compute count() AS n, dc(user_id) AS users` | `from x \| where lower(level) IN ("error","fatal") \| stats count() AS n, dc(user_id) AS users BY service` | Keeps the filter compact while leaving aggregation explicit. |

`errors` intentionally does not infer service-specific concepts such as HTTP status, severity, exception class, or custom boolean flags. Those predicates are data-model decisions and should remain visible in the query:

```
from x
| where status >= 500 OR lower(severity) IN ("high", "critical")
| group by service compute count() AS n
```

This keeps the shortcut predictable across mixed logs. Applications may offer saved templates for richer local error definitions, but the language primitive stays conservative.

### 5.5 Aggregations

#### `group by ... compute`

| LynxFlow | SPL2 |
|---|---|
| `group by service compute count()` | `stats count() BY service` |
| `group by service, region compute count() AS n, avg(dur) AS avg_dur` | `stats count() AS n, avg(dur) AS avg_dur BY service, region` |
| `group compute count()` (no `by`) | `stats count()` |

#### `every` (time bucketing)

| LynxFlow | SPL2 |
|---|---|
| `every 5m compute count()` | `bin _time span=5m \| stats count() BY _time` |
| `every 5m by service compute count()` | `bin _time span=5m \| stats count() BY service, _time` |
| `bucket _time span=5m AS minute` | `bin _time span=5m AS minute` |

`every` always uses `compute`; this avoids ambiguity with a bare pipeline expression after a duration. `bucket` is the single-field form for explicit binning and may be used outside time-series aggregation.

#### `latency`

`latency` requires the metric field. Autocomplete may suggest common field names (`duration_ms`, `duration`, `dur`, `latency`), but execution does not guess.

| LynxFlow | SPL2 |
|---|---|
| `latency duration_ms every 1m` | `timechart span=1m perc50(duration_ms) AS p50, perc95(duration_ms) AS p95, perc99(duration_ms) AS p99, count() AS count` |
| `latency duration_ms every 1m by service compute p50, p99, avg` | `timechart span=1m perc50(duration_ms) AS p50, perc99(duration_ms) AS p99, avg(duration_ms) AS avg BY service` |

Default latency aggregates are `p50`, `p95`, `p99`, and `count`. `compute` overrides the aggregate list.

#### `slowest`

| LynxFlow | SPL2 |
|---|---|
| `slowest 10 by duration_ms` | `sort -duration_ms \| head 10` |
| `slowest 10 endpoint by duration_ms` | `stats max(duration_ms) AS max_duration_ms BY endpoint \| sort -max_duration_ms \| head 10` |
| `slowest endpoint by duration_ms` | `stats max(duration_ms) AS max_duration_ms BY endpoint \| sort -max_duration_ms \| head 10` |

When the metric field is omitted, `duration_ms` is used and lint `L036` recommends making the field explicit. There is no `fastest` command in this RFC; use `sort +<field> | head N` or `bottomby`.

#### `topby` / `bottomby`

| LynxFlow | SPL2 |
|---|---|
| `topby 20 sku using avg(dur) compute count() AS n` | `stats avg(dur) AS avg_dur, count() AS n BY sku \| sort -avg_dur \| head 20` |
| `bottomby 5 host using max(latency)` | `stats max(latency) AS max_latency BY host \| sort +max_latency \| head 5` |
| `top 10 endpoint by p99(duration_ms)` | `stats perc99(duration_ms) AS p99_duration_ms BY endpoint \| sort -p99_duration_ms \| head 10` |
| `bottom 10 endpoint by avg(duration_ms)` | `stats avg(duration_ms) AS avg_duration_ms BY endpoint \| sort +avg_duration_ms \| head 10` |

Plain `top [N] field` and `rare [N] field` remain frequency commands. Metric ranking uses `topby`, `bottomby`, or `top/bottom N field by <agg>`.

#### `rank`

| LynxFlow | SPL2 |
|---|---|
| `rank top 5 by count` | `sort -count \| head 5` |
| `rank bottom 5 by latency` | `sort +latency \| head 5` |

#### `rate`

| LynxFlow | SPL2 |
|---|---|
| `rate` | `timechart span=1m count() AS rate` |
| `rate per 5m` | `timechart span=5m count() AS rate` |
| `rate per 1m by service` | `timechart span=1m count() AS rate BY service` |

`rate` returns counts per bucket. It does not divide by seconds or minutes implicitly; if a per-second normalized value is required, write the `eval` explicitly after aggregation.

#### `proportion`

`proportion` computes "matching events divided by all events" for the current stream. It is the canonical LynxFlow form for error rates, timeout rates, retry rates, and other investigation ratios where the denominator must remain visible.

| LynxFlow | SPL2 |
|---|---|
| `proportion status >= 500 AS error_rate` | `stats count(eval(status >= 500)) AS error_rate_num, count() AS error_rate_den \| eval error_rate = error_rate_num / error_rate_den` |
| `proportion level = "error" AS error_rate by service` | `stats count(eval(level = "error")) AS error_rate_num, count() AS error_rate_den BY service \| eval error_rate = error_rate_num / error_rate_den` |
| `proportion status >= 500 AS error_rate every 5m by service` | `bin _time span=5m \| stats count(eval(status >= 500)) AS error_rate_num, count() AS error_rate_den BY service, _time \| eval error_rate = error_rate_num / error_rate_den` |

The alias after `AS` is required. This prevents generic output names like `rate` from colliding with the `rate` command. Division by zero yields `null()`.

#### `percentiles`

| LynxFlow | SPL2 |
|---|---|
| `percentiles duration_ms` | `stats perc50(duration_ms) AS p50_duration_ms, perc75(duration_ms) AS p75_duration_ms, perc90(duration_ms) AS p90_duration_ms, perc95(duration_ms) AS p95_duration_ms, perc99(duration_ms) AS p99_duration_ms` |
| `percentiles duration_ms by service` | same aggregates `BY service` |

### 5.6 Output

| LynxFlow | SPL2 |
|---|---|
| `take 10` | `head 10` |
| `order by errors desc` | `sort -errors` |
| `order by errors, ts desc` | `sort errors, -ts` |
| `keep host, status` | `fields + host, status` |
| `omit _raw, _internal` | `fields - _raw, _internal` |
| `select host AS h, status` | `table host, status \| rename host AS h` |
| `pack field1, field2 into payload` | `pack_json field1, field2 INTO payload` |
| `pack into payload` | `pack_json INTO payload` |

Use `dedup` directly for uniqueness. The `unique` alias is not part of this RFC because `dedup [N] <fields>` is already compact and matches SPL muscle memory.

### 5.7 Parsing and enrichment

| LynxFlow | SPL2 |
|---|---|
| `parse json(_raw)` | `unpack_json from _raw` |
| `parse logfmt(_raw) as log` | `unpack_logfmt from _raw prefix log.` |
| `parse combined(_raw) extract (status, uri)` | `unpack_combined from _raw fields (status, uri)` |
| `parse regex(_raw, "status=(?<status>\\d+)")` | `rex field=_raw "status=(?<status>\\d+)"` |
| `parse pattern(_raw, "%{method} %{path} %{status:int}")` | `unpack_pattern "%{method} %{path} %{status:int}" from _raw` |
| `json "$.user.id" AS user_id` | `json field=_raw path="$.user.id" AS user_id` |
| `explode tags` | `unroll tags` |
| `explode tags AS tag` | `unroll tags` with output field `tag` |
| `lookup users on user_id` | `join type=left user_id [from users]` |
| `enrich count() AS service_events by service` | `eventstats count() AS service_events BY service` |

Supported parse formats are `json`, `logfmt`, `syslog`, `combined`, `clf`, `nginx_error`, `cef`, `kv`, `docker`, `redis`, `apache_error`, `postgres`, `mysql_slow`, `haproxy`, `leef`, and `w3c`. The same formats may be used as pipe shortcuts (`| json(_raw)`, `| logfmt(_raw)`, `| pattern(_raw, "...")`) when the token is clearly a command call.

Pattern placeholders use `%{name}` with optional types: `%{name:int}`, `%{name:float}`, `%{name:timestamp}`, and `%{name:rest}`.

`enrich` is event-level aggregation sugar. It is not a geo-IP, threat-intel, or arbitrary external lookup primitive. External dataset joins use `lookup`.

### 5.8 Analysis (advanced helpers)

These helpers are pipeline commands with explicit runtime behavior. They do not alter the expression grammar.

| Command | Description |
|---|---|
| `running sum(bytes) by host` | `streamstats sum(bytes) BY host`. |
| `glimpse [N]` | Emits a compact field/value summary over up to `N` rows from the current stream. |
| `describe` | Emits schema/source metadata for the current stream or selected source. |
| `use <fragment>` / `use @namespace/name` | Expands a named pipeline fragment at parse/normalization time. Missing fragments are explicit errors. |
| `facets service, host [limit=10]` | Emits top values and counts per requested field, with `_facet`, `_value`, and `count` columns. |
| `impact [count()\|sum(field)\|avg(field)] by field[, ...]` | Adds total and percentage contribution columns, then sorts by descending contribution. |
| `baseline field window=N [by field[, ...]]` | Adds rolling baseline, delta, and z-score columns from previous rows. |
| `changes field [by field[, ...]]` | Keeps rows where a field changed relative to the previous row in the same group. |
| `exemplars [N] [by field[, ...]]` | Keeps newest representative rows, globally or per group. |
| `outliers field=latency [method=iqr\|zscore\|mad] [threshold=N]` | Marks statistical outliers using the selected method. |
| `patterns [field=_raw] [max_templates=N] [similarity=F]` | Groups similar messages into templates. |
| `trace [trace_id=<field>] [span_id=<field>] [parent_id=<field>]` | Builds a span tree from existing trace/span fields and adds depth/tree fields. |
| `rollup 1m, 1h [by service]` | Produces multiple time resolutions in one stream and adds `_resolution`. |
| `correlate latency errors [method=pearson\|spearman]` | Computes correlation between two numeric fields. |
| `sessionize [maxpause=30m] [by user_id, session_key]` | Adds session id/start/end fields based on time gaps within each group. |
| `topology [source_field=service] [dest_field=downstream] [weight_field=count] [max_nodes=N]` | Builds edge/node summaries from source/destination fields. |
| `compare [previous] 1h` | Re-runs the pipeline prefix over the previous time window and adds `previous_<field>` and `change_<field>` numeric columns. |

Deterministic expansions for investigation helpers:

| LynxFlow | SPL2 expansion |
|---|---|
| `impact by service` | `stats count() AS n BY service \| eventstats sum(n) AS total_n \| eval pct_n = n / total_n \| sort -pct_n` |
| `impact sum(bytes) by host` | `stats sum(bytes) AS sum_bytes BY host \| eventstats sum(sum_bytes) AS total_sum_bytes \| eval pct_sum_bytes = sum_bytes / total_sum_bytes \| sort -pct_sum_bytes` |
| `baseline p95 window=12 by service` | `streamstats current=false window=12 avg(p95) AS baseline_p95, stdev(p95) AS stdev_p95 BY service \| eval delta_p95 = p95 - baseline_p95, z_p95 = if(stdev_p95 > 0, delta_p95 / stdev_p95, null())` |
| `changes version by service` | `sort +_time \| streamstats current=false last(version) AS previous_version BY service \| where isnotnull(previous_version) AND version != previous_version` |
| `exemplars 3 by service` | `sort -_time \| dedup 3 service` |

`facets` is a fan-out suffix like `compare`: the normalizer applies the pipeline prefix once per requested field and combines the per-field outputs with `multisearch`. For example:

```
from nginx[-1h]
| where status >= 500
| facets service, host limit=5
```

normalizes as:

```
multisearch
  [from nginx[-1h] | where status >= 500 | stats count() AS count BY service | sort -count | head 5 | eval _facet = "service", _value = service | table _facet, _value, count]
  [from nginx[-1h] | where status >= 500 | stats count() AS count BY host | sort -count | head 5 | eval _facet = "host", _value = host | table _facet, _value, count]
```

These helpers are intentionally mechanical. `facets` finds high-cardinality slices, `impact` quantifies blast radius, `baseline` exposes change against recent history, `changes` surfaces deployments/config flips, and `exemplars` preserves raw rows for inspection. None of them claims root cause; they only reshape evidence.

`compare` is intentionally a pipeline suffix:

```
from nginx[-1h]
| where status >= 500
| group by service compute count() AS n
| compare previous 1h
```

The prefix before `compare` defines the current window. The command shifts that same prefix backward by the requested duration for the previous window. This avoids a second query shape such as `compare last 1h to previous 1h | ...` and keeps filters/aggregations in normal pipeline order.

### 5.9 Binding to expression contexts

LynxFlow sugar commands reuse the same context set:
- In implicit WHERE (`from x | status >= 500`) — WHERE context.
- In `compute <agg_func>` — eval/stats context.
- In `parse "<pattern>"` — pattern context (same as rex).

LynxFlow does not introduce a fourth expression syntax.

---

## 6. Error model

### 6.1 Message categories

| Category | When | Behavior |
|---|---|---|
| **Parse error** | Syntax does not parse | Query rejected. Response carries `code`, `message`, `position`, `suggestion`. |
| **Lint warning** | Parsing succeeded but the query looks suspicious or has a shorter LynxFlow form | Query executes. `meta.lints` in the response and `Hints:` block in TUI. |
| **Compat hint** | SPL1 style recognized | Query executes (with silent rewrite). `meta.rewrites` shows the expansion. |
| **Runtime error** | Parsing succeeded, execution failed | Standard error. Out of scope for this RFC. |

### 6.2 Lint code catalog

| Code | Condition | Message |
|---|---|---|
| `L001` | Leading wildcard in search/like (`*x*`) | "Leading wildcard slows the query; consider an anchor" |
| `L002` | Pipeline command without explicit `FROM` | "Default source `main` is used; add `FROM` for clarity" |
| `L003` | `index=` SPL1-style rewritten | "`index=X` → `FROM X`; explicit form recommended" |
| `L004` | `stats count` without `BY` on a wide range | "Without `BY` returns a single value; maybe you want `BY <field>`" |
| `L005` | `_raw = "x"` (exact compare on raw) | "For substring search use `_raw LIKE \"%x%\"` or `search \"x\"`" |
| `L010` | Command option after positional arguments | "Options (`key=value`) must precede positional arguments" |
| `L011` | Ambiguous `dedup` arguments | "Canon: `dedup [N] <field>[, <field>...]`" |
| `L012` | Field name in double quotes | "Canon: single quotes `'my-field'` for names with special characters" |
| `L013` | `count` without `()` | "`count` is a function; use `count()`" |
| `L020` | LynxFlow shortcut available | "Equivalent: `<lynxflow form>` (shorter by N tokens)" |
| `L021` | SPL1 command unsupported | "`<cmd>` is unsupported; see `<spl2 alternative>`" |
| `L022` | Deprecated sort syntax `field DESC` used | "Canon: `sort -field`" |
| `L030` | Mixed `AND`/`OR` without parentheses in SEARCH context | "This parses as `<canonical form>`; add parentheses to make it explicit" |
| `L031` | Boolean nesting depth > 5 in SEARCH context | "Deep nesting is hard to read; consider CTEs or split into stages" |
| `L032` | Query scans all sources and source count is high | "Narrow the source with `FROM <source>` or `source=<name>`" |
| `L033` | Unquoted value contains operator characters that may be tokenized unexpectedly | "Use double quotes for literal values containing spaces or operators" |
| `L034` | Field name matches a reserved word | "Use single quotes: `'<field>'`" |
| `L035` | Empty or tautological search (`search *`) over a wide time range | "This scans everything; add a time range, source, or predicate" |
| `L036` | LynxFlow shortcut used a documented default field, for example `slowest` using `duration_ms` | "Default field `<field>` used; specify it explicitly for clarity" |
| `L037` | Free-hand or `FROM *` search selects many sources or segments | "Broad search over `<n>` sources; narrow with `FROM`, `source=`, or a time range" |
| `L038` | Regex/glob has no extractable literal prefix or n-gram | "Pattern cannot be prefiltered efficiently; add a literal anchor if possible" |
| `L039` | PCRE2-only regex feature requested | "This requires `--regex-engine=pcre2` and may be slower" |

Lint checks live in `pkg/spl2/lints.go` and run after a successful parse. They can be disabled via `--no-lint` (CLI) or `lint: false` (REST body).

### 6.3 Rewrite transparency

Each normalizing transformation records a `Rewrite{Before, After, Reason}` entry. Visible via:
- TUI: "Rewritten as" block below the input.
- REST: `meta.rewrites` in the response.
- CLI flag `--show-rewritten`.

### 6.4 UX contract: safe assistance

The UX layer is intentionally outside the execution semantics. It can help users write and understand queries, but it cannot change what runs without an explicit user action.

Required behavior:
- **Same grammar source.** Syntax highlighting, autocomplete, formatter, lint, and examples use the same keyword/function/command catalog as the parser (`pkg/spl2/token.go`, `pkg/spl2/parser.go`, `pkg/spl2/compat_hints.go`). No duplicate UI-only grammar.
- **Preview before execution.** If a query is normalized (`index=`, free-hand `FROM <default>`, implicit `FROM main`, LynxFlow desugaring), the UI may show the canonical SPL2 form before or after execution, but the submitted text is still recorded exactly as typed.
- **No invisible autocorrect.** Unknown commands/functions may get fuzzy suggestions, but they are not automatically replaced.
- **Explainable suggestions.** Every lint or autocomplete template carries a reason (`slow`, `compat`, `canon`, `shortcut`, `schema`) so advanced users can decide whether to ignore it.
- **Stable output contract.** REST responses expose assistance under `meta.lints`, `meta.rewrites`, `meta.suggestions`, and `meta.explain`; existing result rows and error shapes are unchanged.
- **Disable switches.** CLI/TUI/REST support disabling lint and suggestions independently from parsing and execution.

### 6.5 Autocomplete and command palette

Autocomplete is context-aware but non-semantic:
- At query start: free-hand search examples first, then `FROM`, commands, and LynxFlow shortcuts.
- After `|`: commands and LynxFlow shortcuts.
- After `FROM`: known sources, then source globs, then negated source-glob examples (`!logs-debug*`).
- In field contexts (`where`, `by`, `stats`, `table`, `sort`, `keep`, `omit`, `dedup`, `join on`): known fields plus built-ins (`_time`, `_raw`, `_source`).
- After `field=`: top observed values for that field, escaped as string literals when needed.
- After `field=~`: double-quoted regex snippets with clear engine labels (`linear`, `requires pcre2`).
- In function contexts: aggregate functions for `stats`/`timechart`, eval functions for `eval`/`where`.
- In time positions: canonical bracket ranges and relative `_time` predicates. Natural phrases (`last 15m`, `today`, `yesterday`) may be shown as templates that insert canonical query text.

Autocomplete selection inserts text only on explicit accept (`Tab`, `Enter`, or click). It never changes already typed tokens merely because a better candidate exists. Suggestions must be ranked as: exact prefix matches, schema-derived names, built-ins, then templates.

The TUI command palette may expose saved query templates such as "errors by service last 1h" or "p99 latency by endpoint", but templates are inserted into the editor first and executed only after normal submit.

### 6.6 Inline diagnostics and recovery

Parse errors return a structured diagnostic:

```json
{
  "code": "E_PARSE",
  "message": "expected )",
  "position": {"start": 42, "end": 43},
  "expected": [")", "AND", "OR"],
  "suggestion": "Missing closing parenthesis"
}
```

For SEARCH context, diagnostics should include the parsed boolean shape when parsing succeeds:

```
error timeout OR status=500
=> error AND (timeout OR status=500)
```

This makes implicit `AND` and Splunk-compatible SEARCH precedence visible without changing behavior. `L030` is mandatory for mixed `AND`/`OR` queries without parentheses.

### 6.7 Results feedback and empty states

Execution feedback is advisory and must not affect result sets:
- Show result count, scanned row count, time range, selected sources, and whether a rewrite was used.
- Highlight matched terms in `_raw` when a SEARCH expression contributed literal terms. Highlighting is presentation-only.
- For zero results, provide next actions based on observed facts: broaden time range, remove a source filter, inspect available fields, or show the canonical rewritten query. Do not silently broaden the query.
- For slow or broad queries, show the most specific applicable lint first. Avoid flooding: at most five lints per query by default, sorted by severity and position.

### 6.8 Compatibility rollout requirements

Any parser or normalizer change that can affect existing text must satisfy all of these before release:
- Existing parser/search/e2e tests pass with new behavior.
- The `pkg/sigmaqueries/testdata/golden/*.spl2` corpus parses and produces the same AST after normalization, except for explicitly approved RFC changes.
- Saved-query fixtures cover old and new spellings for sort, dedup, quotes, implicit `FROM`, `index=`, `stats count`, and SEARCH precedence.
- A feature flag exists for behavior changes that can alter result sets. The flag must be documented with a removal release.
- If production telemetry is available, run old and new parse/normalize paths in shadow mode and compare at least: parse success, normalized query string, selected sources, and result count for a small sample.

The goal is to make UX improvements observable before they are user-visible, not to create a second runtime.

### 6.9 Query limits for UX and safety

Limits are part of diagnostics, not syntax:
- Maximum boolean nesting depth for SEARCH assistance: 5 before `L031`; parser may accept deeper expressions.
- Maximum autocomplete candidates shown: 20, grouped by kind.
- Maximum lints returned by default: 5; REST can request full lint output.
- Maximum rewritten-query preview length: 4 KiB in TUI/CLI; REST returns the full value unless the API layer has a response limit.
- Generated UI templates must stay bounded: no unbounded `OR` expansion from field values, source lists, or saved filters.
- Maximum regex pattern length defaults to 1 KiB. Larger values require an explicit runtime config increase.
- Maximum expanded source glob candidates defaults to 10,000. The planner stores the source-selector AST and must not stringify it into a giant `OR`.
- Global free-hand search must have a query budget: source count, segment count, scanned bytes estimate, regex compile memory, and wall-clock timeout are reported in `meta.explain`.

### 6.10 Planning contract for global and pattern search

The implementation details of storage and execution are out of scope, but the user-visible performance contract is in scope. A conforming implementation must plan free-hand, glob, and regex queries with these rules:

- **Source selection first.** Resolve `FROM`, source globs, authorization, retention, and time ranges before evaluating row predicates. `FROM *` is a source-selector node, not an expanded query string.
- **Cheap predicates before expensive predicates.** Push down source, time, equality, prefix, and field-existence filters before `_raw` substring, glob, and regex evaluation.
- **Literal extraction.** For `_raw` substring, glob, and regex, extract required literals or n-grams when possible and use the inverted index to produce candidates before running the matcher.
- **Segment skipping.** Use per-segment min/max time, source id, field dictionaries, term dictionaries, and bloom/roaring-style membership metadata to skip segments that cannot match.
- **Linear regex by default.** Default regex execution must avoid catastrophic backtracking. PCRE2-compatible features are optional and require explicit opt-in plus compile/runtime budgets.
- **Parallel fan-out with bounded merge.** Global search may fan out across sources/segments in parallel, but merge must respect `head`, `sort`, time ordering, and cancellation.
- **Explainability.** `meta.explain` must show selected sources, skipped segments, candidate row count when known, regex engine, and whether literal extraction was used.
- **No silent broadening.** If a scoped query returns zero results, UX may suggest `FROM *` or a broader time range, but execution never broadens the query automatically.

Recommended default ranking for free-hand results is newest-first for raw event output. If a relevance score exists, it may be exposed as `_score`, but it must not reorder explicit `sort` or time-series commands.

---

## 7. Splunk SPL1 compatibility

SPL1 commands that **parse**:
- `index=X` (rewritten to `FROM X` plus search).
- `index X` and `index IN (a,b)` as source selectors.
- `source=X` and `source IN (...)` as logical `_source` filters.
- `sourcetype=Y` (mapped to `_sourcetype=Y` in search context).
- `earliest=...`, `latest=...`, `_index_earliest=...`, `_index_latest=...` as search-time modifiers.
- `... | head N`, `tail N` (no parentheses) — positional form.
- `... | stats count` (no `()`) — with lint `L013`.
- `transaction maxspan=<dur> startswith=<cond> endswith=<cond> <fields>` — supported command.
- `chart`, `fieldformat`, `regex`, `replace`, `reverse`, `lookup`, `mvexpand`, `makeresults`, `appendcols`, `appendpipe`, `makemv`, `mvcombine`, `nomv`, `untable`, and `timewrap` — parsed according to §4.4.1.
- `eval` and `where` boolean `XOR` — accepted in WHERE/EVAL contexts. `XOR` remains invalid in SEARCH context.
- SPL1 percentile suffixes (`perc95(field)`, `exactperc95(field)`, `upperperc95(field)`) — normalized to the generic percentile family.

SPL1 commands that **are not supported** (with explicit suggestion):
- `inputlookup`, `outputlookup` — no Splunk-style lookup-table I/O; suggestion: use `lookup` for left joins against configured datasets or `join` against a pipeline source.
- `tstats`, `mstats` — no TSIDX/metric profile unless explicitly enabled; suggestion: `stats` works on event data.
- `map` — none; suggestion: `join` or `correlate`.
- `spath` command — no SPL1 command form; suggestion: `json "$.path" AS field`, `unpack_json`, or `eval field = spath(_raw, "$.path")`.
- `spl1` embedded SPL — no arbitrary embedded SPL execution; suggestion: convert to native SPL2/LynxFlow.

Full list and hints — `pkg/spl2/compat_hints.go`.

---

## 8. Full list of reserved words

See also §3.11. Canonical source: `pkg/spl2/token.go:keywords`.

**Do not use these words as bare identifiers.** When needed, wrap them in single quotes: `where 'sort' = 1`.

```
-- SPL2 commands and modifiers
addinfo and append appendcols appendpipe as asc between bin branch by chart
cont convert dedup desc dropview earliest eval eventstats expand false
fieldformat fields fieldsummary fillnull flatten from head in index into
iplocation is join json latest like lookup makemv makeresults materialize
multisearch mvcombine mvexpand nomv not null or over pack_json rare regex
rename replace reverse rex search sort span spl1 stats streamstats tags tail
table tee thru time timechart timewrap top transaction true tstats mstats typer
union untable unpack_apache_error unpack_cef
unpack_clf unpack_combined unpack_docker unpack_haproxy unpack_json unpack_kv
unpack_leef unpack_logfmt unpack_mysql_slow unpack_nginx_error unpack_pattern
unpack_postgres unpack_redis unpack_syslog unpack_w3c unroll useother usenull
views where xor xyseries _index_earliest _index_latest

-- LynxFlow
baseline bottom bottomby bucket changes compare compute correlate describe
enrich errors every exemplars explode extract facets glimpse group if_missing
impact into keep latency let lookup omit on order outliers pack parse patterns
per percentiles proportion rank rate rollup running select sessionize slowest
take topby topology trace use using
```

Total: ~135 keywords. This is the budget; future expansion happens via RFC amends.

---

## 9. Grammar (simplified EBNF)

```ebnf
program     ::= (cte ';')* pipeline EOF
cte         ::= '$' name '=' pipeline
pipeline    ::= source ('|' command)*
              | freehand_stage ('|' command)*
              | command_no_source ('|' command)*

source      ::= ('FROM' | 'INDEX') source_list time_range? time_modifier* where_clause?
              | spl1_source_prefix      -- 'index=x ...'
source_list ::= source_atom (',' source_atom)*
source_atom ::= ident | quoted | glob | negated_glob | '*' | '$' name
time_range  ::= '[' duration ('..' duration)? ']'
time_modifier ::= ('earliest' | 'latest' | '_index_earliest' | '_index_latest') '=' time_value
time_value  ::= duration | 'now' | 'now()' | string | number
where_clause::= 'WHERE' where_expr
freehand_stage ::= search_expr          -- normalized to FROM <default> | search ...
glob        ::= glob_token | string
negated_glob::= '!' glob
regex       ::= string

command     ::= filter_cmd
              | transform_cmd
              | aggregate_cmd
              | sort_limit_cmd
              | join_cmd
              | compatibility_cmd
              | mv_cmd
              | lynxflow_cmd
              | search_expr           -- implicit search after pipe (SPL1 compat)

filter_cmd      ::= ('search' search_expr) | ('where' where_expr)
transform_cmd   ::= eval_cmd | rename_cmd | fields_cmd | table_cmd
                  | fillnull_cmd | rex_cmd | unpack_cmd | unroll_cmd | pack_cmd
                  | fieldformat_cmd | regex_cmd | replace_cmd
aggregate_cmd   ::= stats_cmd | eventstats_cmd | streamstats_cmd
                  | timechart_cmd | chart_cmd | top_cmd | rare_cmd | xyseries_cmd
                  | untable_cmd
sort_limit_cmd  ::= sort_cmd | head_cmd | tail_cmd | dedup_cmd
                  | reverse_cmd
join_cmd        ::= join_cmd | append_cmd | appendcols_cmd | appendpipe_cmd
                  | multisearch_cmd | transaction_cmd | union_cmd
mv_cmd          ::= materialize_cmd | dropview_cmd | views_cmd
compatibility_cmd ::= addinfo_cmd | convert_cmd | fieldsummary_cmd
                  | flatten_cmd | iplocation_cmd | lookup_cmd | makemv_cmd
                  | makeresults_cmd | mvcombine_cmd | mvexpand_cmd
                  | nomv_cmd | spl1_diagnostic_cmd | tags_cmd | thru_cmd
                  | timewrap_cmd | tstats_cmd

-- LynxFlow
lynxflow_cmd ::= group_compute | every | latency | slowest | topby | bottomby
              | errors | rate_directive | proportion | percentiles
              | take | order_by | keep | omit | rank | parse_cmd
              | enrich_cmd | lookup_cmd | compare | baseline | changes
              | facets | exemplars | impact | sessionize | trace
              | correlate | running | outliers | patterns | glimpse
              | describe | rollup | topology | bucket | explode | select_cmd

group_compute ::= 'GROUP' ('BY' field_list)? 'COMPUTE' agg_list
every         ::= 'EVERY' duration ('BY' field_list)? 'COMPUTE' agg_list
bucket        ::= 'BUCKET' field 'SPAN' '=' duration ('AS' field)?
latency       ::= 'LATENCY' field 'EVERY' duration ('BY' field_list)?
                  ('COMPUTE' latency_agg_list)?
slowest       ::= 'SLOWEST' int? field? ('BY' field)?
topby         ::= 'TOPBY' int field 'USING' eval_expr ('COMPUTE' agg_list)?
errors        ::= 'ERRORS' ('BY' field_list)? ('COMPUTE' agg_list)?
rate_directive::= 'RATE' ('PER' duration)? ('BY' field_list)?
proportion    ::= 'PROPORTION' where_expr 'AS' field
                  ('EVERY' duration)? ('BY' field_list)?
percentiles   ::= 'PERCENTILES' field ('BY' field_list)?
take          ::= 'TAKE' int
order_by      ::= 'ORDER' 'BY' sort_list
rank          ::= 'RANK' ('TOP' | 'BOTTOM') int 'BY' field
parse_cmd     ::= 'PARSE' format '(' field (',' string)? ')' ('AS' ident)?
                  ('EXTRACT' '(' field_list ')')? ('IF_MISSING')?
enrich_cmd    ::= 'ENRICH' agg_list ('BY' field_list)?
lookup_cmd    ::= 'LOOKUP' name 'ON' field
compare       ::= 'COMPARE' ('PREVIOUS')? duration
baseline      ::= 'BASELINE' field 'WINDOW' '=' int ('BY' field_list)?
changes       ::= 'CHANGES' field ('BY' field_list)?
facets        ::= 'FACETS' field_list ('limit' '=' int)?
exemplars     ::= 'EXEMPLARS' int? ('BY' field_list)?
impact        ::= 'IMPACT' agg_func? 'BY' field_list
sessionize    ::= 'SESSIONIZE' ('MAXPAUSE' '=' duration)? ('BY' field_list)?
trace         ::= 'TRACE' ('TRACE_ID' '=' field)? ('SPAN_ID' '=' field)?
                  ('PARENT_ID' '=' field)?
keep          ::= 'KEEP' field_list     -- alias for fields +
omit          ::= 'OMIT' field_list     -- alias for fields -
select_cmd    ::= 'SELECT' select_list
explode       ::= 'EXPLODE' field_list ('AS' field)?
```

Full BNF — `docs/grammar.bnf` (TBD).

---

## 10. Compatibility decisions

1. **Ternary operator in eval (`x ? y : z`).** Short, but conflicts with optional chaining `?.` and nullish coalescing `??`. This RFC keeps `if(cond, then, else)` as the canonical conditional form.

2. **Quotes for names.** Canon is single quotes for field names that need quoting. Double quotes are values. Existing double-quoted field names may continue to parse in compatibility mode with lint `L012`.

3. **Search precedence.** SEARCH context uses Splunk-compatible precedence: parentheses > `NOT` > `OR` > `AND`. WHERE/EVAL context uses the more common expression precedence where `AND` binds before `OR`, with `XOR` lower than `OR`. `L030` shows the explicit-parentheses form for mixed `AND`/`OR` SEARCH queries because the Splunk behavior is surprising outside Splunk.

4. **`errors` scope.** The language shortcut is intentionally conservative: `lower(level) IN ("error","fatal")`. Broader domain definitions such as HTTP 5xx, severity classes, or exception fields belong in visible predicates or saved templates.

5. **Metric field defaults.** `latency` requires its metric field. `slowest` has a documented default (`duration_ms`) only for compatibility and emits lint `L036` when used without an explicit field.

6. **Advanced helpers.** `facets`, `impact`, `baseline`, `changes`, `exemplars`, `outliers`, `patterns`, `topology`, `correlate`, `sessionize`, `trace`, `rollup`, `glimpse`, `describe`, and `compare` are part of the pipeline surface. If a deployment omits an optional runtime operator, failure is a runtime capability error, not a parse-time grammar change.

7. **SQL-style `SELECT`.** Top-level `SELECT ... FROM ... WHERE ...` is outside this RFC. The supported `select` form is a pipeline projection command.

8. **Phrase time shortcuts.** Natural phrases (`last 1h`, `today`, `yesterday`) are UI/templates, not grammar. The canonical query text uses bracket source ranges (`FROM x[-1h]`) or relative durations in `_time` predicates.

9. **Default source.** Free-hand search and source-less pipeline commands must record the selected default source in `meta.rewrites`. Deployments may use `main` for compatibility or `*` for global search, but the user-visible rewrite must make the choice explicit.

---

## 11. Appendix A — "one task, two layers" examples

### A.1 "Top-10 erroring endpoints for the past hour"

**LynxFlow:**
```
from api[-1h] | errors by endpoint compute count() as n | order by n desc | take 10
```

**SPL2 (equivalent):**
```
FROM api[-1h]
| where lower(level) IN ("error","fatal")
| stats count() AS n BY endpoint
| sort -n
| head 10
```

LynxFlow: 12 tokens. SPL2: 30 tokens.

### A.2 "p99 latency by service for the past 24 hours, 1-minute window"

**LynxFlow:**
```
from api[-24h] | latency duration_ms every 1m by service compute p99
```

**SPL2:**
```
FROM api[-24h]
| timechart span=1m perc99(duration_ms) AS p99 BY service
```

LynxFlow keeps the time-series intent compact while still naming the metric field explicitly.

### A.3 "Compare the number of 5xx before and after a deploy"

**LynxFlow:**
```
from nginx[-1h] | status >= 500 | group by service compute count() as n | compare previous 1h
```

**Conceptual equivalent:**
```
$current  = FROM nginx[-1h]      | where status >= 500 | stats count() AS n BY service;
$previous = FROM nginx[-2h..-1h] | where status >= 500 | stats count() AS previous_n BY service;
FROM $current
| join type=left service [FROM $previous]
| eval change_n = n - previous_n
```

`compare previous 1h` performs this previous-window replay against the pipeline prefix and emits `previous_*` / `change_*` columns for numeric fields.

### A.4 "Trace tree for a request"

**LynxFlow:**
```
from *[-1h] | where trace_id = "req-abc-123" | trace
```

**SPL2:**
```
FROM *
| where trace_id = "req-abc-123"
| trace trace_id=trace_id span_id=span_id parent_id=parent_id
```

The filter that selects the request remains explicit; `trace` builds the span tree from the selected rows.

### A.5 "Glimpse — source overview"

**LynxFlow:**
```
from nginx | glimpse
```

**SPL2:** no short equivalent — this is a metadata command. LynxFlow is a pure UX win here.

### A.6 "5xx rate and blast radius"

**LynxFlow:**
```
from nginx[-1h]
| proportion status >= 500 AS error_rate every 5m by service
| baseline error_rate window=12 by service
```

**SPL2:**
```
FROM nginx[-1h]
| bin _time span=5m
| stats count(eval(status >= 500)) AS error_rate_num, count() AS error_rate_den BY service, _time
| eval error_rate = error_rate_num / error_rate_den
| streamstats current=false window=12 avg(error_rate) AS baseline_error_rate, stdev(error_rate) AS stdev_error_rate BY service
| eval delta_error_rate = error_rate - baseline_error_rate, z_error_rate = if(stdev_error_rate > 0, delta_error_rate / stdev_error_rate, null())
```

### A.7 "Find affected slices and inspect examples"

**LynxFlow:**
```
from api[-30m]
| where status >= 500
| facets service, host, region limit=5
```

For raw examples after choosing a slice:

```
from api[-30m]
| where status >= 500 AND service = "checkout"
| exemplars 5 by endpoint
```

### A.8 "Free-hand search"

**Input:**
```
lynxdb query 'errors'
```

**SPL2 rewrite:**
```
FROM <default>
| search errors
```

The result metadata includes selected sources, scanned/skipped segments, and whether the query used `_raw` term-index candidates before row evaluation.

### A.9 "Glob and regex search"

**LynxFlow/SPL2 mixed input:**
```
from logs*,!logs-debug*[-1h]
| where service =~ "^(checkout|payments)$"
| search host=web-* (panic OR fatal)
```

**Planning expectations:**
- `logs*,!logs-debug*` is resolved as a source selector before segment planning.
- `[-1h]` pushes down the time range.
- `host=web-*` is planned as a field glob.
- `panic OR fatal` extracts both terms as candidate literals before row verification.

---

## 12. Appendix B — out of scope for this RFC

- Runtime semantics (exactly how the VM executes commands).
- Low-level optimizer implementation details beyond the user-visible planning contract in §6.10.
- Storage layout, mmap, segment format.
- Indexing strategy.
- Distributed query execution.
- Authentication / authorization.
- REST/gRPC wire protocols (only advisory `meta.lints`, `meta.rewrites`, `meta.suggestions`, and `meta.explain` fields are referenced).

These aspects are covered by other RFCs and existing documentation.

---

## 13. Appendix C — external UX references

Non-normative references used for the UX additions:
- GitHub Issues advanced search rollout: AST-based parsing, backward-compatible grammar, feature-flag testing, shadow comparison, limited nesting depth, highlighting, and autocomplete. Source: https://github.blog/developer-skills/application-development/github-issues-search-now-supports-nested-queries-and-boolean-operators-heres-how-we-rebuilt-it/
- Azure AI Search simple query syntax: explicit distinction between simple and advanced parsers, visible precedence grouping, escaping rules, prefix/wildcard cautions, and bounded query size. Source: https://learn.microsoft.com/en-us/azure/search/query-simple-syntax
- GitHub Code Search syntax: bare terms search broadly, qualifiers narrow the search, boolean operators are explicit, and `path:` uses glob behavior with `*`, `?`, and `**`. LynxDB keeps the broad-term and glob lessons but uses string regex patterns instead of slash regex literals. Source: https://github.com/github/docs/blob/main/content/search-github/github-code-search/understanding-github-code-search-syntax.md
- Apache Lucene query parser syntax: wildcard searches use `?` and `*`; leading wildcards are intentionally discouraged. Source: https://lucene.apache.org/core/2_9_4/queryparsersyntax.html
- Elasticsearch wildcard and regexp query docs: leading wildcards and unanchored regexp patterns are resource-intensive; regexp length is bounded by default; narrow with other query types first. Sources: https://www.elastic.co/docs/reference/query-languages/query-dsl/query-dsl-wildcard-query and https://www.elastic.co/docs/reference/query-languages/query-dsl/query-dsl-regexp-query
- ripgrep docs: fast search relies on a linear-time default regex engine, literal optimizations, smart filtering, bounded regex memory, optional PCRE2, parallel traversal, and glob filters. Sources: https://github.com/BurntSushi/ripgrep and https://manpages.debian.org/testing/ripgrep/rg.1.en.html
- Advanced-search UX guidance: syntax-based search needs feedback and error prevention; faceted/advanced controls should reduce cognitive load and show feedback. Source: https://blog.logrocket.com/ux-design/advanced-ux-search-principles/
- General search UX guidance: autocomplete, result counts, match highlighting, loading/empty feedback, and guided refinement improve discoverability without changing user intent. Source: https://nulab.com/learn/design-and-ux/search-ux-best-practices/
- Grafana Loki LogQL docs: log pipelines chain filtering, parsing, and mutation; pattern parsing gives a faster/easier alternative to regular expressions; pattern filters expose wildcard placeholders; pipeline parse errors stay visible as data. Sources: https://grafana.com/docs/loki/latest/query/log_queries/ and https://grafana.com/docs/loki/latest/query/query_reference/
- Datadog Logs docs: facets support filtering, analytics, top-value summaries, and pattern investigation; pattern inspector exposes the distribution of values behind a log pattern. Sources: https://docs.datadoghq.com/logs/explorer/facets/ and https://docs.datadoghq.com/logs/explorer/analytics/patterns/
- Honeycomb observability workflow docs/posts: breakdowns and derived columns are practical investigation tools for isolating affected hosts, versions, customers, and other slices during latency/error spikes. Sources: https://docs.honeycomb.io/investigate/query/build/ and https://www.honeycomb.io/blog/level-up-with-derived-columns-bucketing-events-for-comparison
- CNCF TAG Observability query-language work: common observability query languages should cover logs, metrics, traces, profiles, events, correlation, trend analytics, result schemas, and recommended APIs without locking investigation workflows to a single telemetry type. Source: https://github.com/cncf/toc/issues/1034

---

## 14. Appendix D — official Splunk references

Normative compatibility references checked while preparing this RFC:
- SPL syntax conventions and command argument rules: https://help.splunk.com/en/splunk-enterprise/spl-search-reference/10.0/introduction/understanding-spl-syntax
- SPL boolean precedence differences between `search`, `where`, and `eval`: https://help.splunk.com/en/splunk-enterprise/search/search-manual/9.0/expressions-and-predicates/boolean-expressions-with-logical-operators
- SPL/SPL2 time modifiers (`earliest`, `latest`, `_index_earliest`, `_index_latest`, `now`): https://help.splunk.com/splunk-cloud-platform/search/spl2-search-manual/dates-and-time/time-modifiers
- SPL2 command quick reference: https://help.splunk.com/en/splunk-cloud-platform/search/spl2-search-reference/quick-reference-for-spl2-commands
- SPL2 commands compatibility profiles: https://help.splunk.com/en/splunk-cloud-platform/process-data-at-the-edge/quick-reference-for-spl2
- SPL2 eval command syntax: https://help.splunk.com/splunk-cloud-platform/search/spl2-search-reference/eval-command/eval-command-overview-and-syntax
- SPL2 eval functions quick reference: https://help.splunk.com/en/splunk-cloud-platform/search/spl2-search-reference/evaluation-functions/quick-reference-for-spl2-eval-functions
- SPL2 statistical and charting functions quick reference: https://help.splunk.com/splunk-cloud-platform/search/spl2-search-reference/statistical-and-charting-functions/quick-reference-for-spl2-stats-and-charting-functions
- SPL2 overview of stats/chart functions and `eval(...)` aggregate arguments: https://help.splunk.com/splunk-cloud-platform/search/spl2-search-reference/statistical-and-charting-functions/overview-of-spl2-stats-and-chart-functions
- SPL commands in SPL2 / embedded SPL: https://help.splunk.com/en/splunk-enterprise/search/spl2-overview/using-spl-commands-in-spl2-searches

## 15. Changelog

| Date | Change |
|---|---|
| 2026-05-12 | Initial draft. |
| 2026-05-12 | Added non-breaking UX assistance contract, autocomplete/diagnostics rules, compatibility rollout gates, query-safety limits, and external UX references. |
| 2026-05-12 | Added configured free-hand search (`lynxdb query 'errors'`), glob/string-regex surface, and user-visible planning contract for optimized broad/pattern search. |
| 2026-05-12 | Added deterministic LynxFlow observability helpers: `proportion`, `facets`, `impact`, `baseline`, `changes`, and `exemplars`; explicitly excluded AI/LLM-style inference from investigation helpers. |
| 2026-05-12 | Reconciled the RFC against official Splunk SPL/SPL2 docs: added Splunk SEARCH precedence (`OR` before `AND`), `XOR` for WHERE/EVAL, time modifiers, a broader SPL2 command compatibility surface, and missing eval/statistical function families. |
