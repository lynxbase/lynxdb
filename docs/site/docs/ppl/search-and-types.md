---
title: Search semantics
sidebar_label: Search semantics
description: How PPL search expressions match events and use LynxDB's indexes.
---

# Search semantics

## The search command

`search source=<index> [<search expression>]` reads events from an index
(or several, comma-separated, or an `*` pattern such as `logs-*`) and keeps
those matching the search expression. `source=<index>` alone at the start of
a query is the same command; `index=` is a synonym of `source=`. The source
clause may appear anywhere among the terms.

The grammar follows OpenSearch PPL: `NOT` binds tightest, then `OR`, then
`AND`; adjacent terms are joined by an implicit `AND`; parentheses group.
`error OR warn timeout` reads as `(error OR warn) AND timeout`.

| Search term | LynxDB semantics | Index use |
| --- | --- | --- |
| `error` | the raw text contains the token `error`, any case | term index and bloom filters |
| `api/v1/users`, `web-01` | a term with punctuation is a phrase of its tokens | term index on its tokens; phrase checked per row |
| `conn*` | a token of the raw text starts with `conn` | term index (token prefix) |
| `us?r`, `*err*` | a token matches the wildcard pattern | term index (token wildcard) |
| `"connection refused"` | the phrase occurs at token boundaries, any case | term index on its tokens; phrase checked per row |
| `level=ERROR`, `host="web-01"` | the column equals the string, case-sensitive | column statistics and column predicates |
| `status=500` | the column equals 500 (a numeric string matches too) | checked per row |
| `host=web-*` | the whole column value matches the wildcard, case-sensitive | checked per row |
| `status!=500` | the column is absent or not 500 | checked per row |
| `status>=500` | numeric or lexical comparison | checked per row |
| `level IN (WARN, ERROR)` | any listed value | checked per row |
| `error OR warn` | either word | term index (any of the words) |
| `NOT error` | the raw text lacks the token | checked per row |
| `earliest=-1h latest=now` | the event time is in the range | segment time ranges; enforced exactly |

Tokens are maximal runs of letters and digits, compared in lower case.

### LynxDB behavior

- OpenSearch searches free text in every field (`index.query.default_field`
  defaults to `*`). LynxDB searches the event's raw text `_raw`, which holds
  every field of the event as ingested.
- OpenSearch analyzes text fields, so a field comparison on a text field
  matches individual tokens. LynxDB compares the whole column value, as
  OpenSearch does for keyword fields. Use `match(field, 'text')` for token
  matching on one field.
- A search term is two-valued: `NOT term` and `field!=value` match events
  without the field.
- A cross-cluster prefix (`cluster:index`) is read as part of the index name.

## Time modifiers

`earliest=` and `latest=` accept `now`, absolute times (`yyyy-MM-dd HH:mm:ss`,
`MM/dd/yyyy:HH:mm:ss`, quoted), Unix seconds, and relative chains such as
`-7d`, `+1d@d`, `-1mon@mon` and `@w1`. Relative values resolve against the
query's start time, in UTC. The bounds narrow the scan's time range and are
enforced exactly. The REST `from`/`to` parameters and the CLI `--since`,
`--from` and `--to` flags add a second range; a query reads the
intersection.

## The where command

`where` evaluates a boolean expression with three-valued logic (see
[Expressions](./expressions.md)). It has no index-specific semantics, but the
optimizer hands its indexable conjuncts (raw-text searches, a column equal
to a string, disjunctions of raw-text words) to the scan in the same way as
search terms. `where host = 'web-01'` and `source=main host="web-01"` read
the same data.

## Searching one field

The relevance functions search the tokens of one column, case-insensitively,
with the same tokens as the search command. LynxDB computes no relevance
scores, so they are plain filters.

| Function | LynxDB semantics |
| --- | --- |
| `match(f, 'a b')` | the column contains the token `a` or the token `b`; with `operator='AND'`, both |
| `match_phrase(f, 'a b')` | the phrase occurs in the column at token boundaries |
| `match_phrase_prefix(f, 'cry')` | a token of the column starts with `cry` |
| `match_phrase_prefix(f, 'a b')` | the phrase occurs from a token boundary, its last token as a prefix |

`match` joins several tokens with OR by default, as OpenSearch does.
`boost` is accepted and ignored. `zero_terms_query` (`none` or `all`)
decides the result when the text has no tokens; the default `none` matches
nothing. Other options, such as `slop` or `fuzziness`, fail with
`UNSUPPORTED_FEATURE`. `multi_match`, `simple_query_string`, `query_string`
and `match_bool_prefix` fail with `UNSUPPORTED_FUNCTION`. Field searches are
checked per row; the term index covers `_raw` only.

## How the server uses search terms

Compilation turns each search into an IR predicate that stays in the plan, so
results never depend on an index. The optimizer also copies the indexable
predicates into the scan (`prune=` in a plan), and the server's planner
(`pkg/planner`) converts the plan's scans into storage hints
(`pkg/engine/physical/hints.go`) that decide which segments and rows the
server reads:

| Hint | From | The server uses it to |
| --- | --- | --- |
| time bounds | `earliest`/`latest`, comparisons of `_time` with constants, request `from`/`to` | skip segments outside the range |
| search terms | words and phrase tokens on `_raw` | skip segments whose bloom filter lacks a term; select rows through the term index |
| token globs | prefix and wildcard terms on `_raw` | select rows through the term index |
| term tree | `OR` of single words on `_raw` | select rows through the term index |
| field predicates | a column equal to a string | skip segments by column statistics; filter rows while reading |
| required columns | the columns the plan reads | read only those columns |
| limit | `head` over row-preserving commands | stop reading early |

A plan with several scans (joins, subsearches, `append`) keeps only the union
of their sources and time ranges. Predicates the hints do not cover are
evaluated per row. See [Performance](./performance.md).

Types and conversions are described in [Data types](./data-types.md).
