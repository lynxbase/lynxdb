---
title: Search semantics and data types
sidebar_label: Search and data types
description: How PPL search expressions use LynxDB's indexes, and how PPL types map onto LynxDB values.
---

# Search semantics and data types

## The search command

`search source=<index> [<search expression>]` reads events from an index
(or several, comma-separated, or an `*` pattern) and keeps those matching
the search expression. The grammar follows OpenSearch PPL: `NOT` binds
tightest, then `OR`, then `AND`; adjacent terms are joined by an implicit
`AND`; parentheses group.

| Search term | LynxDB semantics | Index use |
| --- | --- | --- |
| `error` | the raw text contains the token `error`, any case | term-index hint (whole token) |
| `conn*` | a token of the raw text starts with `conn` | term-index hint (token prefix) |
| `us?r`, `*err*` | a token matches the wildcard pattern | term-index hint (token wildcard) |
| `"connection refused"` | the phrase occurs at token boundaries, any case | term-index hint on its tokens; phrase verified per row |
| `status=500` | the column equals 500 (a numeric string matches too) | verified per row |
| `host="web-01"` | the column equals the string, case-sensitive | column-value pruning hint |
| `host=web-*` | the whole column value matches the wildcard, case-sensitive | verified per row |
| `status!=500` | the column is absent or not 500 | verified per row |
| `status>=500` | numeric or lexical comparison | verified per row |
| `level IN (WARN, ERROR)` | any listed value | verified per row |
| `earliest=-1h@h latest=now` | the event time is in the range | time pruning hint; enforced exactly |

Deviations from OpenSearch:

- OpenSearch searches free text in every field (`index.query.default_field`
  defaults to `*`). LynxDB searches the event's raw text, which holds every
  field of the event as ingested. Every term is verified per row; the term
  hints let a storage engine with a term index skip data that cannot match.
- OpenSearch analyzes text fields, so a field comparison on a text field
  matches individual tokens. LynxDB compares the whole column value, as
  OpenSearch does for keyword fields.
- A search term is two-valued: `NOT term` and `field!=value` match events
  without the field.

Time modifiers accept `now`, absolute times (`yyyy-MM-dd HH:mm:ss`,
`MM/dd/yyyy:HH:mm:ss`), Unix seconds, and relative chains such as `-7d`,
`+1d@d`, `-1month@month` and `@w1`. Relative values resolve against the
query's start time, in UTC.

The `where` command evaluates boolean expressions with three-valued logic
and has no index-specific behaviour, but the optimizer passes its indexable
conjuncts (equality with a string, raw-text searches) to the scan for
pruning in the same way.

## Searching one field

The relevance functions search the tokens of one column, case-insensitively,
with the same tokens as the search command (maximal runs of letters and
digits). LynxDB computes no relevance scores, so they are plain filters.

| Function | LynxDB semantics |
| --- | --- |
| `match(f, 'a b')` | the column contains the token `a` or the token `b`; with `operator='AND'`, both |
| `match_phrase(f, 'a b')` | the phrase occurs in the column at token boundaries |
| `match_phrase_prefix(f, 'cry')` | a token of the column starts with `cry` |
| `match_phrase_prefix(f, 'a b')` | the phrase occurs from a token boundary, its last token as a prefix |

`match` joins several tokens with OR by default, as OpenSearch does.
`boost` is accepted and ignored. `zero_terms_query` (`none` or `all`)
decides the result when the text has no tokens; the default `none` matches
nothing. Other options, such as `slop` or `fuzziness`, are rejected as
unsupported. `multi_match`, `simple_query_string`, `query_string` and
`match_bool_prefix` are not supported.

## Data types

| PPL type | LynxDB value | Notes |
| --- | --- | --- |
| boolean | bool | |
| tinyint, smallint, int, bigint | int (64-bit) | |
| float, double | float (64-bit) | |
| string | string | |
| timestamp | timestamp | nanosecond precision, UTC |
| date | timestamp at 00:00 UTC | `date(x)` and `DATE '...'` return the start of the day |
| time | duration since midnight | `time(x)`, `maketime`, `sec_to_time` |
| interval | duration, or a calendar count for month, quarter and year | `INTERVAL n MONTH` is applied with calendar arithmetic |
| ip | string | `cidrmatch` parses addresses |
| struct | object | |
| array | array | |
| binary, geo_point | not supported | |

Values of columns read from an index carry their stored types; columns
LynxDB extracted as text stay strings until converted. Conversions are
explicit through `cast`, `tonumber`, `tostring` and the `convert` command,
and implicit only where the [IR specification](./ir.md) defines them:
strings meeting numbers or timestamps in comparisons and arithmetic.

Timestamps are written as `yyyy-MM-dd HH:mm:ss[.fraction]` when converted to
strings; durations as `HH:mm:ss[.fraction]`.
