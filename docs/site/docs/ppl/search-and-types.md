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

| Search term | LynxDB semantics | Narrows the scan |
| --- | --- | --- |
| `error` | a field of the event holds the token `error`, any case | token filters |
| `api/v1/users`, `web-01` | a term with punctuation is a phrase of its tokens | token filters |
| `conn*` | a token starts with `conn` | no |
| `us?r`, `*err*` | a token matches the wildcard pattern | no |
| `"connection refused"` | the phrase occurs at token boundaries, any case | token filters |
| `level=ERROR`, `host="web-01"` | the field equals the string, case-sensitive | the field's token filter |
| `status=500` | the field equals 500 (a numeric string matches too) | no |
| `host=web-*` | the whole field value matches the wildcard, case-sensitive | no |
| `status!=500` | the field is absent or not 500 | no |
| `status>=500` | numeric or lexical comparison | no |
| `level IN (WARN, ERROR)` | any listed value | no |
| `error OR warn` | either word | no |
| `NOT error` | no field of the event holds the token | no |
| `earliest=-1h latest=now` | the event time is in the range | time partitions and blocks; enforced exactly |

Tokens are maximal runs of letters and digits, compared in lower case.

### LynxDB behavior

- OpenSearch searches free text in every field (`index.query.default_field`
  defaults to `*`). LynxDB searches the string values of every field of the
  event, and its integer values for a term of digits; strings inside arrays
  are not searched. An event that arrived as a line of text has its text in
  `_raw`, which free text searches with the fields extracted from it.
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
[Expressions](./expressions.md)). A `where` directly after the source is
merged with the search terms into one filter, so `where host = 'web-01'`
and `source=main host="web-01"` read the same blocks.

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
and `match_bool_prefix` fail with `UNSUPPORTED_FUNCTION`. A field search
lets the scan leave out the blocks whose token filter for that field rules
its tokens out.

## How search terms narrow a scan

Compilation turns each search into an IR predicate that stays in the plan,
so results never depend on which blocks a scan leaves out. When the
columnar executor runs the filter, it reads the blocks of the scan's time
range and, before it opens one, leaves it out when:

- the block lies outside the time range of `earliest`/`latest`, of a
  comparison of `_time` with a constant, or of the request's `from`/`to`;
- the token filters of the block show that no string field can hold every
  token of a free-text word or phrase;
- the token filter of a field shows that the field cannot hold the string a
  search term, a `where` equality or a `match` asks for.

An opened block can still be rejected from its column directory, for
example by the bounds of an integer field compared with a number. Every
other condition is evaluated per row. A word of digits and a prefix of one
word (`conn*`) have no tokens a filter can rule out.

The optimizer also copies the predicates it calls indexable into the scan
(`prune=` in a plan): free-text searches other than regular expressions and
substrings, a field equal to a string, and an `OR` of free-text words.
`lynxdb explain` reports them as the plan's search terms, and when the
columnar executor does not take the filter above a scan, the scan applies
them before the row operators evaluate the whole filter. See
[Performance](./performance.md).

Types and conversions are described in [Data types](./data-types.md).
