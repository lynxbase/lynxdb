---
title: PPL syntax
sidebar_label: Syntax
description: Pipelines, identifiers, backticks, literals, operator precedence and comments in PPL.
---

# PPL syntax

The lexer (`pkg/ppl/lexer`) and parser (`pkg/ppl/parser`) follow the
OpenSearch PPL grammar. This page describes the lexical rules and the shape
of a query; [Grammar](./grammar.md) lists every production.

## Pipelines

A query is one pipeline: a source command, then commands separated by `|`.

```
source=main level=ERROR | stats count() by host | sort - `count()`
```

- The first command is `search` (written `source=<index>`, `index=<index>`
  or `search source=<index>`), `describe`, `makeresults`, `multisearch` or
  `union`. Any other command after the first `|`.
- A leading `|` and a trailing `;` are accepted: `| source=main | head 5;`.
- `explain` before the pipeline asks for the plan instead of the rows. The
  modes `simple`, `standard`, `cost` and `extended` are accepted and render
  the same plans.
- `search`, `describe`, `makeresults` and `multisearch` are only valid as the
  first command; filter later in the pipeline with `where`.
- Subsearches are pipelines in square brackets: `[ source=other | fields host ]`.

## Case

Command names, keywords, function names and type names are
case-insensitive: `STATS COUNT() BY host` equals `stats count() by host`.
Field names are case-sensitive: `Host` and `host` are different fields.

## Identifiers

A field name starts with a letter, `_`, `@` or `*`, and continues with
letters, digits, `_`, `-` and `*`. A trailing `-` is not part of the name.

| Text | Lexes as |
| --- | --- |
| `response_time` | the field `response_time` |
| `@timestamp` | the field `@timestamp`, an alias of `_time` |
| `web-01` | the identifier `web-01` |
| `a-b` | the identifier `a-b`, not `a` minus `b` |
| `a*b` | the identifier `a*b`, not `a` times `b` |
| `a - b`, `a * b` | subtraction, multiplication |
| `a/b`, `a+b`, `a%b` | division, addition, modulo |

Because `-` and `*` continue an identifier, as in the official grammar,
write arithmetic with spaces around these operators.

**Dotted names.** `a.b.c` with no spaces around the dots is one qualified
name. LynxDB first looks for a column named exactly `a.b.c`; if there is
none, it reads member `b.c` of the object column `a`.

**Backticks** quote any name: `` `my field` ``, `` `count()` ``,
`` `status-code` ``. Inside backticks, a backtick is written twice or
escaped as ``\` ``. Backticks are how a later command refers to an
unaliased aggregate column such as `` `count()` ``.

**Keywords as names.** Keywords are recognized by position, so most of them
stay usable as field names: `eval type = 1, span = 5` and `where index = 'main'`
are valid.

## Literals

| Literal | Examples | Type |
| --- | --- | --- |
| string | `'it''s'`, `"say \"hi\""` | string |
| integer | `42`, `-7` | int |
| decimal | `1.5`, `.5`, `1e3` | float |
| double, float | `1.5d`, `2.5f` | float |
| boolean | `true`, `FALSE` | bool |
| null | `null` | null |
| date | `DATE '2025-01-15'` | timestamp at 00:00 UTC |
| time | `TIME '08:30:00'` | duration since midnight |
| timestamp | `TIMESTAMP '2025-01-15 08:00:00'` | timestamp |
| interval | `INTERVAL 2 HOUR`, `INTERVAL 1 DAY` | duration, or a calendar count for month, quarter and year |
| span | `5m`, `1h`, `30s`, `100` | used by `span=`, `span()` and `bin` |

Strings use single or double quotes. A quote of the same kind is written
twice (`'it''s'`) or escaped (`\'`). The escapes `\n`, `\t`, `\r`, `\\`,
`\'`, `\"` and `` \` `` are recognized; any other backslash stays as written,
so regular expressions such as `'\d+'` need no doubling.

Span units: `ms`, `us`, `cs`, `ds`, `s` (`sec`, `second`), `m` (`min`,
`minute`), `h` (`hr`, `hour`), `d` (`day`), `w` (`week`), `mon` (`month`),
`q` (`qtr`, `quarter`), `y` (`yr`, `year`), each also in plural. `m` is
minutes; months are `mon`.

Time modifiers (`earliest=`, `latest=` and the `earliest()`/`latest()`
functions) accept `now`, `-7d`, `+1d@d`, `-1mon@mon`, `@w1`, Unix seconds
and quoted absolute times (`'2025-01-15 08:00:00'`, `'01/15/2025:08:00:00'`).

## Operator precedence

From lowest to highest:

| Level | Operators |
| --- | --- |
| 1 | `OR` |
| 2 | `XOR` |
| 3 | `AND` |
| 4 | `NOT` |
| 5 | `=`, `==`, `!=`, `<>`, `<`, `<=`, `>`, `>=`, `LIKE`, `ILIKE`, `REGEXP`, `CONTAINS`, `IN`, `NOT IN`, `BETWEEN`, `NOT BETWEEN`, `IS NULL`, `IS NOT NULL` |
| 6 | `+`, `-` |
| 7 | `*`, `/`, `%` |
| 8 | unary `-`, unary `+` |

Operators of one level associate to the left. Parentheses group:
`1 + 2 * 3` is 7, `(1 + 2) * 3` is 9. `NOT status = 200 AND host = 'web-01'`
reads as `(NOT status = 200) AND host = 'web-01'`.

Search expressions in the source command use their own precedence, which
follows the OpenSearch search grammar: `NOT` binds tightest, then `OR`, then
`AND`; adjacent terms are joined by an implicit `AND`. See
[Search semantics](./search-and-types.md).

## Comments

`//` starts a comment that runs to the end of the line; `/* ... */` is a
block comment.

```
source=main /* web tier only */ host=web-*
| stats count() by status // per status code
```

## Limits

The parser rejects with a syntax error a query longer than 256 KiB, nesting
(parentheses, `NOT`, signs, function calls, search groups, subsearches)
deeper than 256 levels, and a pipeline of more than 1000 commands. The
server also enforces `query.max_query_length`.
