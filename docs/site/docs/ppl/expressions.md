---
title: PPL expressions and operators
sidebar_label: Expressions
description: Operators, comparisons, NULL handling, conditional expressions and subqueries in PPL.
---

# Expressions

Expressions appear in `where`, `eval`, the arguments of functions and
aggregations, `sort` keys, `fillnull` values and join conditions. Precedence
is listed in [Syntax](./syntax.md#operator-precedence).

## Fields and literals

A bare name is a field: `status`, `response_time`, `payload.user.id`,
`` `count()` ``. Literals are described in [Syntax](./syntax.md#literals).
A reference to a field a row does not have is NULL, unless an earlier command
produced a closed schema without that field, in which case the reference is
a `SEMANTIC_ERROR` at compile time.

## Arithmetic

| Operator | Meaning |
| --- | --- |
| `a + b` | addition; two strings concatenate |
| `a - b` | subtraction; timestamp minus timestamp is a duration |
| `a * b` | multiplication |
| `a / b` | division; int / int is integer division |
| `a % b` | modulo |
| `-a` | negation |

Integer division and modulo by zero yield NULL, and so do division and
modulo of floats by zero; `%` of floats is the remainder of truncated
division. A string is read as the number it holds, and two strings
concatenate only with `+`. A timestamp plus or minus a duration
(`_time - INTERVAL 1 HOUR`) is a timestamp. `add`, `subtract`, `multiply`
and `divide` are function forms of the same operators.

## Comparisons

| Operator | Meaning |
| --- | --- |
| `=`, `==` | equal |
| `!=`, `<>` | not equal |
| `<`, `<=`, `>`, `>=` | ordering; numbers numerically, strings lexically, a string and a number numerically, timestamps by time |
| `x LIKE 'a%b_'` | SQL pattern: `%` any run, `_` one character; case-insensitive |
| `x ILIKE 'a%'` | case-insensitive `LIKE` |
| `x REGEXP 're'` | RE2 regular expression matches anywhere in `x` |
| `x CONTAINS 'text'` | `x` contains the substring, case-insensitive |
| `x IN (a, b, c)`, `x NOT IN (...)` | membership |
| `x BETWEEN lo AND hi`, `NOT BETWEEN` | `lo <= x AND x <= hi` |
| `x IS NULL`, `x IS NOT NULL` | NULL test |

`like(x, pattern, true)` is the case-sensitive form of `LIKE`.

### LynxDB behavior

- A string compared with a number is read as a number, and a string
  compared with a timestamp is read as a timestamp:
  `where _time >= '2025-01-15 08:05:00'`. A string that does not parse makes
  the comparison NULL.
- Regular expressions use RE2 syntax: no lookaround and no backreferences.

## Boolean logic

`AND`, `OR`, `XOR` and `NOT` use three-valued (Kleene) logic: a comparison
with a NULL operand is NULL, `NULL AND FALSE` is FALSE, `NULL OR TRUE` is
TRUE, and `NOT NULL` is NULL. `where` keeps a row only when its condition is
TRUE, so `where status != 200` drops rows without `status`. Write
`where status != 200 OR isnull(status)` to keep them.

`x IN (a, b)` is TRUE on a match, NULL when there is no match and `x` or a
list element is NULL, FALSE otherwise.

Search terms in the source command are two-valued: `source=main status!=200`
keeps rows without `status`. See [Search semantics](./search-and-types.md).

## NULL and missing values

| Function | TRUE when |
| --- | --- |
| `isnull(x)`, `x IS NULL` | `x` is NULL or the field is absent |
| `isnotnull(x)`, `ispresent(x)`, `x IS NOT NULL` | `x` has a value |
| `isempty(x)` | `x` is NULL or the empty string |
| `isblank(x)` | `x` is NULL, empty or only white space |

`coalesce(a, b, ...)` returns the first non-NULL argument, `ifnull(a, b)` is
`coalesce(a, b)`, and `nullif(a, b)` is NULL when `a = b`, else `a`.
`fillnull` replaces NULLs of whole columns.

## Conditional expressions

```
eval class = case(status >= 500, 'server error',
                  status >= 400, 'client error'
                  else 'ok')
eval slow = if(response_time > 500, 'slow', 'fast')
```

`case` takes condition and value pairs and an optional `else`; it returns the
value of the first TRUE condition, the `else` value, or NULL. `if(c, a, b)`
is `case(c, a else b)`. Branch types unify as described in
[Data types](./data-types.md#unify).

## Function calls

`name(arg, ...)` calls a scalar function from the
[function table](./functions.md). Some functions take special argument
forms:

| Form | Example |
| --- | --- |
| cast | `cast(status as string)` |
| extract | `extract(hour from _time)` |
| timestampadd, timestampdiff | `timestampdiff(SECOND, _time, now())` |
| position | `position('v1' in path)` |
| interval | `date_add(_time, INTERVAL 1 DAY)` |
| lambda | `transform(arr, x -> x * 2)`, `transform(arr, (x, i) -> x + i)`, `reduce(arr, 0, (acc, x) -> acc + x)` |
| named options | `match(msg, 'a b', operator='AND')` (relevance functions only) |

Aggregation functions such as `count()` or `avg(x)` are only valid in
`stats`, `eventstats`, `streamstats`, `chart` and `timechart`; elsewhere they
are a `SEMANTIC_ERROR`.

## Subqueries in expressions

```
where host IN [ source=main | where status = 503 | fields host ]
where (host, path) IN [ source=allowlist | fields host, path ]
where EXISTS [ source=main | where status = 999 ]
eval max_rt = [ source=main | stats max(response_time) ]
```

Subqueries are uncorrelated: a reference to the outer query's source name,
alias or fields fails to compile with `UNSUPPORTED_FEATURE`. Each subquery
runs once, before the outer query, and its result is held in memory. See
[Joins and subsearches](./joins-and-subsearches.md#subqueries-in-expressions).
