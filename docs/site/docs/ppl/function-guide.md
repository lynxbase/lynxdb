---
title: Function guide
sidebar_label: Function guide
description: How PPL scalar functions behave in LynxDB, by category, with examples.
---

# Function guide

The [function table](./functions.md) lists every OpenSearch PPL scalar
function name and how LynxDB executes it. This page explains the shared
rules and shows the common functions by category. Aggregations are in the
[aggregation guide](./aggregation-guide.md).

## How a function name resolves

The PPL scalar catalog (`pkg/ppl/functions.go`, `pkg/ppl/relevance.go`) maps
each name to one of three things:

- an IR registry function, often of the same name (`abs`, `concat`,
  `date_format`) or an alias (`ceiling` is `ceil`, `substr` is `substring`,
  `current_timestamp` is `now`);
- a lowering onto IR operators (`if` and `nullif` become `CASE`, `isnull`
  becomes a NULL test, `add` becomes `+`, `match` becomes a field search);
- an unsupported error with its reason (`geoip`, `multi_match`,
  `query_string`, `simple_query_string`, `match_bool_prefix`). These fail
  with `UNSUPPORTED_FUNCTION`.

A name that is not in the catalog fails with `UNSUPPORTED_FUNCTION` too
(`unknown function x`). An aggregation used outside an aggregating command is
a `SEMANTIC_ERROR`.

## Shared rules

- **NULL.** Most functions are strict: a NULL argument gives a NULL result
  (`concat('a', null)` is NULL). The NULL-aware functions handle NULL
  arguments themselves: `coalesce`, the NULL tests, `concat_ws` (which skips
  them), `tostring`, `json`, `json_valid`, `json_array`, `array`, `forall`,
  `exists` and `mvappend`.
- **Types.** Arguments are checked against the registry's parameter types at
  compile time; a mismatch with a known type is a `TYPE_ERROR`
  (`lower(5)`), a value of type `any` is converted or rejected per row.
  `concat`, `concat_ws` and `mvjoin` convert non-string arguments to strings.
- **Constants.** Some parameters must be constants, such as the patterns of
  `regexp_match` and `like`. They are compiled once per query, and an invalid
  pattern fails the query before it reads data.
- **Errors at run time.** Invalid values (an unparsable date string, a log of
  a negative number) give NULL; they do not fail the query.
- **Folding.** Calls of deterministic functions with constant arguments are
  evaluated once at planning time. `now()` and the other current-time
  functions return the query's reference time, the same for every row.

## Math

```
eval a = abs(-3), b = round(3.14159, 2), c = pow(2, 10), d = log10(1000), e = mod(7, 3)
```

`abs`, `ceil`, `floor`, `round`, `rint`, `sqrt`, `cbrt`, `pow`, `exp`,
`expm1`, `ln`, `log`, `log2`, `log10`, `mod`, `sign`, trigonometric
functions, `degrees`, `radians`, `pi`, `e`, `rand`, `conv`, `crc32`. The
multi-argument forms `max(a, b, ...)`, `min(...)`, `sum(...)` and `avg(...)`
outside an aggregation compare or combine their arguments.

## Strings

```
eval a = concat(host, ':', status), b = concat_ws('-', host, level), c = length(host),
     d = substring(path, 1, 4), e = upper(level), f = locate('v1', path)
eval r = replace(path, '/api/v[0-9]+', ''), m = regexp_match(path, 'users$')
```

`substring` and `locate` count from 1. `replace(s, regex, replacement)` uses
a regular expression; `like(s, pattern [, case_sensitive])` and `ilike` match
SQL patterns. Regular expressions are RE2.

## Conditions

```
eval class = if(status >= 500, 'error', 'ok'), who = coalesce(user, 'anonymous')
eval y = nullif(status, 200)
where isnotnull(user) AND NOT isblank(user)
```

See [Expressions](./expressions.md#null-and-missing-values).

## Date and time

```
eval day = date_format(_time, '%Y-%m-%d'), h = hour(_time), dow = dayofweek(_time)
eval t = strftime(_time, '%H:%M:%S'), epoch = unix_timestamp(_time)
eval age_days = datediff(now(), _time), mins = timestampdiff(MINUTE, start, _time)
eval prev = date_sub(_time, INTERVAL 1 DAY), next = adddate(_time, 1)
where latest('-1d', _time)
```

- `date_format` and `time_format` take MySQL-style `%` specifiers
  (`%Y-%m-%d %H:%i:%s`); `strftime` takes POSIX specifiers
  (`%Y-%m-%d %H:%M:%S`).
- `earliest('-1d@d', _time)` and `latest('-1h', _time)` compare a time with a
  [time modifier](./syntax.md#literals).
- Times are UTC. `convert_tz` converts between zones.
- `extract(hour from _time)`, `timestampadd(DAY, 1, _time)` and
  `timestampdiff(SECOND, a, b)` take a unit keyword as their first argument.

## Type conversion

```
eval s = cast(status as string), n = tonumber('ff', 16), c = tostring(1234567, 'commas')
```

See [Data types](./data-types.md#conversions).

## JSON

```
eval id = json_extract(payload, '$.user.id'), keys = json_keys(payload), ok = json_valid(payload)
eval o = json_object('host', host, 'status', status), a = json_array(1, 2, 3)
```

JSON functions take and return JSON text. `json_set`, `json_delete`,
`json_append` and `json_extend` return the modified text. The `spath`
command extracts JSON values into columns.

## Arrays and lambdas

```
eval tags = split(tag_list, ','), n = array_length(tags), first = mvindex(tags, 0)
eval doubled = transform(values, x -> x * 2), big = filter(values, x -> x > 10)
eval total = reduce(values, 0, (acc, x) -> acc + x), any_big = exists(values, x -> x > 10)
```

`mvindex` and `mvfind` count from 0. Lambdas (`x -> expr`,
`(acc, x) -> expr`) are valid only as arguments of `forall`, `exists`,
`filter`, `transform` and `reduce`. `mvmap(field, expr)` evaluates `expr`
with the field name standing for each element.

## IP, hashing, system

```
where cidrmatch(client_ip, '10.0.0.0/8')
eval h = md5(user), s = sha2(user, 256), t = typeof(status)
```

`cidrmatch` accepts IPv4 and IPv6. `typeof` returns the run-time type name
(see [Data types](./data-types.md)).

## Relevance functions

`match`, `match_phrase` and `match_phrase_prefix` filter one field by its
tokens; LynxDB computes no scores. See
[Search semantics](./search-and-types.md#searching-one-field).
