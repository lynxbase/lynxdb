---
title: Data types
sidebar_label: Data types
description: PPL types, LynxDB's value types, and the conversions between them.
---

# Data types

PPL type names map onto the IR types of `pkg/ir/types.go`. Every value
LynxDB stores or computes has one of these IR types.

| IR type | Values | PPL types that map to it |
| --- | --- | --- |
| `bool` | TRUE, FALSE | boolean |
| `int` | 64-bit signed integer | tinyint, smallint, int, bigint |
| `float` | 64-bit IEEE 754 | float, double |
| `string` | UTF-8 text | string, ip |
| `timestamp` | an instant with nanosecond precision, UTC | timestamp, date |
| `duration` | a signed span of time | time, interval |
| `array<T>` | ordered list | array |
| `object` | string-keyed map | struct |
| `any` | type decided per row | columns read from an index |
| `null` | the NULL literal | |

`binary` and `geo_point` are not supported.

## OpenSearch PPL types in LynxDB

| PPL type | LynxDB behavior |
| --- | --- |
| date | a timestamp at 00:00 UTC: `date(x)` and `DATE '2025-01-15'` return the start of the day |
| time | a duration since midnight: `time(x)`, `TIME '08:30:00'`, `maketime`, `sec_to_time` |
| interval | a duration; `INTERVAL n MONTH`, `QUARTER` and `YEAR` keep a calendar count and are applied with calendar arithmetic |
| ip | a string; `cidrmatch` parses addresses, `sort ip(field)` sorts by the text form |
| struct | an object; members are read with dotted names (`payload.user.id`) |

`describe <index>` reports the PPL names `string`, `bigint`, `double`,
`boolean`, `timestamp`, `interval`, `array` and `struct`, typed by the first
non-NULL value of each column. `typeof(x)` returns the IR name of a value's
run-time type: `null`, `string`, `int`, `float`, `bool`, `timestamp`,
`duration`, `array`, `object`.

## Schema-on-read columns

Events have two typed columns: `_time` (timestamp) and `_raw` (string).
`_raw` holds the text of an event that arrived as a line that is not a JSON
object; a JSON event has no `_raw`. `@timestamp` is an alias of `_time`.
Every other column of an index is discovered when it is read and has type
`any` at planning time: its values carry the types they were stored with.
The members of a JSON object are columns named by their path, such as
`http.status`. Fields LynxDB extracted from text at ingest (`key=value`
pairs, format fields) are stored with the type their text parses as
(integers, floats, otherwise strings).

A reference to a column that a row does not have evaluates to NULL, but
results are sparse: a field that an event lacks is absent from its result
row, not NULL. Commands that build their own schema, such as `stats`, `fields`
or `top`, produce closed schemas: a later reference to a column they do not
output is a semantic error.

## Conversions

Explicit conversions:

| Form | Result |
| --- | --- |
| `cast(x as int)` | int; a float truncates toward zero; an unparsable string gives NULL. `integer`, `long`, `bigint`, `short` and `byte` are the same target |
| `cast(x as double)` | float; `float` is the same target |
| `cast(x as string)` | string |
| `cast(x as boolean)` | bool; strings `true` and `false` in any case |
| `cast(x as timestamp)` | timestamp; a number reads epoch seconds |
| `cast(x as date)`, `cast(x as time)` | `date(x)`, `time(x)` |
| `cast(x as json)` | `json(x)`: the text when it is valid JSON, else NULL |
| `cast(x as ip)` | string |
| `tonumber(s)`, `tostring(x)` | number, string |
| `convert num(f)`, `convert dur2sec(f)`, ... | in place, see the [command guide](./command-guide.md#convert) |

A value that cannot be converted becomes NULL; conversions never fail a
query.

Implicit conversions happen only where the [IR specification](./ir.md#run-time-conversions)
defines them:

- int with int is int (`7 / 2` is 3); int with float is float (`7.0 / 2` is 3.5).
- A string meeting a number in arithmetic or a comparison is read as a
  number; it yields NULL when it does not parse.
- A string compared with a timestamp is read as a timestamp:
  `where _time >= '2025-01-15 08:05:00'`.
- `+` on two strings concatenates them.
- timestamp minus timestamp is a duration; timestamp plus or minus a
  duration is a timestamp: `_time + INTERVAL 2 HOUR`.
- Function arguments typed `string` accept other types only for `concat`,
  `concat_ws` and `mvjoin`, which convert them; other functions reject a
  mismatched static type with a `TYPE_ERROR` (`lower(5)`), while a value of
  type `any` is checked per row.

## Output formats

Converted to strings, timestamps are written `yyyy-MM-dd HH:mm:ss[.fraction]`
and durations `HH:mm:ss[.fraction]`. JSON responses write timestamps in
RFC 3339.

## Unify

When branches of `case`, arguments of `coalesce` or columns of `append` and
`union` meet, their types unify: equal types stay, NULL takes the other
type, int and float widen to float, anything else becomes `any`.
