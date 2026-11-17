---
title: Field Extraction
description: Which fields LynxDB extracts when it ingests events, and how to extract more at query time with parse, rex, grok, spath and regular expression functions.
---

# Field Extraction

LynxDB reads schemas on query, not on write: you never declare fields before
sending data. Fields come from two places:

- **Ingest time.** The ingest pipeline pulls the timestamp, JSON keys and
  `key=value` pairs out of each event and stores them as columns.
- **Query time.** PPL commands such as `parse`, `rex`, `grok` and `spath`
  extract more fields from `_raw` or any other text field while a query runs.

## What ingestion extracts

Every event has these columns:

| Column | Content |
|--------|---------|
| `_time` | Event time (`@timestamp` is an alias) |
| `_raw` | The original event text |
| `_source` | Where the event came from: `--source`, the `X-Source` header, a file path, a shipper's source |
| `_sourcetype` | Source type: `--sourcetype`, the `X-Source-Type` header, `raw` for raw HTTP lines |
| `host` | Host name, when the event or its sender provides one |
| `index` | The index the event is stored in (`main` unless chosen otherwise) |

The server's ingest pipeline then processes each line in this order:

1. **Timestamp.** LynxDB looks for a timestamp at the start of the line or
   after a space or quote within its first 100 characters. It recognizes RFC
   3339 with a numeric offset, UTC timestamps with milliseconds
   (`2026-01-15T08:00:00.105Z`), `2026-01-15 08:00:00[.000]` and syslog-style
   `Jan 15 08:00:00`. When none is found, the event gets the time it was
   received. Structured payloads carry their own time instead: the `time`
   field (epoch seconds) of `/api/v1/ingest` and Splunk HEC, `@timestamp` of
   Elasticsearch bulk documents, the record time of OTLP logs.
2. **JSON.** When the line is a JSON object, each top-level string, number
   and boolean becomes a typed field. Nested objects and arrays stay in
   `_raw`; extract them with [`spath`](#json-with-spath) or the
   [JSON functions](./json-processing.md).
3. **`key=value` pairs.** For lines that are not JSON, every `key=value` and
   `key="quoted value"` pair becomes a field. Values that look like integers,
   floats or `true`/`false` are stored with that type, so `status >= 500`
   compares numbers. The keys `host`, `source`, `sourcetype` and `index` fill
   the built-in columns. A key never overwrites a field the event already has.

A line such as

```text
2025-01-15T08:00:00.105Z host=web-03 level=ERROR method=GET path=/api/v1/users status=502 user=grace response_time=786 bytes=32228 msg="GET /api/v1/users completed"
```

arrives with `_time`, `host`, `level`, `method`, `path`, `status`, `user`,
`response_time`, `bytes` and `msg` already extracted.

Structured transports add their own fields: the `fields` object of
`POST /api/v1/ingest` and of Splunk HEC events, every key of an
Elasticsearch bulk document (nested objects are kept as object values), and
OTLP attributes (resource attributes with a `resource.` prefix). See
[Ingest Data](./ingest-data.md).

### Lightweight ingest mode

With `ingest.mode: lightweight` the server skips full JSON and `key=value`
extraction. It keeps only `host`, `source`, `sourcetype`, `index`, `level`
and the timestamp keys of JSON events; everything else stays in `_raw` for
query-time extraction. This lowers ingest CPU at the cost of slower queries
on the skipped fields. See [Ingest configuration](../configuration/ingest.md).

### Pipe mode

`lynxdb query --file` and piped input run the same pipeline. In addition,
when most sampled lines are combined access logs or syslog lines with a
`<PRI>` header, the CLI extracts those formats' fields while loading
(`client_ip`, `method`, `uri`, `status`, ... for combined; `hostname`,
`appname`, `severity_name`, `message`, ... for syslog). The server does not
detect these formats; use `grok` or `parse` on them. See
[Pipe Mode](../getting-started/pipe-mode.md#format-detection).

## See which fields exist

On a server, `lynxdb fields` lists the field catalog with types and
coverage:

```bash
lynxdb fields
```

```text
┌───────────────┬──────────┬──────────┬────────────┐
│ FIELD         │ TYPE     │ COVERAGE │ TOP VALUES │
├───────────────┼──────────┼──────────┼────────────┤
│ _raw          │ string   │      100 │            │
│ _time         │ datetime │      100 │            │
│ host          │ string   │      100 │            │
│ level         │ string   │     99.8 │            │
│ status        │ int      │     99.3 │            │
│ ...           │          │          │            │
└───────────────┴──────────┴──────────┴────────────┘
```

In a query, `describe <index>` returns one row per stored column:

```
describe main
```

```text
COLUMN_NAME,ORDINAL_POSITION,TABLE_NAME,TYPE_NAME
_raw,1,main,string
_source,2,main,string
_time,3,main,timestamp
bytes,4,main,bigint
host,5,main,string
...
```

## Extract at query time

The examples below run over an OpenSSH log (lines such as
`Dec 10 06:55:46 LabSZ sshd[24200]: Invalid user webmaster from 173.234.31.186`)
and over nginx access logs stored without combined-format extraction.

Extracted values are strings. Convert them before numeric comparisons with
`cast(x as int)`, `cast(x as double)` or `convert num(x)`.

### `parse`: named regular expression groups

`parse <field> '<regex>'` adds one field per named group `(?<name>...)`.
Rows that do not match get empty strings.

```
source=main "Invalid user"
| parse _raw 'Invalid user (?<user>\S+) from (?<ip>\S+)'
| where user != ''
| stats count() as attempts by user
| sort - attempts
| head 3
```

```text
attempts,user
21,admin
6,oracle
6,support
```

`parse` works on any string field, not only `_raw`:

```
source=main | parse path '/api/v1/(?<resource>[a-z]+)' | stats count() as n by resource
```

Regular expressions use RE2 syntax: no lookaround and no backreferences.

### `rex`: extract or rewrite

`rex field=<field> "<regex>"` extracts named groups like `parse`:

```
source=main "Invalid user"
| rex field=_raw "Invalid user (?<user>\S+) from (?<ip>\S+)"
| where ip != ''
| stats dc(user) as users, count() as attempts by ip
| sort - attempts
| head 3
```

```text
attempts,ip,users
35,103.99.0.122,15
29,187.141.143.180,24
9,183.62.140.253,8
```

With `mode=sed`, `rex` rewrites the field with an `s/regex/replacement/[g]`
expression, for example to mask addresses:

```
source=main | rex field=_raw mode=sed "s/\d+\.\d+\.\d+\.\d+/x.x.x.x/g" | fields _raw | head 2
```

```text
Dec 10 06:55:46 LabSZ sshd[24200]: reverse mapping checking getaddrinfo for ns.marryaldkfaczcz.com [x.x.x.x] failed - POSSIBLE BREAK-IN ATTEMPT!
Dec 10 06:55:46 LabSZ sshd[24200]: Invalid user webmaster from x.x.x.x
```

### `grok`: named patterns

`grok <field> '<pattern>'` matches with `%{PATTERN:name}` building blocks from
a built-in library (`IP`, `WORD`, `NOTSPACE`, `NUMBER`, `POSINT`,
`HOSTNAME`, `HTTPDATE`, `SYSLOGTIMESTAMP`, `GREEDYDATA`, `COMMONAPACHELOG`,
`COMBINEDAPACHELOG` and more). Custom pattern definitions are not supported.

```
source=main
| grok _raw '%{SYSLOGTIMESTAMP:ts} %{HOSTNAME:host} %{WORD:program}\[%{POSINT:pid}\]: %{GREEDYDATA:message}'
| where like(message, 'Failed password%')
| stats count() as failures by host
```

```text
host,failures
LabSZ,518
```

A whole access log line in one pattern:

```
source=nginx | grok _raw '%{COMBINEDAPACHELOG}' | stats count() as n by response | sort - n
```

```text
n,response
16,200
6,401
3,400
3,403
...
```

`COMBINEDAPACHELOG` yields `clientip`, `ident`, `auth`, `timestamp`, `verb`,
`request`, `httpversion`, `response`, `bytes`, `referrer` and `agent`.

### JSON with `spath`

`spath` reads JSON text. With `path=`, it extracts one value; `{0}` indexes an
array:

```
source=main | spath path=user.id output=user_id | stats count() as requests by user_id
```

Without `path=`, it parses the whole document into an object column, and
`flatten` turns its members into dotted columns:

```
source=main | spath output=doc | flatten doc | fields service, user.id, user.plan
```

[Working with JSON Logs](./json-processing.md) covers `spath`, arrays and the
`json_*` functions.

### Filter rows with `regex`

The `regex` command keeps rows whose field matches (`=`) or does not match
(`!=`) a regular expression:

```
source=main | regex _raw = 'Failed password' | stats count() as failures
source=main | regex _raw != 'pam_unix' | stats count() as n
```

### Regular expression functions

Inside `eval` and `where`:

| Function | Result |
|----------|--------|
| `regexp_match(s, re)` | true when `s` matches `re` |
| `regexp_replace(s, re, repl)` | `s` with every match replaced; `$1` refers to a group |
| `replace(s, re, repl)` | same as `regexp_replace` |
| `like(s, pattern)` | SQL `LIKE` with `%` and `_` |
| `split(s, sep)` | array of the parts of `s` |

```
source=main
| where regexp_match(_raw, 'Failed password for (invalid user )?root')
| stats count() as root_failures
```

```
source=main "Failed password"
| eval ip = regexp_replace(_raw, '.* from (\S+) port .*', '$1')
| top 3 ip
```

```text
count,ip
286,183.62.140.253
80,187.141.143.180
46,103.99.0.122
```

See the [function guide](../ppl/function-guide.md) for string, conversion
and date functions.

## Extract, convert, aggregate

Extracted strings need a cast before arithmetic. This query pulls the port
out of failed logins and reports the lowest port per address:

```
source=main "Failed password"
| parse _raw 'from (?<ip>\d+\.\d+\.\d+\.\d+) port (?<port>\d+)'
| eval port = cast(port as int)
| stats count() as n, min(port) as min_port by ip
| sort - n
| head 3
```

```text
ip,min_port,n
183.62.140.253,32826,286
187.141.143.180,33314,80
103.99.0.122,49289,46
```

On access logs, `convert` casts several fields at once:

```
source=nginx
| parse _raw '"(?<method>[A-Z]+) (?<uri>\S+) [^"]*" (?<status>\d{3}) (?<bytes>\d+)'
| convert num(status), num(bytes)
| where status >= 500
| stats count() as errors, sum(bytes) as bytes by uri
```

## Tips

- Put search terms in the source command (`source=main "Failed password"`)
  so the term index narrows the events before the regular expression runs.
- Extraction is per query. When a field is queried all the time, send it as a
  structured field or a `key=value` pair so ingestion stores it as a column.
- `lynxdb query --file` is a fast way to try a pattern on a sample file
  before running it on a server.

## Next steps

- [Working with JSON Logs](./json-processing.md)
- [Command guide](../ppl/command-guide.md): `parse`, `rex`, `grok`, `spath`, `regex` in depth
- [Supported commands](../ppl/commands.md): support level and options of each command
- [Ingest Data](./ingest-data.md)
