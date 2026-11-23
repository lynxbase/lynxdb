---
title: Field Extraction
description: Which fields LynxDB extracts when it ingests events, and how to extract more at query time with parse, rex, grok, spath and regular expression functions.
---

# Field Extraction

LynxDB reads schemas on query, not on write: you never declare fields before
sending data. Fields come from two places:

- **Ingest time.** The keys of a JSON event are its fields. For a line of
  log text, the server takes the timestamp and `key=value` pairs out of the
  line and keeps the line in `_raw`.
- **Query time.** PPL commands such as `parse`, `rex`, `grok` and `spath`
  extract more fields from `_raw` or any other text field while a query runs.

## What ingestion extracts

### JSON events

An event sent as a JSON object is stored as sent. Its keys are its fields,
with their JSON types; the members of a nested object are fields named by
their path (`http.status` for `{"http":{"status":502}}`), and arrays keep
their elements. A JSON event has no `_raw`. See
[Working with JSON Logs](./json-processing.md).

### Lines of text

`POST /api/v1/ingest/raw`, `lynxdb ingest` and pipe mode turn each line that
is not a JSON object into an event:

| Field | Content |
|-------|---------|
| `_raw` | The line |
| `_time` | The timestamp found in the line, otherwise the time it arrived |
| `source` | `--source`, the `X-Source` header (default `http`), or the file name in pipe mode |
| `sourcetype` | `--sourcetype`, the `X-Source-Type` header (default `raw`) |
| `host` | The `X-Host` header, or a `host=` pair in the line |

The ingest pipeline processes each line in this order:

1. **Timestamp.** LynxDB takes the first time that starts at the beginning
   of the line or after a space, tab, quote, `[`, `(`, `>` or `=` within its
   first 100 bytes. It reads RFC 3339 with any fraction and zone
   (`2026-01-15T08:00:00Z`, `2026-01-15T08:00:00.105+01:00`,
   `2026-01-15 08:00:00,105`, UTC when the zone is missing), syslog times
   (`Jan 15 08:00:00`, in the current year) and access log times
   (`15/Jan/2026:08:00:00 +0000`); the
   [raw ingest reference](../api/ingest.md#post-ingestraw) lists the forms.
   When none is found, the event gets the time it was received.
2. **`key=value` pairs.** Every `key=value` and `key="quoted value"` pair
   becomes a field. Values that look like integers, floats or `true`/`false`
   are stored with that type, so `status >= 500` compares numbers. A key never
   overwrites a field the event already has. `host`, `source` and
   `sourcetype` fill those labels when the sender left them empty; an
   `index` pair is an ordinary field, as the sender alone chooses the index.

A line such as

```text
2026-11-17T08:00:00.105Z host=web-03 level=ERROR method=GET path=/api/v1/users status=502 user=grace response_time=786 msg="GET /api/v1/users completed"
```

arrives with `_time`, `host`, `level`, `method`, `path`, `status`, `user`,
`response_time` and `msg` already extracted.

### Shipper protocols

Elasticsearch bulk documents are stored as sent, like JSON events. Splunk HEC,
OTLP and syslog events are decoded and stored as one JSON object each: the
event text in `_raw`, its time in `_time`, `host`, `source` and
`sourcetype`, and the protocol's own fields, such as the `fields` object of
a HEC event or the attributes of an OTLP record. The ingest pipeline then
extracts `key=value` pairs from the event text, and the keys of event text
that is a JSON object. See [Ingest Data](./ingest-data.md).

### Lightweight ingest mode

With `ingest.mode: lightweight`, the pipeline of Splunk HEC, OTLP and syslog
events skips full JSON and `key=value` extraction. It keeps only `host`,
`source`, `sourcetype`, `level` and the timestamp keys of JSON event text;
everything else stays in `_raw` for query-time extraction. See
[Ingest configuration](../configuration/ingest.md).

### Pipe mode

`lynxdb query --file` and piped input load lines like raw ingest. In addition,
when most of the first 20 lines are combined access logs, syslog lines or
Docker JSON log lines, the CLI extracts those formats' fields while loading
(`client_ip`, `method`, `uri`, `status`, ... for combined). The server does
not detect these formats; use `grok` or `parse` on them. `--raw` turns
detection off. See [Pipe Mode](../getting-started/pipe-mode.md#format-detection).

## See which fields exist

On a server, `lynxdb fields` lists every field and the indexes that hold it:

```bash
lynxdb fields --source kv
```

```text
┌───────────────┬─────────┐
│ FIELD         │ INDEXES │
├───────────────┼─────────┤
│ _raw          │ kv      │
│ _time         │ kv      │
│ host          │ kv      │
│ level         │ kv      │
│ method        │ kv      │
│ msg           │ kv      │
...
```

In a query, `describe <index>` returns one row per stored column with its
type:

```
describe kv
```

```text
COLUMN_NAME,ORDINAL_POSITION,TABLE_NAME,TYPE_NAME
_raw,1,kv,string
_time,2,kv,timestamp
host,3,kv,string
level,4,kv,string
method,5,kv,string
msg,6,kv,string
path,7,kv,string
response_time,8,kv,bigint
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
| sort - attempts, user
| head 3
```

```text
attempts,user
21,admin
6,oracle
6,support
```

`parse` works on any string field, not only `_raw`. Over the JSON events of a
backend service:

```
source=main | parse path '/api/v2/(?<resource>[a-z]+)' | stats count() as n by resource | sort - n | head 3
```

```text
n,resource
8,users
5,
4,payments
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
source=nginx | grok _raw '%{COMBINEDAPACHELOG}' | stats count() as n by response | sort - n, response
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

### JSON text with `spath`

The fields of a JSON event need no extraction. `spath` reads JSON that sits
inside a text field, such as the `log` field of Docker `json-file` lines.
With `path=`, it extracts one value; `{0}` indexes an array:

```
source=main | spath input=log path=request.id output=request_id | where isnotnull(request_id) | fields _time, request_id
```

[Working with JSON Logs](./json-processing.md) covers nested fields, arrays,
`spath` and the `json_*` functions.

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

- Put search terms in the search (`source=main "Failed password"`) so the
  scan skips the blocks without the term before the regular expression runs.
- Extraction is per query. When a field is queried all the time, send it as a
  JSON key or a `key=value` pair so ingestion stores it as a column.
- `lynxdb query --file` is a fast way to try a pattern on a sample file
  before running it on a server.

## Next steps

- [Working with JSON Logs](./json-processing.md)
- [Command guide](../ppl/command-guide.md): `parse`, `rex`, `grok`, `spath`, `regex` in depth
- [Supported commands](../ppl/commands.md): support level and options of each command
- [Ingest Data](./ingest-data.md)
