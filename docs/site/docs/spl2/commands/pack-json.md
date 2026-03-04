---
title: pack_json
description: Assemble event fields into a JSON string.
---

# pack_json

Assemble one or more event fields into a JSON string stored in a target field. This is the inverse of `unpack_json` -- it constructs JSON from structured fields.

## Syntax

```spl
| pack_json [<field1>, <field2>, ...] into <target>
```

## Arguments

| Argument | Default | Description |
|----------|---------|-------------|
| `field1, field2, ...` | (all non-internal) | Specific fields to include in the JSON object. When omitted, all non-internal fields are packed. |
| `target` | (required) | Output field name for the JSON string |

## Examples

```spl
-- Pack specific fields into a JSON envelope
| pack_json level, service, host into payload

-- Pack all non-internal fields
| pack_json into output

-- Create a JSON summary for forwarding
| stats count, avg(duration_ms) as avg_dur by service
| pack_json service, count, avg_dur into summary

-- Round-trip: extract then repack
| unpack_json
| where level="error"
| pack_json level, service, message into filtered_json
```

## Notes

- **Internal fields** (`_time`, `_raw`, `_source`, and any field starting with `_`) are excluded when no explicit field list is provided.
- Field values preserve their native JSON types: integers become JSON numbers, booleans become JSON booleans, null values are omitted from the output.
- Null or missing fields are silently skipped (they don't appear in the output JSON).
- The `target` field is not included in its own output when packing all fields (avoids circular reference).
- `pack_json` is a streaming operator -- it processes events one at a time without buffering.

## See Also

- [unpack_json](/docs/spl2/commands/unpack-json) -- Extract fields from JSON
- [json command](/docs/spl2/commands/json-cmd) -- Quick JSON field extraction
- [json_object function](/docs/spl2/functions/json-functions) -- Build JSON in eval expressions
- [Working with JSON Logs](/docs/guides/json-processing) -- Comprehensive JSON processing guide
