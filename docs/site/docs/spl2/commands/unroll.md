---
title: unroll
description: Explode a JSON array field into multiple rows.
---

# unroll

Explode a field containing a JSON array into multiple rows -- one row per array element. Non-array values pass through unchanged.

## Syntax

```spl
| unroll field=<field>
```

## Arguments

| Argument | Default | Description |
|----------|---------|-------------|
| `field` | Required | Field containing a JSON array to explode |

## Examples

```spl
-- Explode an array of objects
-- Input row: { "order": "ORD-1", "items": [{"sku":"A1","qty":2},{"sku":"B3","qty":1}] }
| json | unroll field=items
-- Output: 2 rows, each with order="ORD-1" plus items.sku, items.qty

-- Explode an array of scalars
-- Input row: { "name": "alice", "tags": ["admin","user","dev"] }
| json | unroll field=tags
-- Output: 3 rows, each with name="alice" and tags set to one value

-- Chain with aggregation
| json | unroll field=items | stats sum(items.qty) by items.sku

-- Multi-level unroll
| json | unroll field=orders | unroll field=orders.items
```

### E-commerce order analysis

```spl
-- Find top-selling SKUs across all orders
| json
| unroll field=items
| stats sum(items.qty) AS total_sold, dc(order_id) AS order_count by items.sku
| sort -total_sold
| head 20
```

### Tag-based analysis

```spl
-- Count events by tag
| json
| unroll field=tags
| stats count by tags
| sort -count
```

## Notes

- **Object elements**: When an array element is a JSON object, its keys are flattened with dot-notation. For `field=items` and element `{"sku":"A1","qty":2}`, the output row gets `items.sku="A1"` and `items.qty=2`.
- **Scalar elements**: When an array element is a string, number, or boolean, it replaces the field value directly.
- **Non-array values**: If the field doesn't contain a JSON array, the row passes through unchanged.
- **Empty arrays**: Rows with empty arrays (`[]`) pass through unchanged.
- **Null/missing fields**: Rows where the field is missing or null pass through unchanged.
- `unroll` changes the row count (one input row can produce N output rows), so it cannot be pushed through sort or head optimizations.

## See Also

- [json](/docs/spl2/commands/json-cmd) -- Extract JSON fields before unrolling
- [unpack_json](/docs/spl2/commands/unpack-json) -- Full JSON extraction with prefix support
- [Working with JSON Logs](/docs/guides/json-processing) -- Comprehensive JSON processing guide
