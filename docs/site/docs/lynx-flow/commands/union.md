---
title: union
description: Merge incoming rows with datasets or subsearch results.
---

# union

Merge the current result set with one or more datasets or subsearch results.

## Syntax

```spl
| union <dataset> [, <dataset> ...]
| union [<subsearch>] [[<subsearch>] ...]
| union [maxout=<n>] [maxtime=<n>] [timeout=<n>] <dataset-or-subsearch> ...
```

## Examples

```spl
-- Merge rows from another source
| union other_index

-- Merge current results with a subsearch
| union [FROM other_index | where level="error"]

-- Use Splunk subsearch options
| union maxout=20000 [search error | stats count by source]
```

## Notes

- When used after an existing pipeline, `union` includes the incoming rows and appends branch rows.
- Dataset operands are parsed as source names. Subsearch operands run as independent pipelines.
- `maxout` limits rows emitted by each union branch.
- `maxtime` and `timeout` parse for compatibility but are not enforced yet.
- Splunk's `_time` interleaving behavior is not implemented; output order follows the union iterator mode.

## See Also

- [append](/docs/lynx-flow/commands/append) -- Append one subsearch
- [multisearch](/docs/lynx-flow/commands/multisearch) -- Combine independent searches
