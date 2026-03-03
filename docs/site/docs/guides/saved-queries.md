---
title: Save and Reuse Queries
description: How to save, list, run, and manage saved queries in LynxDB for repeatable analysis.
---

# Save and Reuse Queries

Saved queries let you store frequently used SPL2 queries on the server so you can re-run them by name instead of retyping them. This is useful for standardized reports, on-call runbooks, and team-shared analysis.

## Save a query

Use [`lynxdb save`](/docs/cli/shortcuts) (shortcut for `lynxdb saved create`):

```bash
lynxdb save "5xx-rate" 'source=nginx status>=500 | stats count by uri | sort -count'
```

Or use the full form:

```bash
lynxdb saved create "error-by-source" 'level=error | stats count by source | sort -count | head 10'
```

### Save via the REST API

Use [`POST /api/v1/queries`](/docs/api/saved-queries):

```bash
curl -X POST localhost:3100/api/v1/queries -d '{
  "name": "5xx-rate",
  "q": "source=nginx status>=500 | stats count by uri | sort -count"
}'
```

---

## Run a saved query

Use [`lynxdb run`](/docs/cli/shortcuts) (shortcut for `lynxdb saved run`):

```bash
lynxdb run 5xx-rate
```

### Override the time range

```bash
lynxdb run 5xx-rate --since 24h
lynxdb run 5xx-rate --from 2026-01-15T00:00:00Z --to 2026-01-15T23:59:59Z
```

### Change the output format

```bash
lynxdb run 5xx-rate --format table
lynxdb run 5xx-rate --format csv > report.csv
```

### Run via the REST API

```bash
curl -s localhost:3100/api/v1/queries/5xx-rate/run -d '{"from": "-24h"}' | jq .
```

Saved query names support tab completion in the CLI shell.

---

## List saved queries

```bash
lynxdb saved
```

Or via the API:

```bash
curl -s localhost:3100/api/v1/queries | jq .
```

---

## Delete a saved query

```bash
lynxdb saved delete 5xx-rate
lynxdb saved delete 5xx-rate --force   # skip confirmation
```

Or via the API:

```bash
curl -X DELETE localhost:3100/api/v1/queries/5xx-rate
```

---

## Practical examples

### On-call runbook queries

Save the queries your on-call team uses most often:

```bash
# Error overview
lynxdb save "oncall-errors" 'level=error | stats count by source | sort -count | head 20'

# Slow endpoints
lynxdb save "oncall-slow-endpoints" 'source=nginx duration_ms>1000 | stats count, avg(duration_ms) AS avg, p99(duration_ms) AS p99 by uri | sort -count | head 10'

# Recent fatal errors
lynxdb save "oncall-fatal" 'level=fatal | sort -_timestamp | head 20 | table _timestamp, source, message'

# Run during an incident
lynxdb run oncall-errors --since 1h
lynxdb run oncall-slow-endpoints --since 15m
lynxdb run oncall-fatal --since 1h
```

### Daily reports

```bash
# Save the report query
lynxdb save "daily-summary" 'source=nginx | stats count, count(eval(status>=500)) AS errors, avg(duration_ms) AS avg_lat by uri | eval error_rate=round(errors/count*100, 1) | sort -count | head 20'

# Generate a daily CSV
lynxdb run daily-summary --since 24h --format csv > "report-$(date +%Y-%m-%d).csv"
```

### CI/CD integration

Use saved queries in CI pipelines to check for regressions:

```bash
# Save a health check query
lynxdb save "ci-error-check" 'level=error source=api | stats count AS errors | where errors > 0'

# In your CI script
if lynxdb run ci-error-check --since 10m --fail-on-empty 2>/dev/null; then
  echo "FAIL: Errors detected after deployment"
  exit 1
fi
```

---

## Tips

- **Use descriptive names**: Names like `5xx-rate`, `oncall-errors`, `daily-summary` are easier to remember than generic names.
- **Share with your team**: Saved queries are stored on the server and accessible to anyone with access. Use them as a team knowledge base for common analysis patterns.
- **Combine with `lynxdb last`**: The [`lynxdb last`](/docs/cli/shortcuts) command re-runs your most recently executed query with optional time range overrides, which pairs well with saved queries.

---

## Next steps

- [Search and filter logs](/docs/guides/search-and-filter) -- write effective queries to save
- [Run aggregations](/docs/guides/aggregations) -- build aggregation queries worth saving
- [CLI: shortcuts](/docs/cli/shortcuts) -- `save`, `run`, `last` and other quick-access commands
- [REST API: Saved queries](/docs/api/saved-queries) -- full API reference for query CRUD
