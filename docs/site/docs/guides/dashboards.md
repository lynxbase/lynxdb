---
title: Create Dashboards
description: How to create panel-based dashboards in LynxDB with SPL2 queries, grid layout, and template variables.
---

# Create Dashboards

LynxDB dashboards are collections of panels, each powered by an SPL2 query. Panels are arranged in a 12-column grid layout and support template variables for interactive filtering. You define dashboards as JSON and manage them through the CLI or REST API.

## Dashboard structure

A dashboard is a JSON object with a name, an array of panels, and optional template variables:

```json
{
  "name": "Production Overview",
  "panels": [
    {
      "id": "p1",
      "title": "Error Rate",
      "type": "timechart",
      "q": "level=error | timechart count span=5m",
      "from": "-6h",
      "position": {"x": 0, "y": 0, "w": 6, "h": 4}
    },
    {
      "id": "p2",
      "title": "Top Error Sources",
      "type": "table",
      "q": "level=error | stats count by source | sort -count | head 10",
      "from": "-1h",
      "position": {"x": 6, "y": 0, "w": 6, "h": 4}
    }
  ],
  "variables": [
    {"name": "source", "type": "field_values", "field": "source", "default": "*"}
  ]
}
```

---

## Panel fields

Each panel has these fields:

| Field | Required | Description |
|-------|----------|-------------|
| `id` | Yes | Unique panel identifier (e.g., `"p1"`) |
| `title` | Yes | Display title shown above the panel |
| `type` | Yes | Panel type (see below) |
| `q` | Yes | SPL2 query that produces the panel data |
| `from` | No | Time range for this panel (e.g., `"-6h"`, `"-24h"`) |
| `position` | Yes | Grid position and size |

### Position object

The grid is 12 columns wide. Row height is determined by the `h` value.

| Field | Description |
|-------|-------------|
| `x` | Column start (0-11) |
| `y` | Row start (0-based) |
| `w` | Width in columns (1-12) |
| `h` | Height in row units |

### Panel types

| Type | Description | Typical query |
|------|-------------|---------------|
| `timechart` | Time series line chart | `\| timechart count span=5m` |
| `table` | Tabular results | `\| stats count by source \| sort -count` |
| `stat` | Single large number | `\| stats count` |
| `bar` | Bar chart | `\| stats count by status` |

---

## Create a dashboard via the CLI

Save your dashboard JSON to a file and create it with [`lynxdb dashboards create`](/docs/cli/alerts):

```bash
cat > dashboard.json <<'EOF'
{
  "name": "Nginx Monitoring",
  "panels": [
    {
      "id": "p1",
      "title": "Request Rate",
      "type": "timechart",
      "q": "source=nginx | timechart count span=5m",
      "from": "-6h",
      "position": {"x": 0, "y": 0, "w": 12, "h": 4}
    },
    {
      "id": "p2",
      "title": "Error Rate",
      "type": "timechart",
      "q": "source=nginx status>=500 | timechart count span=5m",
      "from": "-6h",
      "position": {"x": 0, "y": 4, "w": 6, "h": 4}
    },
    {
      "id": "p3",
      "title": "Top Slow Endpoints",
      "type": "table",
      "q": "source=nginx | stats avg(duration_ms) AS avg_lat, p99(duration_ms) AS p99_lat, count by uri | sort -avg_lat | head 10",
      "from": "-1h",
      "position": {"x": 6, "y": 4, "w": 6, "h": 4}
    },
    {
      "id": "p4",
      "title": "Total Requests",
      "type": "stat",
      "q": "source=nginx | stats count",
      "from": "-24h",
      "position": {"x": 0, "y": 8, "w": 4, "h": 2}
    },
    {
      "id": "p5",
      "title": "Error Count",
      "type": "stat",
      "q": "source=nginx status>=500 | stats count",
      "from": "-24h",
      "position": {"x": 4, "y": 8, "w": 4, "h": 2}
    },
    {
      "id": "p6",
      "title": "Avg Latency",
      "type": "stat",
      "q": "source=nginx | stats avg(duration_ms)",
      "from": "-1h",
      "position": {"x": 8, "y": 8, "w": 4, "h": 2}
    }
  ]
}
EOF

lynxdb dashboards create --file dashboard.json
```

---

## Create a dashboard via the REST API

Use [`POST /api/v1/dashboards`](/docs/api/dashboards):

```bash
curl -X POST localhost:3100/api/v1/dashboards \
  -d @dashboard.json
```

---

## Template variables

Template variables let users interactively filter all panels on a dashboard. Define them in the `variables` array.

### Field values variable

Populates a dropdown with the top values of a field:

```json
{
  "variables": [
    {"name": "source", "type": "field_values", "field": "source", "default": "*"},
    {"name": "level", "type": "field_values", "field": "level", "default": "error"}
  ]
}
```

### Using variables in queries

Reference variables with `$variable_name` in your panel queries:

```json
{
  "id": "p1",
  "title": "Events by Level",
  "type": "timechart",
  "q": "source=$source level=$level | timechart count span=5m",
  "from": "-6h",
  "position": {"x": 0, "y": 0, "w": 12, "h": 4}
}
```

When the user selects "nginx" from the source dropdown, the query becomes `source=nginx level=error | timechart count span=5m`.

---

## Manage dashboards

### List all dashboards

```bash
lynxdb dashboards
```

Or via the API:

```bash
curl -s localhost:3100/api/v1/dashboards | jq .
```

### View a dashboard

```bash
lynxdb dashboards <dashboard_id>
```

### Open in the Web UI

```bash
lynxdb dashboards open <dashboard_id>
```

### Export a dashboard

Export a dashboard definition as JSON for backup or version control:

```bash
lynxdb dashboards export <dashboard_id> > dashboard-backup.json
```

### Delete a dashboard

```bash
lynxdb dashboards delete <dashboard_id>
lynxdb dashboards delete <dashboard_id> --force   # skip confirmation
```

---

## Example: multi-service monitoring dashboard

A dashboard that monitors multiple services with a source selector:

```json
{
  "name": "Multi-Service Overview",
  "panels": [
    {
      "id": "p1",
      "title": "Error Trend",
      "type": "timechart",
      "q": "source=$source level=error | timechart count span=5m",
      "from": "-6h",
      "position": {"x": 0, "y": 0, "w": 8, "h": 4}
    },
    {
      "id": "p2",
      "title": "Error Breakdown by Source",
      "type": "bar",
      "q": "level=error | stats count by source | sort -count",
      "from": "-1h",
      "position": {"x": 8, "y": 0, "w": 4, "h": 4}
    },
    {
      "id": "p3",
      "title": "Latency Distribution",
      "type": "table",
      "q": "source=$source | stats count, avg(duration_ms) AS avg, perc50(duration_ms) AS p50, perc95(duration_ms) AS p95, perc99(duration_ms) AS p99 by uri | sort -count | head 15",
      "from": "-1h",
      "position": {"x": 0, "y": 4, "w": 12, "h": 5}
    }
  ],
  "variables": [
    {"name": "source", "type": "field_values", "field": "source", "default": "*"}
  ]
}
```

---

## Tips for effective dashboards

- **Use stat panels for KPIs**: Put key metrics (total requests, error count, p99 latency) at the top as `stat` panels for a quick at-a-glance view.
- **Use consistent time ranges**: Align time series panels to the same `from` value so trends are comparable.
- **Add template variables**: Even a single `source` variable makes dashboards vastly more useful by letting users drill into specific services.
- **Keep panel count manageable**: 6 to 10 panels is usually enough. Too many panels make dashboards slow and hard to read.
- **Export and version control**: Use `lynxdb dashboards export` to save dashboard definitions in your Git repository alongside your application code.

---

## Next steps

- [Set up alerts](/docs/guides/alerts) -- get notified when dashboard metrics cross thresholds
- [Materialized views](/docs/guides/materialized-views) -- speed up dashboard panel queries
- [Time series analysis](/docs/guides/time-series) -- write better TIMECHART queries for panels
- [REST API: Dashboards](/docs/api/dashboards) -- full API reference for dashboard CRUD
