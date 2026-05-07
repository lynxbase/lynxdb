# Sigma troubleshooting

[Back to Sigma docs](index.md)

| Symptom | Diagnosis | Fix |
|---|---|---|
| `rsigma emitted a regex; my query is slow` | The query uses `| where field =~ "pattern"`, which may require scanning candidate rows. | Prefer exact, contains, startswith, or endswith rules when possible. For `_raw` regex searches, turn on the inverted index for `_raw` so literal extraction can reduce scans. |
| `rsigma says rule X is unsupported` | rsigma could not convert the Sigma construct for the LynxDB backend. | Check the upstream rsigma issue tracker and file a minimal rule if one does not exist: [rsigma issues](https://github.com/timescale/rsigma/issues). |
| `My index isn't main` | rsigma defaults to `FROM main` unless a pipeline sets another index. | Add a pipeline with `set_state index=security`; see [pipelines](pipelines.md). |
| A converted query returns no rows | Field names in the Sigma rule do not match the ingested event shape. | Use an rsigma field-mapping pipeline, or adjust ingestion so fields match the rule pack. |
| A CIDR rule misses IPv6 events | IPv6 CIDR edge cases are listed as a current limitation. | Track the limitation in [limitations](limitations.md) and keep a rule-specific regression case when support changes. |

The first check is always to inspect the SPL2 rsigma emitted:

```bash
rsigma convert -t lynxdb rule.yml
```

Then run the same query directly through LynxDB:

```bash
lynxdb query "$(rsigma convert -t lynxdb rule.yml)"
```
