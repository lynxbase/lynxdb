# rsigma pipelines for LynxDB

[Back to Sigma docs](index.md)

rsigma pipelines adapt a Sigma rule before conversion. For LynxDB, the common
uses are selecting an index and renaming fields so the rule matches the events
you ingest.

## Select an index

Without pipeline state, rsigma emits queries against `main`:

```spl
FROM main | search CommandLine="whoami"
```

A pipeline with `set_state index=security` changes the generated query to:

```spl
FROM security | search CommandLine="whoami"
```

Use this when your events are stored in a LynxDB index other than `main`.

## Rename fields

Sigma rules often use ECS, OCSF, or Windows event field names. Your LynxDB
events only match if those names exist at query time.

For example, if a rule tests `process.command_line` but the ingested event uses
`CommandLine`, define an rsigma field-mapping pipeline so conversion emits:

```spl
FROM security | search CommandLine=*"whoami"*
```

instead of:

```spl
FROM security | search process.command_line=*"whoami"*
```

Keep the mapping in rsigma. LynxDB receives the final SPL2 string and does not
need to know the original Sigma field name.

See [tutorial 05](tutorials/05-pipelines.md) for a copy-pasteable ECS example.
