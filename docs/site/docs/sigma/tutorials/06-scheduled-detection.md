# Scheduled detection

[Back to Sigma docs](../index.md)

LynxDB executes SPL2 queries. Scheduling stays outside LynxDB.

Convert a rule:

```bash
rsigma convert -t lynxdb whoami.yml > whoami.spl2
```

Create a script:

```bash
cat > run-whoami.sh <<'SH'
#!/bin/sh
set -eu

query="$(cat whoami.spl2)"
lynxdb query "$query" --since 5m --format ndjson \
  | while IFS= read -r event; do
      printf '%s\n' "$event"
    done
SH
chmod +x run-whoami.sh
```

Run it every five minutes with cron:

```cron
*/5 * * * * /path/to/run-whoami.sh >> /var/log/lynxdb-sigma.log 2>&1
```

For multiple rules, keep one `.spl2` file per rule or run a query file:

```bash
lynxdb query --queries-file rules.spl2 --since 5m --format ndjson
```
