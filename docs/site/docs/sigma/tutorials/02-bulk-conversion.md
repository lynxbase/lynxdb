# Bulk conversion

[Back to Sigma docs](../index.md)

This tutorial converts a SigmaHQ rules checkout into LynxDB SPL2 query files.

Clone SigmaHQ rules:

```bash
git clone https://github.com/SigmaHQ/sigma.git sigma
```

Convert the rules:

```bash
rsigma convert -t lynxdb -r sigma/rules > all.spl2
```

Inspect the output before loading it:

```bash
head -20 all.spl2
```

Run one query manually:

```bash
sed -n '1p' all.spl2 > first.spl2
lynxdb query "$(cat first.spl2)" --since 24h
```

Import the generated file as saved queries:

```bash
lynxdb saved import all.spl2 --update-existing
lynxdb saved
```

If you keep a sidecar manifest for rule metadata, pass it during import:

```bash
lynxdb saved import all.spl2 --manifest manifest.json --update-existing
```

For one-off runs, skip saved queries and run the file directly:

```bash
lynxdb query --queries-file all.spl2 --since 24h --format ndjson
```
