# Format Downgrade Runbook

A data directory cannot be converted to an older format in place.

Releases from v0.1.8 on read formats 1 and 2 and write format 2, so moving between any of them needs no format step: stop the server, install the other binary, start it.

Rolling back to v0.1.6 or v0.1.7, which read only format 1, fails at startup once a newer release has written format 2 parts or compaction outputs. To roll back that far:

1. Stop LynxDB.
2. Move the data directory aside.
3. Install the older binary.
4. Restore a backup taken with that older binary, or start with an empty data directory and re-ingest from the original log sources.

Releases before v0.1.6 cannot read format 1 or 2 data at all; the same procedure applies.
