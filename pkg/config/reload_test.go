package config

import (
	"testing"
	"time"
)

func TestClassifyReloadChanges(t *testing.T) {
	oldCfg := DefaultConfig()
	newCfg := *oldCfg

	newCfg.Retention = Duration(14 * 24 * time.Hour)
	newCfg.Query.MaxQueryLength = 256
	newCfg.Query.GlobalQueryPoolBytes = ByteSize(2 * GB)
	newCfg.Ingest.Mode = "lightweight"
	newCfg.Ingest.DedupEnabled = true
	newCfg.HTTP.ReadHeaderTimeout = 15 * time.Second
	newCfg.HTTP.RateLimit = 250
	newCfg.TLS.Enabled = true
	newCfg.Auth.Enabled = true
	newCfg.NoUI = true
	newCfg.Storage.CompactionWorkers = 4

	changes := ClassifyReloadChanges(oldCfg, &newCfg)

	assertContains(t, changes.HotReloaded, "retention")
	assertContains(t, changes.HotReloaded, "query.max_query_length")
	assertContains(t, changes.HotReloaded, "ingest.mode")

	assertContains(t, changes.RestartRequired, "http.read_header_timeout")
	assertContains(t, changes.RestartRequired, "query.global_query_pool_bytes")
	assertContains(t, changes.RestartRequired, "ingest.dedup_enabled")
	assertContains(t, changes.RestartRequired, "http.rate_limit")
	assertContains(t, changes.RestartRequired, "tls.enabled")
	assertContains(t, changes.RestartRequired, "auth.enabled")
	assertContains(t, changes.RestartRequired, "no_ui")
	assertContains(t, changes.RestartRequired, "storage.compaction_workers")
}

func TestClassifyReloadChangesIgnoresProfiles(t *testing.T) {
	oldCfg := DefaultConfig()
	newCfg := *oldCfg
	newCfg.Profiles = map[string]Profile{
		"prod": {URL: "https://example.com"},
	}

	changes := ClassifyReloadChanges(oldCfg, &newCfg)
	if len(changes.HotReloaded) != 0 || len(changes.RestartRequired) != 0 {
		t.Fatalf("profiles should be ignored, got hot=%v restart=%v", changes.HotReloaded, changes.RestartRequired)
	}
}

func assertContains(t *testing.T, values []string, want string) {
	t.Helper()

	for _, value := range values {
		if value == want {
			return
		}
	}

	t.Fatalf("missing %q in %v", want, values)
}
