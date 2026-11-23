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
	newCfg.Query.MaxConcurrent = 64
	newCfg.Query.JobTTL = time.Hour
	newCfg.Ingest.Mode = "lightweight"
	newCfg.Ingest.MaxLineBytes = 4096
	newCfg.Ingest.ESCompat.AdvertisedVersion = "8.17.0"
	newCfg.Ingest.Limits.MaxCompressedBodyBytes = 64 * MB
	newCfg.Ingest.SplunkHEC.Enabled = false
	newCfg.HTTP.ReadHeaderTimeout = 15 * time.Second
	newCfg.HTTP.RateLimit = 250
	newCfg.TLS.Enabled = true
	newCfg.Auth.Enabled = true
	newCfg.Storage.Partition = Duration(time.Hour)
	newCfg.Storage.TimeFields = []string{"ts"}

	changes := ClassifyReloadChanges(oldCfg, &newCfg)

	assertContains(t, changes.HotReloaded, "query.max_query_length")
	assertContains(t, changes.HotReloaded, "ingest.max_line_bytes")
	assertContains(t, changes.HotReloaded, "ingest.splunk_hec.enabled")

	assertContains(t, changes.RestartRequired, "retention")
	assertContains(t, changes.RestartRequired, "query.max_concurrent")
	assertContains(t, changes.RestartRequired, "query.job_ttl")
	assertContains(t, changes.RestartRequired, "ingest.mode")
	assertContains(t, changes.RestartRequired, "ingest.es_compat.advertised_version")
	assertContains(t, changes.RestartRequired, "ingest.limits.max_compressed_body_bytes")

	assertContains(t, changes.RestartRequired, "http.read_header_timeout")
	assertContains(t, changes.RestartRequired, "http.rate_limit")
	assertContains(t, changes.RestartRequired, "tls.enabled")
	assertContains(t, changes.RestartRequired, "auth.enabled")
	assertContains(t, changes.RestartRequired, "storage.partition")
	assertContains(t, changes.RestartRequired, "storage.time_fields")
}

func TestReloadKeepsSettingsThatNeedARestart(t *testing.T) {
	oldCfg := DefaultConfig()
	newCfg := *oldCfg
	newCfg.Listen = "0.0.0.0:1"
	newCfg.Retention = Duration(time.Hour)
	newCfg.LogLevel = "debug"
	newCfg.Query.SyncTimeout = time.Second
	newCfg.Query.MaxConcurrent = 1
	newCfg.Storage.TimeFields = []string{"ts"}

	applied, changes := Reload(oldCfg, &newCfg)

	if applied.LogLevel != "debug" || applied.Query.SyncTimeout != time.Second {
		t.Errorf("hot settings not applied: log_level %q, query.sync_timeout %v", applied.LogLevel, applied.Query.SyncTimeout)
	}
	if applied.Listen != oldCfg.Listen || applied.Retention != oldCfg.Retention ||
		applied.Query.MaxConcurrent != oldCfg.Query.MaxConcurrent || len(applied.Storage.TimeFields) != len(oldCfg.Storage.TimeFields) {
		t.Errorf("restart settings changed: %+v", applied)
	}
	for _, field := range []string{"listen", "retention", "query.max_concurrent", "storage.time_fields"} {
		assertContains(t, changes.RestartRequired, field)
	}
	if oldCfg.LogLevel != "info" {
		t.Errorf("Reload changed the old config: log_level %q", oldCfg.LogLevel)
	}
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
