package main

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/client"
)

func TestSavedImport_WithManifestAndUpdateExisting(t *testing.T) {
	baseURL := newTestServer(t)
	dir := t.TempDir()
	queriesPath := dir + "/rules.spl2"
	manifestPath := dir + "/manifest.json"
	if err := os.WriteFile(queriesPath, []byte("FROM main | search CommandLine=\"whoami\"\n"), 0o600); err != nil {
		t.Fatalf("write queries: %v", err)
	}
	if err := os.WriteFile(manifestPath, []byte(`{
  "rsigma_version": "0.9.0",
  "queries": [
    {
      "line": 1,
      "rule_id": "win-whoami",
      "title": "Whoami Process",
      "level": "low",
      "tags": ["attack.discovery"]
    }
  ]
}`), 0o600); err != nil {
		t.Fatalf("write manifest: %v", err)
	}

	stdout, _, err := runCmd(t, "--server", baseURL, "saved", "import", queriesPath, "--manifest", manifestPath)
	if err != nil {
		t.Fatalf("saved import failed: %v", err)
	}
	envelopes := mustParseJSON(t, stdout)
	if len(envelopes) != 1 {
		t.Fatalf("expected one envelope, got %d", len(envelopes))
	}
	if envelopes[0]["name"] != "win-whoami" || envelopes[0]["result"] != "created" {
		t.Fatalf("unexpected envelope: %#v", envelopes[0])
	}

	c := client.NewClient(client.WithBaseURL(baseURL))
	saved, err := c.ListSavedQueries(context.Background())
	if err != nil {
		t.Fatalf("list saved queries: %v", err)
	}
	if len(saved) != 1 {
		t.Fatalf("saved query count = %d, want 1", len(saved))
	}
	if saved[0].Source != "rsigma" {
		t.Fatalf("source = %q, want rsigma", saved[0].Source)
	}
	if saved[0].Metadata["rule_id"] != "win-whoami" {
		b, _ := json.Marshal(saved[0].Metadata)
		t.Fatalf("metadata = %s", b)
	}

	_, _, err = runCmd(t, "--server", baseURL, "saved", "import", queriesPath, "--manifest", manifestPath)
	if err == nil {
		t.Fatal("expected conflict without --update-existing")
	}
	if !strings.Contains(err.Error(), "failed to import") {
		t.Fatalf("unexpected conflict error: %v", err)
	}

	if err := os.WriteFile(queriesPath, []byte("FROM main | search CommandLine=*\"whoami\"*\n"), 0o600); err != nil {
		t.Fatalf("rewrite queries: %v", err)
	}
	stdout, _, err = runCmd(t, "--server", baseURL, "saved", "import", queriesPath, "--manifest", manifestPath, "--update-existing")
	if err != nil {
		t.Fatalf("saved import update failed: %v", err)
	}
	envelopes = mustParseJSON(t, stdout)
	if envelopes[0]["result"] != "updated" {
		t.Fatalf("expected updated envelope, got %#v", envelopes[0])
	}
}

func TestSavedImport_NoManifestAndDryRun(t *testing.T) {
	baseURL := newTestServer(t)
	queriesPath := t.TempDir() + "/rules.spl2"
	if err := os.WriteFile(queriesPath, []byte("FROM main | search error\n"), 0o600); err != nil {
		t.Fatalf("write queries: %v", err)
	}

	stdout, _, err := runCmd(t, "--server", baseURL, "saved", "import", queriesPath, "--dry-run")
	if err != nil {
		t.Fatalf("saved import dry-run failed: %v", err)
	}
	envelopes := mustParseJSON(t, stdout)
	if envelopes[0]["name"] != "rules.spl2:1" || envelopes[0]["result"] != "dry-run" {
		t.Fatalf("unexpected dry-run envelope: %#v", envelopes[0])
	}

	c := client.NewClient(client.WithBaseURL(baseURL))
	saved, err := c.ListSavedQueries(context.Background())
	if err != nil {
		t.Fatalf("list saved queries: %v", err)
	}
	if len(saved) != 0 {
		t.Fatalf("dry-run wrote %d saved queries", len(saved))
	}
}

func TestSigmaHelperHelpText(t *testing.T) {
	queryHelp, _, err := runCmd(t, "query", "--help")
	if err != nil {
		t.Fatalf("query help: %v", err)
	}
	if !strings.Contains(queryHelp, "--queries-file") {
		t.Fatalf("query help missing --queries-file:\n%s", queryHelp)
	}

	savedHelp, _, err := runCmd(t, "saved", "--help")
	if err != nil {
		t.Fatalf("saved help: %v", err)
	}
	if !strings.Contains(savedHelp, "import") {
		t.Fatalf("saved help missing import:\n%s", savedHelp)
	}

	importHelp, _, err := runCmd(t, "saved", "import", "--help")
	if err != nil {
		t.Fatalf("saved import help: %v", err)
	}
	for _, want := range []string{"--manifest", "--dry-run", "--update-existing"} {
		if !strings.Contains(importHelp, want) {
			t.Fatalf("saved import help missing %s:\n%s", want, importHelp)
		}
	}
}
