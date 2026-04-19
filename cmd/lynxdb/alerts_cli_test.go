package main

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/client"
)

func TestAlertsCreateFromFile(t *testing.T) {
	baseURL := newTestServer(t)
	alertPath := testdataPath("alerts/webhook_alert.json")

	_, _, err := runCmd(t, "--server", baseURL, "alerts", "create", "--file", alertPath)
	if err != nil {
		t.Fatalf("alerts create --file failed: %v", err)
	}

	stdout, _, err := runCmd(t, "--server", baseURL, "alerts", "--format", "json")
	if err != nil {
		t.Fatalf("alerts list failed: %v", err)
	}

	line := strings.TrimSpace(stdout)
	if line == "" {
		t.Fatal("expected JSON alert output, got empty stdout")
	}

	var alert client.Alert
	if err := json.Unmarshal([]byte(line), &alert); err != nil {
		t.Fatalf("decode alert JSON: %v", err)
	}
	if alert.Name != "CLI file alert" {
		t.Fatalf("name = %q, want %q", alert.Name, "CLI file alert")
	}
	if alert.Q != `FROM main | stats count | where count > 0` {
		t.Fatalf("query = %q", alert.Q)
	}
}

func TestAlertsCreateLegacyFlagsReturnLocalError(t *testing.T) {
	_, _, err := runCmd(t, "alerts", "create", "--name", "legacy", "--query", "FROM main | stats count")
	if err == nil {
		t.Fatal("expected legacy inline create flags to fail")
	}
	if !strings.Contains(err.Error(), "requires --file <alert.json>") {
		t.Fatalf("error = %q, want migration hint", err.Error())
	}
}

func TestAlertsEnableDisable(t *testing.T) {
	baseURL := newTestServer(t)
	c := client.NewClient(client.WithBaseURL(baseURL))
	ctx := context.Background()

	alert, err := c.CreateAlert(ctx, client.AlertInput{
		Name:     "toggle-via-cli",
		Q:        `FROM main | stats count`,
		Interval: "1m",
		Channels: []client.NotificationChannel{
			{Type: "webhook", Config: map[string]interface{}{"url": "https://example.com/hook"}},
		},
	})
	if err != nil {
		t.Fatalf("CreateAlert: %v", err)
	}

	if _, _, err := runCmd(t, "--server", baseURL, "alerts", "disable", alert.ID); err != nil {
		t.Fatalf("alerts disable failed: %v", err)
	}
	disabled, err := c.GetAlert(ctx, alert.ID)
	if err != nil {
		t.Fatalf("GetAlert after disable: %v", err)
	}
	if disabled.Enabled {
		t.Fatal("expected alert to be disabled")
	}

	if _, _, err := runCmd(t, "--server", baseURL, "alerts", "enable", alert.ID); err != nil {
		t.Fatalf("alerts enable failed: %v", err)
	}
	enabled, err := c.GetAlert(ctx, alert.ID)
	if err != nil {
		t.Fatalf("GetAlert after enable: %v", err)
	}
	if !enabled.Enabled {
		t.Fatal("expected alert to be enabled")
	}
}
