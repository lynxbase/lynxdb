package channels

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/OrlovEvgeny/Lynxdb/pkg/alerts"
)

func TestSlackSend(t *testing.T) {
	var received map[string]string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewDecoder(r.Body).Decode(&received)
		w.WriteHeader(200)
	}))
	defer srv.Close()

	n, err := NewSlack(map[string]interface{}{"webhook_url": srv.URL})
	if err != nil {
		t.Fatal(err)
	}

	alert := alerts.Alert{Name: "slack-alert", Query: "search error", Interval: "1m"}
	if err := n.Send(context.Background(), alert, nil); err != nil {
		t.Fatal(err)
	}

	if !strings.Contains(received["text"], "slack-alert") {
		t.Errorf("text missing alert name: %s", received["text"])
	}
}

func TestSlackTest(t *testing.T) {
	var received map[string]string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewDecoder(r.Body).Decode(&received)
		w.WriteHeader(200)
	}))
	defer srv.Close()

	n, _ := NewSlack(map[string]interface{}{"webhook_url": srv.URL})
	alert := alerts.Alert{Name: "slack-test", Query: "search error", Interval: "1m"}
	latency, err := n.Test(context.Background(), alert)
	if err != nil {
		t.Fatal(err)
	}
	if latency <= 0 {
		t.Error("latency should be positive")
	}
	if !strings.HasPrefix(received["text"], "[TEST]") {
		t.Errorf("test message should start with [TEST], got: %s", received["text"])
	}
}

func TestSlackMissingURL(t *testing.T) {
	_, err := NewSlack(map[string]interface{}{})
	if err == nil {
		t.Fatal("expected error for missing webhook_url")
	}
}
