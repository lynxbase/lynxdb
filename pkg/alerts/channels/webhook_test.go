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

func TestWebhookSend(t *testing.T) {
	var received map[string]interface{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			t.Errorf("method = %s, want POST", r.Method)
		}
		if ct := r.Header.Get("Content-Type"); ct != "application/json" {
			t.Errorf("Content-Type = %s, want application/json", ct)
		}
		json.NewDecoder(r.Body).Decode(&received)
		w.WriteHeader(200)
	}))
	defer srv.Close()

	n, err := NewWebhook(map[string]interface{}{"url": srv.URL})
	if err != nil {
		t.Fatal(err)
	}

	alert := alerts.Alert{Name: "test-alert", Query: "search error", Interval: "1m"}
	result := map[string]interface{}{"rows": []map[string]interface{}{{"count": 5}}}
	if err := n.Send(context.Background(), alert, result); err != nil {
		t.Fatal(err)
	}

	if received["alert"] != "test-alert" {
		t.Errorf("alert = %v, want test-alert", received["alert"])
	}
	msg, _ := received["message"].(string)
	if !strings.Contains(msg, "test-alert") {
		t.Errorf("message missing alert name: %s", msg)
	}
}

func TestWebhookTest(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]interface{}
		json.NewDecoder(r.Body).Decode(&body)
		msg, _ := body["message"].(string)
		if !strings.HasPrefix(msg, "[TEST]") {
			t.Errorf("test message should start with [TEST], got: %s", msg)
		}
		w.WriteHeader(200)
	}))
	defer srv.Close()

	n, _ := NewWebhook(map[string]interface{}{"url": srv.URL})
	alert := alerts.Alert{Name: "test-alert", Query: "search error", Interval: "1m"}
	latency, err := n.Test(context.Background(), alert)
	if err != nil {
		t.Fatal(err)
	}
	if latency <= 0 {
		t.Errorf("latency should be positive, got %v", latency)
	}
}

func TestWebhookError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
	}))
	defer srv.Close()

	n, _ := NewWebhook(map[string]interface{}{"url": srv.URL})
	alert := alerts.Alert{Name: "test-alert"}
	if err := n.Send(context.Background(), alert, nil); err == nil {
		t.Fatal("expected error for 500 status")
	}
}

func TestWebhookCustomHeaders(t *testing.T) {
	var gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		w.WriteHeader(200)
	}))
	defer srv.Close()

	n, _ := NewWebhook(map[string]interface{}{
		"url":     srv.URL,
		"headers": map[string]interface{}{"Authorization": "Bearer token123"},
	})
	n.Send(context.Background(), alerts.Alert{Name: "t"}, nil)
	if gotAuth != "Bearer token123" {
		t.Errorf("Authorization = %q, want %q", gotAuth, "Bearer token123")
	}
}

func TestWebhookMissingURL(t *testing.T) {
	_, err := NewWebhook(map[string]interface{}{})
	if err == nil {
		t.Fatal("expected error for missing url")
	}
}
