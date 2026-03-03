package alerts

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// testWebhookFactory creates a NotifierFactory that builds webhook notifiers for testing.
func testWebhookFactory() NotifierFactory {
	return func(chType ChannelType, config map[string]interface{}) (Notifier, error) {
		url, _ := config["url"].(string)
		if url == "" {
			return nil, fmt.Errorf("url is required")
		}

		return &testNotifier{url: url}, nil
	}
}

type testNotifier struct {
	url string
}

func (n *testNotifier) Send(ctx context.Context, alert Alert, result map[string]interface{}) error {
	req, _ := http.NewRequestWithContext(ctx, "POST", n.url, http.NoBody)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode >= 400 {
		return fmt.Errorf("status %d", resp.StatusCode)
	}

	return nil
}

func (n *testNotifier) Test(ctx context.Context, alert Alert) (time.Duration, error) {
	start := time.Now()
	err := n.Send(ctx, alert, nil)

	return time.Since(start), err
}

func TestDispatcherDispatch(t *testing.T) {
	var callCount atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount.Add(1)
		w.WriteHeader(200)
	}))
	defer srv.Close()

	d := NewDispatcher(testWebhookFactory(), slog.Default())
	alert := Alert{
		Name:     "test-dispatch",
		Query:    "search error",
		Interval: "1m",
		Channels: []NotificationChannel{
			{Type: ChannelWebhook, Name: "hook1", Config: map[string]interface{}{"url": srv.URL}},
			{Type: ChannelWebhook, Name: "hook2", Config: map[string]interface{}{"url": srv.URL}},
		},
	}

	results := d.Dispatch(context.Background(), alert, nil)
	if len(results) != 2 {
		t.Fatalf("got %d results, want 2", len(results))
	}
	for _, r := range results {
		if r.Status != "ok" {
			t.Errorf("channel %s: status = %s, error = %s", r.Name, r.Status, r.Error)
		}
	}
	if c := callCount.Load(); c != 2 {
		t.Errorf("server received %d calls, want 2", c)
	}
}

func TestDispatcherSkipsDisabled(t *testing.T) {
	var callCount atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount.Add(1)
		w.WriteHeader(200)
	}))
	defer srv.Close()

	disabled := false
	d := NewDispatcher(testWebhookFactory(), slog.Default())
	alert := Alert{
		Name:     "test-skip",
		Query:    "search error",
		Interval: "1m",
		Channels: []NotificationChannel{
			{Type: ChannelWebhook, Name: "enabled", Config: map[string]interface{}{"url": srv.URL}},
			{Type: ChannelWebhook, Name: "disabled", Enabled: &disabled, Config: map[string]interface{}{"url": srv.URL}},
		},
	}

	results := d.Dispatch(context.Background(), alert, nil)
	if len(results) != 1 {
		t.Fatalf("got %d results, want 1", len(results))
	}
	if results[0].Name != "enabled" {
		t.Errorf("expected 'enabled' channel, got %q", results[0].Name)
	}
}

func TestDispatcherPartialFailure(t *testing.T) {
	goodSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	}))
	defer goodSrv.Close()

	badSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
	}))
	defer badSrv.Close()

	d := NewDispatcher(testWebhookFactory(), slog.Default())
	alert := Alert{
		Name:     "partial-fail",
		Query:    "search error",
		Interval: "1m",
		Channels: []NotificationChannel{
			{Type: ChannelWebhook, Name: "good", Config: map[string]interface{}{"url": goodSrv.URL}},
			{Type: ChannelWebhook, Name: "bad", Config: map[string]interface{}{"url": badSrv.URL}},
		},
	}

	results := d.Dispatch(context.Background(), alert, nil)
	if len(results) != 2 {
		t.Fatalf("got %d results, want 2", len(results))
	}

	statusByName := map[string]string{}
	for _, r := range results {
		statusByName[r.Name] = r.Status
	}
	if statusByName["good"] != "ok" {
		t.Error("good channel should be ok")
	}
	if statusByName["bad"] != "error" {
		t.Error("bad channel should be error")
	}
}

func TestDispatcherTestChannels(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	}))
	defer srv.Close()

	d := NewDispatcher(testWebhookFactory(), slog.Default())
	alert := Alert{
		Name:     "test-channels",
		Query:    "search error",
		Interval: "1m",
		Channels: []NotificationChannel{
			{Type: ChannelWebhook, Name: "hook", Config: map[string]interface{}{"url": srv.URL}},
		},
	}

	results := d.TestChannels(context.Background(), alert)
	if len(results) != 1 {
		t.Fatalf("got %d results, want 1", len(results))
	}
	if results[0].Status != "ok" {
		t.Errorf("status = %s, want ok; error = %s", results[0].Status, results[0].Error)
	}
	if results[0].LatencyMs < 0 {
		t.Errorf("latency should be non-negative, got %d", results[0].LatencyMs)
	}
}
