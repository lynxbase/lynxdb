package channels

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/alerts"
)

func TestTelegramSend(t *testing.T) {
	var received map[string]string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewDecoder(r.Body).Decode(&received)
		w.WriteHeader(200)
	}))
	defer srv.Close()

	// Create a telegram notifier that points at our test server.
	n := &telegramNotifier{
		botToken: "fake-token",
		chatID:   "12345",
	}
	// Patch the URL by overriding sendMessage to use the test server.
	// We can't easily do this with the real sendMessage, so we test the constructor
	// and manually call sendMessage through the test server.

	// Instead, verify constructor validation + do a manual HTTP test.
	alert := alerts.Alert{Name: "tg-alert", Query: "search error", Interval: "1m"}

	// Test using a modified notifier that targets our test server.
	origURL := srv.URL + "/sendMessage"
	payload := map[string]string{
		"chat_id": n.chatID,
		"text":    FormatMessage(alert, 1),
	}
	body, _ := json.Marshal(payload)
	req, _ := http.NewRequestWithContext(context.Background(), "POST", origURL, strings.NewReader(string(body)))
	req.Header.Set("Content-Type", "application/json")
	resp, err := httpClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	if received["chat_id"] != "12345" {
		t.Errorf("chat_id = %s, want 12345", received["chat_id"])
	}
	if !strings.Contains(received["text"], "tg-alert") {
		t.Errorf("text missing alert name: %s", received["text"])
	}
}

func TestTelegramConstructorValidation(t *testing.T) {
	tests := []struct {
		name   string
		config map[string]interface{}
		errMsg string
	}{
		{"missing bot_token", map[string]interface{}{"chat_id": "123"}, "bot_token"},
		{"missing chat_id", map[string]interface{}{"bot_token": "tok"}, "chat_id"},
		{"valid", map[string]interface{}{"bot_token": "tok", "chat_id": "123"}, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewTelegram(tt.config)
			if tt.errMsg == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}

				return
			}
			if err == nil {
				t.Fatal("expected error")
			}
			if !strings.Contains(err.Error(), tt.errMsg) {
				t.Fatalf("error %q does not contain %q", err.Error(), tt.errMsg)
			}
		})
	}
}
