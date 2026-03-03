package channels

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/OrlovEvgeny/Lynxdb/pkg/alerts"
)

var httpClient = &http.Client{Timeout: 10 * time.Second}

type webhookNotifier struct {
	url     string
	method  string
	headers map[string]string
}

// NewWebhook creates a webhook Notifier.
func NewWebhook(config map[string]interface{}) (alerts.Notifier, error) {
	url, _ := config["url"].(string)
	if url == "" {
		return nil, fmt.Errorf("webhook: url is required")
	}
	method, _ := config["method"].(string)
	if method == "" {
		method = "POST"
	}
	headers := make(map[string]string)
	if h, ok := config["headers"].(map[string]interface{}); ok {
		for k, v := range h {
			if s, ok := v.(string); ok {
				headers[k] = s
			}
		}
	}

	return &webhookNotifier{url: url, method: method, headers: headers}, nil
}

func (w *webhookNotifier) Send(ctx context.Context, alert alerts.Alert, result map[string]interface{}) error {
	rowCount := 0
	if rows, ok := result["rows"].([]map[string]interface{}); ok {
		rowCount = len(rows)
	}
	msg := FormatMessage(alert, rowCount)

	payload := map[string]interface{}{
		"alert":   alert.Name,
		"message": msg,
		"result":  result,
	}

	return w.post(ctx, payload)
}

func (w *webhookNotifier) Test(ctx context.Context, alert alerts.Alert) (time.Duration, error) {
	payload := map[string]interface{}{
		"alert":   alert.Name,
		"message": "[TEST] " + FormatMessage(alert, 0),
	}
	start := time.Now()
	err := w.post(ctx, payload)

	return time.Since(start), err
}

func (w *webhookNotifier) post(ctx context.Context, payload map[string]interface{}) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("webhook: marshal: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, w.method, w.url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("webhook: new request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range w.headers {
		req.Header.Set(k, v)
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("webhook: send: %w", err)
	}
	resp.Body.Close()

	if resp.StatusCode >= 400 {
		return fmt.Errorf("webhook: unexpected status %d", resp.StatusCode)
	}

	return nil
}
