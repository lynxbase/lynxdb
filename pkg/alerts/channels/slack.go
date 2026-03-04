package channels

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/lynxbase/lynxdb/pkg/alerts"
)

type slackNotifier struct {
	webhookURL string
}

// NewSlack creates a Slack Notifier.
func NewSlack(config map[string]interface{}) (alerts.Notifier, error) {
	url, _ := config["webhook_url"].(string)
	if url == "" {
		return nil, fmt.Errorf("slack: webhook_url is required")
	}

	return &slackNotifier{webhookURL: url}, nil
}

func (s *slackNotifier) Send(ctx context.Context, alert alerts.Alert, result map[string]interface{}) error {
	rowCount := 0
	if rows, ok := result["rows"].([]map[string]interface{}); ok {
		rowCount = len(rows)
	}
	msg := FormatMessage(alert, rowCount)

	return s.post(ctx, msg)
}

func (s *slackNotifier) Test(ctx context.Context, alert alerts.Alert) (time.Duration, error) {
	msg := "[TEST] " + FormatMessage(alert, 0)
	start := time.Now()
	err := s.post(ctx, msg)

	return time.Since(start), err
}

func (s *slackNotifier) post(ctx context.Context, text string) error {
	payload := map[string]string{"text": text}
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("slack: marshal: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", s.webhookURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("slack: new request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("slack: send: %w", err)
	}
	resp.Body.Close()

	if resp.StatusCode >= 400 {
		return fmt.Errorf("slack: unexpected status %d", resp.StatusCode)
	}

	return nil
}
