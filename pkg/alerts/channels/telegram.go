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

type telegramNotifier struct {
	botToken string
	chatID   string
}

// NewTelegram creates a Telegram Notifier.
func NewTelegram(config map[string]interface{}) (alerts.Notifier, error) {
	token, _ := config["bot_token"].(string)
	if token == "" {
		return nil, fmt.Errorf("telegram: bot_token is required")
	}
	chatID, _ := config["chat_id"].(string)
	if chatID == "" {
		return nil, fmt.Errorf("telegram: chat_id is required")
	}

	return &telegramNotifier{botToken: token, chatID: chatID}, nil
}

func (t *telegramNotifier) Send(ctx context.Context, alert alerts.Alert, result map[string]interface{}) error {
	rowCount := 0
	if rows, ok := result["rows"].([]map[string]interface{}); ok {
		rowCount = len(rows)
	}
	msg := FormatMessage(alert, rowCount)

	return t.sendMessage(ctx, msg)
}

func (t *telegramNotifier) Test(ctx context.Context, alert alerts.Alert) (time.Duration, error) {
	msg := "[TEST] " + FormatMessage(alert, 0)
	start := time.Now()
	err := t.sendMessage(ctx, msg)

	return time.Since(start), err
}

func (t *telegramNotifier) sendMessage(ctx context.Context, text string) error {
	url := fmt.Sprintf("https://api.telegram.org/bot%s/sendMessage", t.botToken)

	payload := map[string]string{
		"chat_id": t.chatID,
		"text":    text,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("telegram: marshal: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("telegram: new request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("telegram: send: %w", err)
	}
	resp.Body.Close()

	if resp.StatusCode >= 400 {
		return fmt.Errorf("telegram: unexpected status %d", resp.StatusCode)
	}

	return nil
}
