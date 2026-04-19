package alerts

import (
	crypto_rand "crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"time"
)

// ChannelType identifies a notification channel backend.
type ChannelType string

const (
	ChannelWebhook  ChannelType = "webhook"
	ChannelTelegram ChannelType = "telegram"
	ChannelSlack    ChannelType = "slack"
)

var knownChannelTypes = map[ChannelType]bool{
	ChannelWebhook:  true,
	ChannelTelegram: true,
	ChannelSlack:    true,
}

// channelRequiredFields maps each channel type to its required config keys.
var channelRequiredFields = map[ChannelType][]string{
	ChannelWebhook:  {"url"},
	ChannelTelegram: {"bot_token", "chat_id"},
	ChannelSlack:    {"webhook_url"},
}

// AlertStatus describes the current state of an alert.
type AlertStatus string

const (
	StatusOK        AlertStatus = "ok"
	StatusTriggered AlertStatus = "triggered"
	StatusError     AlertStatus = "error"
)

// NotificationChannel configures a single notification destination.
type NotificationChannel struct {
	Type    ChannelType            `json:"type"`
	Name    string                 `json:"name"`
	Enabled *bool                  `json:"enabled,omitempty"`
	Config  map[string]interface{} `json:"config"`
}

// IsEnabled returns true if the channel is enabled (default true when nil).
func (c NotificationChannel) IsEnabled() bool {
	return c.Enabled == nil || *c.Enabled
}

// Alert is a persisted alert definition.
type Alert struct {
	ID            string                `json:"id"`
	Name          string                `json:"name"`
	Query         string                `json:"query"`
	Interval      string                `json:"interval"`
	Channels      []NotificationChannel `json:"channels"`
	Enabled       bool                  `json:"enabled"`
	LastTriggered *time.Time            `json:"last_triggered,omitempty"`
	LastChecked   *time.Time            `json:"last_checked,omitempty"`
	Status        AlertStatus           `json:"status"`
}

// AlertInput is the request body for creating or updating an alert.
type AlertInput struct {
	Name     string                `json:"name"`
	Query    string                `json:"query"`
	Interval string                `json:"interval"`
	Channels []NotificationChannel `json:"channels"`
	Enabled  *bool                 `json:"enabled,omitempty"`
}

// Validate checks all fields and returns the first validation error found.
func (in *AlertInput) Validate() error {
	if in.Name == "" {
		return ErrAlertNameEmpty
	}
	if in.Query == "" {
		return ErrQueryEmpty
	}

	dur, err := time.ParseDuration(in.Interval)
	if err != nil || dur < 10*time.Second {
		return ErrInvalidInterval
	}

	if len(in.Channels) == 0 {
		return ErrNoChannels
	}

	for i, ch := range in.Channels {
		if !knownChannelTypes[ch.Type] {
			return ErrUnknownChannelType
		}
		required := channelRequiredFields[ch.Type]
		for _, key := range required {
			if ch.Config == nil {
				return fmt.Errorf("channels[%d].config.%s is required for type '%s'", i, key, ch.Type)
			}
			v, ok := ch.Config[key]
			if !ok || v == nil || v == "" {
				return fmt.Errorf("channels[%d].config.%s is required for type '%s'", i, key, ch.Type)
			}
		}
	}

	return nil
}

// ToAlert converts an AlertInput to an Alert with generated ID and defaults.
func (in *AlertInput) ToAlert() *Alert {
	enabled := true
	if in.Enabled != nil {
		enabled = *in.Enabled
	}

	return &Alert{
		ID:       generateAlertID(),
		Name:     in.Name,
		Query:    in.Query,
		Interval: in.Interval,
		Channels: in.Channels,
		Enabled:  enabled,
		Status:   StatusOK,
	}
}

func generateAlertID() string {
	b := make([]byte, 8)
	if _, err := crypto_rand.Read(b); err != nil {
		// Fallback: use timestamp-based ID if crypto/rand fails.
		binary.BigEndian.PutUint64(b, uint64(time.Now().UnixNano()))
	}

	return "alt_" + hex.EncodeToString(b)
}
