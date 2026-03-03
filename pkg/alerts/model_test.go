package alerts

import (
	"strings"
	"testing"
)

func TestAlertInputValidate(t *testing.T) {
	boolPtr := func(v bool) *bool { return &v }

	validInput := func() AlertInput {
		return AlertInput{
			Name:     "high-error-rate",
			Query:    `search index=main level=error | stats count`,
			Interval: "1m",
			Channels: []NotificationChannel{
				{Type: ChannelWebhook, Name: "ops", Config: map[string]interface{}{"url": "https://example.com/hook"}},
			},
		}
	}

	tests := []struct {
		name    string
		modify  func(*AlertInput)
		wantErr string
	}{
		{
			name:   "valid input",
			modify: func(in *AlertInput) {},
		},
		{
			name:    "empty name",
			modify:  func(in *AlertInput) { in.Name = "" },
			wantErr: "name must not be empty",
		},
		{
			name:    "empty query",
			modify:  func(in *AlertInput) { in.Query = "" },
			wantErr: "query must not be empty",
		},
		{
			name:    "invalid interval format",
			modify:  func(in *AlertInput) { in.Interval = "abc" },
			wantErr: "interval must be a valid duration",
		},
		{
			name:    "interval too short",
			modify:  func(in *AlertInput) { in.Interval = "5s" },
			wantErr: "interval must be a valid duration",
		},
		{
			name:    "no channels",
			modify:  func(in *AlertInput) { in.Channels = nil },
			wantErr: "at least one channel",
		},
		{
			name: "unknown channel type",
			modify: func(in *AlertInput) {
				in.Channels = []NotificationChannel{{Type: "smoke_signal", Config: map[string]interface{}{}}}
			},
			wantErr: "unknown channel type",
		},
		{
			name: "missing webhook url",
			modify: func(in *AlertInput) {
				in.Channels = []NotificationChannel{{Type: ChannelWebhook, Config: map[string]interface{}{}}}
			},
			wantErr: "channels[0].config.url is required for type 'webhook'",
		},
		{
			name: "missing telegram bot_token",
			modify: func(in *AlertInput) {
				in.Channels = []NotificationChannel{{Type: ChannelTelegram, Config: map[string]interface{}{"chat_id": "123"}}}
			},
			wantErr: "channels[0].config.bot_token is required for type 'telegram'",
		},
		{
			name: "missing telegram chat_id",
			modify: func(in *AlertInput) {
				in.Channels = []NotificationChannel{{Type: ChannelTelegram, Config: map[string]interface{}{"bot_token": "tok"}}}
			},
			wantErr: "channels[0].config.chat_id is required for type 'telegram'",
		},
		{
			name: "missing slack webhook_url",
			modify: func(in *AlertInput) {
				in.Channels = []NotificationChannel{{Type: ChannelSlack, Config: map[string]interface{}{}}}
			},
			wantErr: "channels[0].config.webhook_url is required for type 'slack'",
		},
		{
			name: "second channel invalid",
			modify: func(in *AlertInput) {
				in.Channels = append(in.Channels, NotificationChannel{Type: ChannelTelegram, Config: map[string]interface{}{}})
			},
			wantErr: "channels[1].config.bot_token is required for type 'telegram'",
		},
		{
			name: "nil config",
			modify: func(in *AlertInput) {
				in.Channels = []NotificationChannel{{Type: ChannelWebhook}}
			},
			wantErr: "channels[0].config.url is required for type 'webhook'",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			in := validInput()
			tt.modify(&in)
			err := in.Validate()
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}

				return
			}
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("error %q does not contain %q", err.Error(), tt.wantErr)
			}
		})
	}

	t.Run("ToAlert defaults", func(t *testing.T) {
		in := validInput()
		a := in.ToAlert()
		if !strings.HasPrefix(a.ID, "alt_") {
			t.Fatalf("expected ID prefix 'alt_', got %q", a.ID)
		}
		if len(a.ID) != 4+16 { // "alt_" + 16 hex chars
			t.Fatalf("expected 20-char ID, got %d: %q", len(a.ID), a.ID)
		}
		if !a.Enabled {
			t.Fatal("expected enabled=true by default")
		}
		if a.Status != StatusOK {
			t.Fatalf("expected status ok, got %v", a.Status)
		}
	})

	t.Run("ToAlert explicit disabled", func(t *testing.T) {
		in := validInput()
		in.Enabled = boolPtr(false)
		a := in.ToAlert()
		if a.Enabled {
			t.Fatal("expected enabled=false")
		}
	})
}

func TestNotificationChannelIsEnabled(t *testing.T) {
	boolPtr := func(v bool) *bool { return &v }

	tests := []struct {
		name    string
		enabled *bool
		want    bool
	}{
		{"nil defaults to true", nil, true},
		{"explicit true", boolPtr(true), true},
		{"explicit false", boolPtr(false), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ch := NotificationChannel{Enabled: tt.enabled}
			if got := ch.IsEnabled(); got != tt.want {
				t.Fatalf("IsEnabled() = %v, want %v", got, tt.want)
			}
		})
	}
}
