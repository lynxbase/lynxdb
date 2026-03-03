package channels

import (
	"fmt"
	"time"

	"github.com/OrlovEvgeny/Lynxdb/pkg/alerts"
)

// Registry maps channel types to their constructors.
type Registry struct {
	constructors map[alerts.ChannelType]func(config map[string]interface{}) (alerts.Notifier, error)
}

// NewRegistry creates a registry with all built-in channel constructors.
func NewRegistry() *Registry {
	r := &Registry{
		constructors: make(map[alerts.ChannelType]func(config map[string]interface{}) (alerts.Notifier, error)),
	}
	r.constructors[alerts.ChannelWebhook] = NewWebhook
	r.constructors[alerts.ChannelSlack] = NewSlack
	r.constructors[alerts.ChannelTelegram] = NewTelegram

	return r
}

// Create instantiates a Notifier for the given channel type and config.
func (r *Registry) Create(chType alerts.ChannelType, config map[string]interface{}) (alerts.Notifier, error) {
	ctor, ok := r.constructors[chType]
	if !ok {
		return nil, fmt.Errorf("no constructor for channel type %q", chType)
	}

	return ctor(config)
}

// Factory returns a NotifierFactory backed by this registry.
func (r *Registry) Factory() alerts.NotifierFactory {
	return func(chType alerts.ChannelType, config map[string]interface{}) (alerts.Notifier, error) {
		return r.Create(chType, config)
	}
}

// FormatMessage builds a human-readable notification message.
func FormatMessage(alert alerts.Alert, rowCount int) string {
	return fmt.Sprintf(
		"Alert %q triggered — query returned %d rows\nAlert: %s | Query: %s | Interval: %s | Rows: %d | Time: %s",
		alert.Name, rowCount,
		alert.Name, alert.Query, alert.Interval, rowCount,
		time.Now().UTC().Format(time.RFC3339),
	)
}
