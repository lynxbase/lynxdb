package alerts

import "errors"

var (
	ErrAlertNotFound      = errors.New("alerts: not found")
	ErrAlertAlreadyExists = errors.New("alerts: already exists")
	ErrAlertNameEmpty     = errors.New("alerts: name must not be empty")
	ErrQueryEmpty         = errors.New("alerts: query must not be empty")
	ErrInvalidInterval    = errors.New("alerts: interval must be a valid duration >= 10s")
	ErrNoChannels         = errors.New("alerts: at least one channel is required")
	ErrUnknownChannelType = errors.New("alerts: unknown channel type")
)
