package views

import "errors"

var (
	ErrViewNotFound      = errors.New("views: not found")
	ErrViewAlreadyExists = errors.New("views: already exists")
	ErrViewNameInvalid   = errors.New("views: invalid name")
	ErrViewNameEmpty     = errors.New("views: name must not be empty")
	ErrInvalidRetention  = errors.New("views: retention must be non-negative")
	ErrNoColumns         = errors.New("views: columns must not be empty")
)
