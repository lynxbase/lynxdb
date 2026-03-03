package usecases

import (
	"errors"

	"github.com/OrlovEvgeny/Lynxdb/pkg/server"
)

// ErrTooManyQueries is returned when the concurrency limit is exceeded.
// Points to server.ErrTooManyQueries so errors.Is works across layers.
var ErrTooManyQueries = server.ErrTooManyQueries

// Sentinel errors for validation failures. Using typed errors instead of
// string-prefix matching makes errors.Is reliable across the codebase (U3).
var (
	ErrInvalidFrom  = errors.New("invalid from")
	ErrInvalidTo    = errors.New("invalid to")
	ErrFromBeforeTo = errors.New("from must be before to")
)
