package apicontracts

import (
	"fmt"
	"strings"
)

const (
	QueryResponseFormatJSON                = "json"
	QueryFormatSuggestion                  = "Use the default JSON response, or POST /api/v1/query/stream for NDJSON exports."
	QueryStreamUnsupportedFieldsSuggestion = "Use only q/query, from/to, earliest/latest, and variables with /query/stream."
)

var QueryStreamUnsupportedFields = []string{
	"limit",
	"offset",
	"wait",
	"profile",
	"format",
}

func UnsupportedQueryFormatMessage(format string) string {
	return fmt.Sprintf("unsupported format %q; only %q is supported", format, QueryResponseFormatJSON)
}

func UnsupportedQueryStreamFieldsMessage(fields []string) string {
	return fmt.Sprintf("unsupported fields for /query/stream: %s", strings.Join(fields, ", "))
}
