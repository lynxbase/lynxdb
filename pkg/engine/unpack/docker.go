package unpack

import (
	"strings"

	"github.com/lynxbase/lynxdb/pkg/event"
)

// DockerParser extracts fields from Docker json-file logging driver output.
// This format is the default output of `docker logs` when using the json-file
// or journald driver. It wraps a JSONParser and strips trailing newlines from
// the "log" field, which Docker appends to every log line.
//
// Example:
//
//	{"log":"hello world\n","stream":"stderr","time":"2026-01-01T00:00:00.000000000Z"}
type DockerParser struct {
	inner JSONParser
}

// Name returns the parser format name.
func (p *DockerParser) Name() string { return "docker" }

// DeclareFields declares the fields produced by the Docker json-file parser.
func (p *DockerParser) DeclareFields() FieldDeclaration {
	return FieldDeclaration{
		Known:   []string{"log", "stream", "time"},
		Dynamic: true,
	}
}

// Parse extracts fields from Docker json-file log line.
// The "log" field has trailing \n stripped. All other fields pass through
// with standard JSON type inference (booleans, numbers, nested objects).
func (p *DockerParser) Parse(input string, emit func(key string, val event.Value) bool) error {
	return p.inner.Parse(input, func(key string, val event.Value) bool {
		if key == "log" {
			// Docker appends \n to every log line; strip it for cleaner output.
			s := strings.TrimRight(val.String(), "\n")

			return emit(key, event.StringValue(s))
		}

		return emit(key, val)
	})
}
