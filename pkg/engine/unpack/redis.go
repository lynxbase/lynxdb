package unpack

import (
	"strings"

	"github.com/lynxbase/lynxdb/pkg/event"
)

// RedisParser extracts fields from Redis server log lines.
// Format: pid:role_char DD Mon YYYY HH:MM:SS.mmm level_char message
//
// Role characters: M=master, S=replica, C=rdb_child, X=sentinel
// Level characters: .=debug, -=verbose, *=notice, #=warning
//
// Example:
//
//	12345:M 14 Feb 2026 14:52:01.234 * Ready to accept connections
type RedisParser struct{}

// Name returns the parser format name.
func (p *RedisParser) Name() string { return "redis" }

// DeclareFields declares the fields produced by the Redis parser.
func (p *RedisParser) DeclareFields() FieldDeclaration {
	return FieldDeclaration{
		Known:    []string{"pid", "role_char", "timestamp", "level_char", "message"},
		Optional: []string{"role", "level"},
	}
}

// roleMap maps Redis single-character roles to human-readable names.
var roleMap = map[byte]string{
	'M': "master",
	'S': "replica",
	'C': "rdb_child",
	'X': "sentinel",
}

// levelMap maps Redis single-character log levels to human-readable names.
var levelMap = map[byte]string{
	'.': "debug",
	'-': "verbose",
	'*': "notice",
	'#': "warning",
}

// Parse extracts fields from a Redis server log line.
func (p *RedisParser) Parse(input string, emit func(key string, val event.Value) bool) error {
	s := strings.TrimSpace(input)
	if len(s) == 0 {
		return nil
	}

	i := 0

	// Parse pid:role_char — e.g., "12345:M"
	colonIdx := strings.IndexByte(s, ':')
	if colonIdx <= 0 || colonIdx+1 >= len(s) {
		return nil
	}

	pidStr := s[:colonIdx]
	roleChar := s[colonIdx+1]
	i = colonIdx + 2

	if !emit("pid", InferValue(pidStr)) {
		return nil
	}
	if !emit("role_char", event.StringValue(string(roleChar))) {
		return nil
	}
	if roleName, ok := roleMap[roleChar]; ok {
		if !emit("role", event.StringValue(roleName)) {
			return nil
		}
	}

	// Skip space after role char.
	if i >= len(s) || s[i] != ' ' {
		return nil
	}
	i++

	// Parse timestamp: "DD Mon YYYY HH:MM:SS.mmm"
	// Format: "14 Feb 2026 14:52:01.234" — skip past 3 spaces to reach the time.
	tsStart := i
	spaceCount := 0
	for i < len(s) && spaceCount < 3 {
		if s[i] == ' ' {
			spaceCount++
		}
		i++
	}
	if spaceCount < 3 || i >= len(s) {
		return nil
	}

	// Now i points to the start of the time part "HH:MM:SS.mmm".
	// Find the next space after the time.
	timeEnd := strings.IndexByte(s[i:], ' ')
	if timeEnd < 0 {
		return nil
	}
	timeEnd += i
	timestamp := s[tsStart:timeEnd]
	i = timeEnd

	if !emit("timestamp", event.StringValue(timestamp)) {
		return nil
	}

	// Skip space before level char.
	i = skipSpaces(s, i)
	if i >= len(s) {
		return nil
	}

	// Parse level_char — single character.
	levelChar := s[i]
	i++

	if !emit("level_char", event.StringValue(string(levelChar))) {
		return nil
	}
	if levelName, ok := levelMap[levelChar]; ok {
		if !emit("level", event.StringValue(levelName)) {
			return nil
		}
	}

	// Skip space before message.
	i = skipSpaces(s, i)
	if i >= len(s) {
		return nil
	}

	// Rest is the message.
	message := s[i:]
	if !emit("message", event.StringValue(message)) {
		return nil
	}

	return nil
}
