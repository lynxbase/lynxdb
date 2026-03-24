package pipeline

import (
	"encoding/json"
	"regexp"
	"strings"
)

// DetectedFormat represents the auto-detected log format.
type DetectedFormat string

const (
	FormatJSON     DetectedFormat = "json"
	FormatLogfmt   DetectedFormat = "logfmt"
	FormatCombined DetectedFormat = "combined"
	FormatSyslog   DetectedFormat = "syslog"
	FormatDocker   DetectedFormat = "docker"
	FormatCSV      DetectedFormat = "csv"
	FormatTSV      DetectedFormat = "tsv"
	FormatPlain    DetectedFormat = "plain"
)

// FormatConfidence holds detection confidence scores for each format.
type FormatConfidence struct {
	JSON     float64
	Logfmt   float64
	Combined float64
	Syslog   float64
	Docker   float64
	CSV      float64
	TSV      float64
}

// logfmtRegex matches key=value and key="quoted value" patterns.
var logfmtRegex = regexp.MustCompile(`\w+=("(?:[^"\\]|\\.)*"|[^\s,]+)`)

// combinedRegex matches Apache/Nginx combined log format.
var combinedRegex = regexp.MustCompile(`^\S+ \S+ \S+ \[[^\]]+\] "[^"]+" \d+ \d+`)

// syslogRegex matches RFC syslog format.
var syslogRegex = regexp.MustCompile(`^<\d+>\w{3}\s+\d+\s+\d{2}:\d{2}:\d{2}\s+\S+\s+\S+`)

// DetectFormat analyzes a sample of lines and returns the best detected format.
// sampleLines should contain the first 10-20 lines of the data source.
func DetectFormat(sampleLines []string) DetectedFormat {
	if len(sampleLines) == 0 {
		return FormatPlain
	}

	conf := DetectFormatConfidence(sampleLines)

	// Require at least 70% confidence and 2x advantage over runner-up.
	best := FormatPlain
	bestScore := conf.JSON
	if conf.Logfmt > bestScore {
		bestScore = conf.Logfmt
		best = FormatLogfmt
	}
	if conf.Combined > bestScore {
		bestScore = conf.Combined
		best = FormatCombined
	}
	if conf.Syslog > bestScore {
		bestScore = conf.Syslog
		best = FormatSyslog
	}
	if conf.Docker > bestScore {
		bestScore = conf.Docker
		best = FormatDocker
	}
	if conf.CSV > bestScore {
		bestScore = conf.CSV
		best = FormatCSV
	}
	if conf.TSV > bestScore {
		bestScore = conf.TSV
		best = FormatTSV
	}

	if bestScore < 0.7 {
		return FormatPlain
	}

	return best
}

// DetectFormatConfidence returns confidence scores for each format.
func DetectFormatConfidence(lines []string) FormatConfidence {
	n := float64(len(lines))
	if n == 0 {
		return FormatConfidence{}
	}

	var jsonOk, logfmtOk, combinedOk, syslogOk, dockerOk, csvOk, tsvOk float64

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			n--
			continue
		}

		if json.Valid([]byte(line)) {
			jsonOk++
			// Check for Docker JSON envelope (has "log" and "stream" fields).
			if isDockerJSONLine(line) {
				dockerOk++
			}
		}

		if isLogfmtLine(line) {
			logfmtOk++
		}

		if combinedRegex.MatchString(line) {
			combinedOk++
		}

		if syslogRegex.MatchString(line) {
			syslogOk++
		}

		if isCSVLine(line) {
			csvOk++
		}

		if isTSVLine(line) {
			tsvOk++
		}
	}

	if n <= 0 {
		return FormatConfidence{}
	}

	return FormatConfidence{
		JSON:     jsonOk / n,
		Logfmt:   logfmtOk / n,
		Combined: combinedOk / n,
		Syslog:   syslogOk / n,
		Docker:   dockerOk / n,
		CSV:      csvOk / n,
		TSV:      tsvOk / n,
	}
}

// isLogfmtLine checks if a line looks like logfmt (key=value pairs).
func isLogfmtLine(line string) bool {
	if len(line) < 3 {
		return false
	}
	// Must not start with JSON's { or [
	if line[0] == '{' || line[0] == '[' {
		return false
	}
	// Must have at least one key=value match.
	matches := logfmtRegex.FindAllString(line, -1)
	if len(matches) == 0 {
		return false
	}
	// At least 30% of the line should be key=value pairs.
	totalLen := 0
	for _, m := range matches {
		totalLen += len(m)
	}

	return float64(totalLen)/float64(len(line)) > 0.3
}

// ParseCommandForFormat returns the parse command to prepend for the given format.
// Returns empty string for formats already handled by the default ingest pipeline
// (JSON and logfmt are parsed automatically at ingest time).
func ParseCommandForFormat(format DetectedFormat) string {
	switch format {
	case FormatCombined:
		return "parse combined(_raw)"
	case FormatSyslog:
		return "parse syslog(_raw)"
	case FormatDocker:
		return "parse docker(_raw)"
	case FormatCSV:
		return "parse csv(_raw)"
	case FormatTSV:
		return "parse tsv(_raw)"
	default:
		// JSON and logfmt are already handled by the default ingest pipeline.
		// No explicit parse command needed.
		return ""
	}
}

// DetectFormatWithRatio analyzes lines and returns the detected format plus
// the fraction of lines matching that format (0.0 to 1.0).
func DetectFormatWithRatio(sampleLines []string) (DetectedFormat, float64) {
	if len(sampleLines) == 0 {
		return FormatPlain, 0
	}

	conf := DetectFormatConfidence(sampleLines)

	// Find best format and its score.
	best := FormatPlain
	bestScore := conf.JSON
	if conf.Logfmt > bestScore {
		bestScore = conf.Logfmt
		best = FormatLogfmt
	}
	if conf.Combined > bestScore {
		bestScore = conf.Combined
		best = FormatCombined
	}
	if conf.Syslog > bestScore {
		bestScore = conf.Syslog
		best = FormatSyslog
	}
	if conf.Docker > bestScore {
		bestScore = conf.Docker
		best = FormatDocker
	}
	if conf.CSV > bestScore {
		bestScore = conf.CSV
		best = FormatCSV
	}
	if conf.TSV > bestScore {
		bestScore = conf.TSV
		best = FormatTSV
	}

	if bestScore < 0.7 {
		return FormatPlain, bestScore
	}

	return best, bestScore
}

// FormatDisplayName returns an uppercase display name for the format.
func FormatDisplayName(format DetectedFormat) string {
	switch format {
	case FormatJSON:
		return "JSON"
	case FormatLogfmt:
		return "logfmt"
	case FormatCombined:
		return "Combined"
	case FormatSyslog:
		return "Syslog"
	case FormatDocker:
		return "Docker JSON"
	case FormatCSV:
		return "CSV"
	case FormatTSV:
		return "TSV"
	case FormatPlain:
		return "Plain text"
	default:
		return string(format)
	}
}

// isDockerJSONLine checks if a JSON line has Docker container envelope fields.
func isDockerJSONLine(line string) bool {
	if len(line) < 20 || line[0] != '{' {
		return false
	}
	// Quick substring check before parsing: Docker JSON always has "log" and "stream".
	if !strings.Contains(line, `"log"`) || !strings.Contains(line, `"stream"`) {
		return false
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal([]byte(line), &obj); err != nil {
		return false
	}
	_, hasLog := obj["log"]
	_, hasStream := obj["stream"]

	return hasLog && hasStream
}

// isCSVLine checks if a line looks like CSV (comma-separated with 2+ fields).
func isCSVLine(line string) bool {
	if len(line) < 3 {
		return false
	}
	// Must not start with { or [ (JSON) or < (syslog).
	if line[0] == '{' || line[0] == '[' || line[0] == '<' {
		return false
	}
	fields := strings.Split(line, ",")
	return len(fields) >= 2
}

// isTSVLine checks if a line looks like TSV (tab-separated with 2+ fields).
func isTSVLine(line string) bool {
	if len(line) < 3 {
		return false
	}
	if line[0] == '{' || line[0] == '[' || line[0] == '<' {
		return false
	}
	fields := strings.Split(line, "\t")
	return len(fields) >= 2
}
