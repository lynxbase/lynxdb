package shippers

import (
	"regexp"
	"strings"
)

type Fingerprint struct {
	Tool    string `json:"tool"`
	Version string `json:"version,omitempty"`
}

var shipperPatterns = []struct {
	tool string
	re   *regexp.Regexp
}{
	{"filebeat", regexp.MustCompile(`(?i)\bfilebeat[/ -]?([0-9]+(?:\.[0-9]+){1,2})?`)},
	{"fluent-bit", regexp.MustCompile(`(?i)\bfluent[- ]?bit[/ -]?v?([0-9]+(?:\.[0-9]+){1,2})?`)},
	{"vector", regexp.MustCompile(`(?i)\bvector[/ -]?([0-9]+(?:\.[0-9]+){1,2})?`)},
	{"otelcol", regexp.MustCompile(`(?i)\b(?:otelcol|otel-collector|opentelemetry-collector(?:-contrib)?|otlp|otel-otlp-exporter)[/\- ]?v?([0-9]+(?:\.[0-9]+){1,2})?`)},
}

func DetectUserAgent(userAgent string) Fingerprint {
	ua := strings.TrimSpace(userAgent)
	for _, p := range shipperPatterns {
		m := p.re.FindStringSubmatch(ua)
		if m == nil {
			continue
		}
		version := ""
		if len(m) > 1 {
			version = m[1]
		}
		return Fingerprint{Tool: p.tool, Version: version}
	}
	return Fingerprint{Tool: "unknown"}
}
