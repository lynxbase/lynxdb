package shippers

import "testing"

func TestDetector_UserAgents(t *testing.T) {
	tests := []struct {
		name     string
		ua       string
		wantTool string
		wantVer  string
	}{
		{"filebeat", "Filebeat/8.15.0 (linux; amd64)", "filebeat", "8.15.0"},
		{"fluent-bit", "Fluent-Bit v3.1.4", "fluent-bit", "3.1.4"},
		{"vector", "Vector/0.40.0", "vector", "0.40.0"},
		{"otelcol", "opentelemetry-collector-contrib/0.105.0", "otelcol", "0.105.0"},
		{"unknown", "curl/8.0", "unknown", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := DetectUserAgent(tt.ua)
			if got.Tool != tt.wantTool || got.Version != tt.wantVer {
				t.Fatalf("DetectUserAgent() = %#v, want tool=%q version=%q", got, tt.wantTool, tt.wantVer)
			}
		})
	}
}
