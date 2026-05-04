package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDocsShipperConfigsContainRenderedTemplates(t *testing.T) {
	tests := []struct {
		tool   string
		remote string
		doc    string
	}{
		{"filebeat", "http://lynxdb:3100", "filebeat.md"},
		{"fluent-bit", "http://lynxdb:3100", "fluent-bit.md"},
		{"vector", "http://lynxdb:3100", "vector.md"},
		{"otelcol", "http://lynxdb:4318", "opentelemetry.md"},
		{"splunk-hec", "http://lynxdb:3100", "splunk-hec.md"},
	}

	for _, tt := range tests {
		t.Run(tt.tool, func(t *testing.T) {
			tmplPath := shipperConfigTemplatePaths[tt.tool]
			tmpl, err := shipperConfigFS.ReadFile(tmplPath)
			if err != nil {
				t.Fatalf("read template: %v", err)
			}
			rendered, err := renderShipperConfig(string(tmpl), shipperTemplateData{Remote: tt.remote})
			if err != nil {
				t.Fatalf("render template: %v", err)
			}

			docPath := filepath.Join("..", "..", "docs", "site", "docs", "guides", "ingest-data", tt.doc)
			body, err := os.ReadFile(docPath)
			if err != nil {
				t.Fatalf("read doc: %v", err)
			}
			if !strings.Contains(normalizeDocConfig(string(body)), normalizeDocConfig(rendered)) {
				t.Fatalf("%s does not contain rendered %s config:\n%s", docPath, tt.tool, rendered)
			}
		})
	}
}

func normalizeDocConfig(s string) string {
	s = strings.ReplaceAll(s, "\r\n", "\n")
	return strings.TrimSpace(s)
}
