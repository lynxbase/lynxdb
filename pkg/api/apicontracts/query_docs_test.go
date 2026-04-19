package apicontracts

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestQueryContractsStayInDocsAndOpenAPI(t *testing.T) {
	t.Parallel()

	fieldList := quotedFieldList(QueryStreamUnsupportedFields)
	checks := []struct {
		path     string
		snippets []string
	}{
		{
			path: "docs/site/docs/api/query.md",
			snippets: []string{
				"Only empty or `json` are accepted on this endpoint.",
				fieldList + " are not silently ignored on this path.",
			},
		},
		{
			path: "docs/site/docs/operations/troubleshooting.md",
			snippets: []string{
				"If you send",
				"`limit`",
				"`offset`",
				"`wait`",
				"`profile`",
				"`format`",
			},
		},
		{
			path: "docs/site/static/api/swagger.yaml",
			snippets: []string{
				"enum: [json]",
				"- " + fieldList + " are rejected with `400`.",
			},
		},
	}

	for _, check := range checks {
		check := check
		t.Run(check.path, func(t *testing.T) {
			content := readRepoFile(t, check.path)
			for _, snippet := range check.snippets {
				if !strings.Contains(content, snippet) {
					t.Fatalf("%s is missing contract snippet %q", check.path, snippet)
				}
			}
		})
	}
}

func readRepoFile(t *testing.T, rel string) string {
	t.Helper()

	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}

	root := filepath.Join(filepath.Dir(filename), "..", "..", "..")
	data, err := os.ReadFile(filepath.Join(root, rel))
	if err != nil {
		t.Fatalf("read %s: %v", rel, err)
	}

	return string(data)
}

func quotedFieldList(fields []string) string {
	switch len(fields) {
	case 0:
		return ""
	case 1:
		return "`" + fields[0] + "`"
	case 2:
		return "`" + fields[0] + "` and `" + fields[1] + "`"
	default:
		quoted := make([]string, len(fields))
		for i, field := range fields {
			quoted[i] = "`" + field + "`"
		}

		return strings.Join(quoted[:len(quoted)-1], ", ") + ", and " + quoted[len(quoted)-1]
	}
}
