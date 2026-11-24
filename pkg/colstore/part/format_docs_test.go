package part

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// A reader refuses a part of any other version, so the storage page has to
// name the one this build writes.
func TestStoragePageNamesTheFormatVersion(t *testing.T) {
	const page = "docs/site/docs/architecture/storage.md"
	text, err := os.ReadFile(filepath.Join("..", "..", "..", page))
	if err != nil {
		t.Fatal(err)
	}
	want := fmt.Sprintf("`LXPT` magic and version %d.", formatVersion)
	if !strings.Contains(string(text), want) {
		t.Fatalf("%s does not say %q", page, want)
	}
}
