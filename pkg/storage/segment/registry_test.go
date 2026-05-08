package segment

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCapabilityRegistryStatic(t *testing.T) {
	if CapBit_ColumnZSTD != 1 {
		t.Fatalf("CapBit_ColumnZSTD = %#x, want 0x1", CapBit_ColumnZSTD)
	}
	if LSG_REQUIRED_CAPS_KNOWN != CapBit_ColumnZSTD {
		t.Fatalf("LSG_REQUIRED_CAPS_KNOWN = %#x, want %#x", LSG_REQUIRED_CAPS_KNOWN, CapBit_ColumnZSTD)
	}
	if CapBit_RangeBSI != 1<<1 {
		t.Fatalf("CapBit_RangeBSI = %#x, want 0x2", CapBit_RangeBSI)
	}
	if LSG_OPTIONAL_CAPS_KNOWN != CapBit_RangeBSI {
		t.Fatalf("LSG_OPTIONAL_CAPS_KNOWN = %#x, want %#x", LSG_OPTIONAL_CAPS_KNOWN, CapBit_RangeBSI)
	}

	data, err := os.ReadFile(filepath.Join("..", "..", "..", "docs", "storage-format.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	text := string(data)
	if strings.Count(text, "name: ColumnZSTD") != 1 {
		t.Fatalf("docs capability registry should contain ColumnZSTD exactly once")
	}
	if strings.Count(text, "bit: 0") != 1 {
		t.Fatalf("docs capability registry should assign bit 0 exactly once")
	}
}

func TestMagicRegistryStatic(t *testing.T) {
	magics := []string{
		LSG_MAGIC_V1,
		"LSG2",
		LSG_FOOTER_MAGIC,
		LSG_INVERTED_MAGIC,
		LSG_PRIMARY_MAGIC,
		LSG_BLOOM_MAGIC,
	}
	seen := make(map[string]struct{}, len(magics))
	for _, magic := range magics {
		if len(magic) != 4 {
			t.Fatalf("magic %q has length %d, want 4", magic, len(magic))
		}
		if !strings.HasPrefix(magic, "LS") {
			t.Fatalf("magic %q does not use the LS namespace", magic)
		}
		if _, ok := seen[magic]; ok {
			t.Fatalf("duplicate magic %q", magic)
		}
		seen[magic] = struct{}{}
	}

	data, err := os.ReadFile(filepath.Join("..", "..", "..", "docs", "storage-format.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	text := string(data)
	for _, magic := range []string{LSG_MAGIC_V1, LSG_FOOTER_MAGIC, LSG_INVERTED_MAGIC, LSG_PRIMARY_MAGIC, LSG_BLOOM_MAGIC} {
		if strings.Count(text, "magic: "+magic) != 1 {
			t.Fatalf("docs magic registry should contain %s exactly once", magic)
		}
	}
}

func TestMagicRegistryFileDatabaseCollision(t *testing.T) {
	dbRoot := "/usr/share/file/magic"
	if _, err := os.Stat(dbRoot); err != nil {
		if os.IsNotExist(err) {
			t.Skip("file(1) magic database not available")
		}
		t.Fatal(err)
	}

	magics := []string{
		LSG_MAGIC_V1,
		"LSG2",
		LSG_FOOTER_MAGIC,
		LSG_INVERTED_MAGIC,
		LSG_PRIMARY_MAGIC,
		LSG_BLOOM_MAGIC,
	}
	err := filepath.WalkDir(dbRoot, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		text := string(data)
		for _, magic := range magics {
			if strings.Contains(text, magic) {
				t.Fatalf("magic %s collides with file(1) database entry %s", magic, path)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
