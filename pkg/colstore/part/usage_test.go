package part

import (
	"testing"

	"github.com/lynxbase/lynxdb/internal/benchdata"
)

func TestUsageAddsUpToThePartSize(t *testing.T) {
	data := benchdata.New(1, 5000).Append(nil, 40000)
	key := []string{"kind", "commit.operation", "commit.collection", "did"}
	p, err := Open(encodeNDJSON(t, data, 4096, Options{Ordered: true, SortKey: key}, "time_us"))
	if err != nil {
		t.Fatal(err)
	}
	var u Usage
	if err := p.AddUsage(&u, true); err != nil {
		t.Fatal(err)
	}
	total := int64(0)
	for name, n := range u.Regions {
		if n < 0 || (n != 0 && (name == "blocks.unaccounted" || name == "directories.unaccounted" || name == "footer.unparsed")) {
			t.Errorf("region %s holds %d bytes", name, n)
		}
		total += n
	}
	for key, c := range u.Columns {
		total += c.ChunkBytes + c.DirectoryBytes + c.FilterBytes
		sections := int64(0)
		for name, a := range c.Sections {
			if name == "unparsed" {
				t.Errorf("%s %s: %d bytes unparsed", key.Path, key.Kind, a.Bytes)
			}
			sections += a.Bytes
		}
		if sections != c.ChunkBytes {
			t.Errorf("%s %s: sections hold %d bytes of %d", key.Path, key.Kind, sections, c.ChunkBytes)
		}
	}
	if total != u.Bytes || u.Bytes != int64(p.Size()) {
		t.Fatalf("usage accounts for %d bytes, usage size %d, part size %d", total, u.Bytes, p.Size())
	}
}
