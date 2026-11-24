package part

import (
	"testing"

	"github.com/lynxbase/lynxdb/internal/benchdata"
	"github.com/lynxbase/lynxdb/pkg/vec"
)

func anyPath(string) bool { return true }

func TestUsageAddsUpToThePartSize(t *testing.T) {
	data := benchdata.New(1, 5000).Append(nil, 40000)
	key := []string{"kind", "commit.operation", "commit.collection", "did"}
	p, err := Open(encodeNDJSON(t, data, 4096, Options{Ordered: true, SortKey: key}, "time_us"))
	if err != nil {
		t.Fatal(err)
	}
	var u Usage
	if err := p.AddUsage(&u, anyPath); err != nil {
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
		if c.Dictionary != nil {
			d := c.Dictionary
			if d.SlotBytes+d.ExceptionBytes+d.LiveBytes+d.ChecksumBytes != d.Bytes {
				t.Errorf("%s: the sections of the dictionaries do not add up to their %d bytes", key.Path, d.Bytes)
			}
			total += d.Bytes
		}
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

func TestUsageCountsPartDictionaries(t *testing.T) {
	docs := accountDocs(20000, 1500, 97, 4)
	p := plannedPart(t, docs, 2048, Options{})
	var u Usage
	if err := p.AddUsage(&u, anyPath); err != nil {
		t.Fatal(err)
	}
	c := u.Columns[ColumnKey{Path: "did", Kind: vec.KindString}]
	if c == nil || c.Dictionary == nil {
		t.Fatal("the column has no dictionary in the report")
	}
	d := c.Dictionary
	if d.Parts != 1 || d.Entries == 0 || d.Entries != d.Codes || d.Exceptions == 0 || d.Values != c.Values || d.FlatBytes == 0 {
		t.Fatalf("dictionary of %d parts, %d entries, %d codes, %d exceptions, %d of %d values, %d bytes in single blocks",
			d.Parts, d.Entries, d.Codes, d.Exceptions, d.Values, c.Values, d.FlatBytes)
	}
	if c.Encodings["shared"] == nil || c.Encodings["shared"].Blocks != p.NumBlocks() {
		t.Fatalf("encodings %v, want every block shared", c.Encodings)
	}
	total := int64(0)
	for _, n := range u.Regions {
		total += n
	}
	for key, c := range u.Columns {
		total += c.ChunkBytes + c.DirectoryBytes + c.FilterBytes
		sections := int64(0)
		for name, a := range c.Sections {
			if name == "unparsed" {
				t.Errorf("%s: %d bytes unparsed", key.Path, a.Bytes)
			}
			sections += a.Bytes
		}
		if sections != c.ChunkBytes {
			t.Errorf("%s: sections hold %d bytes of %d", key.Path, sections, c.ChunkBytes)
		}
		if c.Dictionary != nil {
			total += c.Dictionary.Bytes
		}
	}
	if total != u.Bytes || u.Bytes != int64(p.Size()) {
		t.Fatalf("usage accounts for %d bytes, usage size %d, part size %d", total, u.Bytes, p.Size())
	}
	if d.SlotBytes+d.ExceptionBytes+d.LiveBytes+d.ChecksumBytes != d.Bytes {
		t.Fatalf("the sections of the dictionary do not add up to its %d bytes", d.Bytes)
	}
	// The dictionary and its codes take less than the writer took the
	// column to take in single blocks.
	if uint64(c.ChunkBytes+d.Bytes) >= d.FlatBytes {
		t.Fatalf("codes of %d bytes and a dictionary of %d against %d in single blocks", c.ChunkBytes, d.Bytes, d.FlatBytes)
	}
	var e ColumnExplain
	e.Path = "did"
	if err := p.ExplainColumn(&e, func() bool { return true }); err != nil {
		t.Fatal(err)
	}
	if e.Dictionary == nil || e.Dictionary.Entries != d.Entries || e.Stored["shared"] == nil || len(e.Lost) != 0 {
		t.Fatalf("explain: dictionary %v, stored %v, lost %v", e.Dictionary, e.Stored, e.Lost)
	}
}
