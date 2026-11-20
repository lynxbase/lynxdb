package part

import (
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/vec"
)

func TestStampsSpellBackAsParsed(t *testing.T) {
	for _, v := range []string{
		"2024-11-21T16:25:48.470Z", "2024-02-29T00:00:00Z", "1999-12-31T23:59:59.999999+02:00",
		"0001-01-01T00:00:00.1Z", "2261-12-31T23:59:59.123456789-11:30", "1969-07-20T20:17:40.000Z",
	} {
		frac, zone, ok := stampLayout([]byte(v))
		if !ok {
			t.Fatalf("%s is no stamp", v)
		}
		x, ok := parseStamp([]byte(v), frac, zone)
		if !ok {
			t.Fatalf("%s does not parse", v)
		}
		s := stampSpeller{frac: frac, zone: zone}
		out := make([]byte, len(v))
		if s.put(out, x); string(out) != v {
			t.Fatalf("%s spells back as %s", v, out)
		}
	}
	for _, v := range []string{
		"2023-02-29T00:00:00Z", "2024-13-01T00:00:00Z", "2024-11-21T24:00:00Z", "2024-11-21T16:25:60Z",
		"2024-11-21 16:25:48Z", "9999-12-31T23:59:59.123456789Z",
	} {
		frac, zone, ok := stampLayout([]byte(v))
		if _, parsed := parseStamp([]byte(v), frac, zone); ok && parsed {
			t.Fatalf("%s parses", v)
		}
	}
}

// stampDocs returns rows documents some seconds apart whose key holds the
// time the row was made at in milliseconds and whose stamp holds that time
// as RFC 3339, a few of them in other layouts, long before or no times.
func stampDocs(rows int, r *rand.Rand) []string {
	docs := make([]string, 0, rows)
	us := int64(1732206349000000)
	for i := 0; i < rows; i++ {
		us += r.Int63n(3000000)
		made := us - r.Int63n(60e6)
		at := time.UnixMicro(made).UTC()
		stamp := at.Format("2006-01-02T15:04:05.000Z")
		switch r.Intn(100) {
		case 0:
			stamp = at.Format("2006-01-02T15:04:05Z")
		case 1:
			stamp = at.Format("2006-01-02T15:04:05.000000+00:00")
		case 2:
			stamp = at.Add(-400 * 24 * time.Hour).Format("2006-01-02T15:04:05.000Z")
		case 3:
			stamp = "not a time"
		}
		key := timedKey("234567abcdefghijklmnopqrstuvwxyz", 5, 13, uint64(made)<<10|uint64(r.Intn(32)))
		docs = append(docs, fmt.Sprintf(`{"us":%d,"key":%q,"stamp":%q}`, us, key, stamp))
	}

	return docs
}

func TestStampsRoundTripAndFollowTheirKeys(t *testing.T) {
	r := rand.New(rand.NewSource(9))
	docs := stampDocs(9000, r)
	p, err := Open(encodeNDJSON(t, []byte(strings.Join(docs, "\n")+"\n"), 4096, Options{}, "us"))
	if err != nil {
		t.Fatal(err)
	}
	got, _ := readAll(t, p)
	for i := range docs {
		if !sameJSON(t, got[i], docs[i]) {
			t.Fatalf("row %d\n got  %s\n want %s", i, got[i], docs[i])
		}
	}
	var u Usage
	if err := p.AddUsage(&u, true); err != nil {
		t.Fatal(err)
	}
	c := u.Columns[ColumnKey{"stamp", vec.KindString}]
	if c.Encodings["timed"] == nil || c.Sections["unparsed"] != nil {
		t.Fatalf("stamps: encodings %v, sections %v", c.Encodings, c.Sections)
	}
	t.Logf("stamps: %.2f bytes a value", float64(c.ChunkBytes)/float64(c.Values))
	var b Block
	id, _ := p.PathID("stamp")
	for i := 0; i < p.NumBlocks(); i++ {
		if err := p.OpenBlock(i, &b); err != nil {
			t.Fatal(err)
		}
		ref, _ := b.Column(id, vec.KindString)
		var v timedValues
		chunk, _ := b.timedChunk(ref)
		if ref.enc != encTimed {
			continue
		}
		if _, ok := readTimedHead(chunk, ref.Count, &v, false); !ok || v.form != formStamp || v.refKind != refColumn {
			t.Fatalf("block %d: stamps of form %d follow reference %d", i, v.form, v.refKind)
		}
	}
}
