package part

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
)

// testdata/format16.part was written by the writer of format 16, which knew
// no dictionaries, from accountDocs(400, 40, 7, 5) in blocks of 128 rows.
func TestOpenReadsAPartOfThePlainVersion(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("testdata", "format16.part"))
	if err != nil {
		t.Fatal(err)
	}
	if v := binary.LittleEndian.Uint32(data[4:]); v != plainVersion {
		t.Fatalf("the part is of version %d, want %d", v, plainVersion)
	}
	p, err := Open(data)
	if err != nil {
		t.Fatal(err)
	}
	docs := accountDocs(400, 40, 7, 5)
	checkDocs(t, p, docs)
	if p.Shares() {
		t.Fatal("a part of the plain version has a dictionary")
	}
	// A writer writes that version unless it may give the part dictionaries.
	for _, tc := range []struct {
		opts Options
		want uint32
	}{{Options{}, plainVersion}, {Options{Dictionary: true}, formatVersion}} {
		written := buildPart(t, docs, 128, tc.opts).data
		head, trailer := binary.LittleEndian.Uint32(written[4:]), binary.LittleEndian.Uint32(written[len(written)-8:])
		if head != tc.want || trailer != tc.want {
			t.Fatalf("a writer with %+v writes version %d and ends on %d, want %d", tc.opts, head, trailer, tc.want)
		}
	}
}

func TestOpenRefusesOtherVersions(t *testing.T) {
	data := buildPart(t, accountDocs(50, 5, 0, 0), 128, Options{}).data
	for _, version := range []uint32{plainVersion - 1, formatVersion + 1} {
		other := append([]byte(nil), data...)
		binary.LittleEndian.PutUint32(other[4:], version)
		binary.LittleEndian.PutUint32(other[len(other)-8:], version)
		if _, err := Open(other); err != ErrCorrupt {
			t.Fatalf("a part of version %d: %v", version, err)
		}
	}
	// The trailer names the version of the header.
	mixed := append([]byte(nil), data...)
	binary.LittleEndian.PutUint32(mixed[4:], formatVersion)
	if _, err := Open(mixed); err != ErrCorrupt {
		t.Fatalf("a part whose header and trailer name two versions: %v", err)
	}
}
