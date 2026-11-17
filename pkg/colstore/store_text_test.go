package colstore

import (
	"context"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/colstore/part"
	"github.com/lynxbase/lynxdb/pkg/colstore/shred"
	"github.com/lynxbase/lynxdb/pkg/vec"
)

// textRows returns the number of rows and the text of every row that holds
// one, followed by its time.
func textRows(t testing.TB, s *Store) (rows int, texts []string) {
	t.Helper()
	snap := s.Table().Snapshot()
	defer snap.Release()
	var mu sync.Mutex
	_, err := snap.Scan(context.Background(), ScanOptions{Workers: 2}, func(_ int, p *part.Part, b *part.Block) error {
		var raw, times vec.Column
		timeID, _ := p.PathID(part.TimePath)
		ref, _ := b.Column(timeID, vec.KindInt)
		if err := b.Decode(ref, &times); err != nil {
			return err
		}
		mu.Lock()
		defer mu.Unlock()
		rows += b.Rows
		id, ok := p.PathID(shred.RawPath)
		if !ok {
			return nil
		}
		ref, ok = b.Column(id, vec.KindString)
		if !ok {
			return nil
		}
		if err := b.Decode(ref, &raw); err != nil {
			return err
		}
		for i := 0; i < b.Rows; i++ {
			if raw.IsValid(i) {
				texts = append(texts, string(raw.Bytes(i))+" @"+strconv.FormatInt(times.Int(i), 10))
			}
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(texts)

	return rows, texts
}

const mixedLines = `{"ts":1700000000000000,"msg":"first"}
203.0.113.7 - - [02/Mar/2026:06:00:01 +0000] "GET /healthz HTTP/1.1" 200 2 "-" "kube-probe/1.31"
{"ts":1700000001000000,"msg":"second"

Mar  2 06:00:01 node-1 sshd[411]: Failed password for invalid user admin` + "\r\n" + `[1,2,3]
`

func TestStoreKeepsLinesThatAreNotObjectsAsText(t *testing.T) {
	s, err := OpenStore(t.TempDir(), StoreOptions{TimeField: "ts", KeepText: true, FlushInterval: -1})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	rejected, err := s.AppendAt([]byte(mixedLines), 42)
	if err != nil || rejected != 0 {
		t.Fatalf("rejected %d lines, error %v", rejected, err)
	}
	if err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	rows, texts := textRows(t, s)
	want := []string{
		`203.0.113.7 - - [02/Mar/2026:06:00:01 +0000] "GET /healthz HTTP/1.1" 200 2 "-" "kube-probe/1.31" @42`,
		`Mar  2 06:00:01 node-1 sshd[411]: Failed password for invalid user admin @42`,
		`[1,2,3] @42`,
		`{"ts":1700000001000000,"msg":"second" @42`,
	}
	if rows != 5 || strings.Join(texts, "\n") != strings.Join(want, "\n") {
		t.Fatalf("%d rows with the texts\n%s\nwant 5 with\n%s", rows, strings.Join(texts, "\n"), strings.Join(want, "\n"))
	}
}

func TestStoreRejectsLinesThatAreNotObjectsByDefault(t *testing.T) {
	s, err := OpenStore(t.TempDir(), StoreOptions{TimeField: "ts", FlushInterval: -1})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	rejected, err := s.Append([]byte(mixedLines))
	if err != nil || rejected != 4 {
		t.Fatalf("rejected %d lines, error %v; want 4", rejected, err)
	}
	if err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	if rows, texts := textRows(t, s); rows != 1 || len(texts) != 0 {
		t.Fatalf("%d rows, %d of them text; want 1 and 0", rows, len(texts))
	}
}
