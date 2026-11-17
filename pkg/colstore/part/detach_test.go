package part

import (
	"bytes"
	"encoding/binary"
	"reflect"
	"sync"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/colstore/shred"
	"github.com/lynxbase/lynxdb/pkg/colstore/tokenfilter"
	"github.com/lynxbase/lynxdb/pkg/vec"
)

func TestDetachedPartFindsItsBlocksWithoutItsFooterAndDirectories(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf, Options{Ordered: true, SortKey: []string{"svc"}})
	batch := shred.NewBatch(shred.Options{})
	for i, d := range sampleDocs(600) {
		if err := batch.AddJSON([]byte(d), int64(i)); err != nil {
			t.Fatal(err)
		}
		if i%100 == 99 {
			batch.SortBy([]string{"svc"})
			if err := w.WriteBatch(batch); err != nil {
				t.Fatal(err)
			}
			batch.Reset()
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	whole, err := Open(bytes.Clone(buf.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	data := bytes.Clone(buf.Bytes())
	p, err := Open(data)
	if err != nil {
		t.Fatal(err)
	}
	p.Detach()
	// The directory of a block is kept from the first time it is opened,
	// on whichever goroutine that is.
	var opening sync.WaitGroup
	for g := 0; g < 4; g++ {
		opening.Add(1)
		go func() {
			defer opening.Done()
			var b Block
			for i := 0; i < p.NumBlocks()-1; i++ {
				if err := p.OpenBlock(i, &b); err != nil {
					t.Error(err)
				}
			}
		}()
	}
	opening.Wait()
	footer := int(binary.LittleEndian.Uint64(data[len(data)-trailerSize:]))
	clear(data[footer:])
	clear(data[p.dirsOff : p.dirsOff+len(p.dirs)])
	// The last block was not opened: its directory is still read from the
	// data, where it is damaged by now.
	var b Block
	if err := p.OpenBlock(p.NumBlocks()-1, &b); err != ErrCorrupt {
		t.Fatalf("a block whose directory was cleared before it was opened: %v", err)
	}

	if p.NumBlocks() != whole.NumBlocks() || p.NumBlocks() < 4 {
		t.Fatalf("%d blocks, the whole part has %d", p.NumBlocks(), whole.NumBlocks())
	}
	if !reflect.DeepEqual(p.firstKeys, whole.firstKeys) || !bytes.Equal(p.lastKey, whole.lastKey) {
		t.Fatal("the keys of the detached part differ")
	}
	token := tokenfilter.AppendHashes(nil, []byte("alpha"))[0]
	var wb Block
	for i := 0; i < p.NumBlocks()-1; i++ {
		if err := p.OpenBlock(i, &b); err != nil {
			t.Fatalf("block %d: %v", i, err)
		}
		if err := whole.OpenBlock(i, &wb); err != nil {
			t.Fatal(err)
		}
		got, err := p.MayContainToken(i, token)
		want, werr := whole.MayContainToken(i, token)
		if err != nil || werr != nil || got != want {
			t.Fatalf("block %d: token answer %v (%v), the whole part says %v (%v)", i, got, err, want, werr)
		}
		refs, wrefs := b.Columns(), wb.Columns()
		if len(refs) != len(wrefs) || len(refs) == 0 {
			t.Fatalf("block %d: %d columns, the whole part has %d", i, len(refs), len(wrefs))
		}
		for j := range refs {
			var c, wc vec.Column
			if err := b.Decode(&refs[j], &c); err != nil {
				t.Fatalf("block %d column %d: %v", i, j, err)
			}
			if err := wb.Decode(&wrefs[j], &wc); err != nil {
				t.Fatal(err)
			}
			if c.Rows != wc.Rows || !bytes.Equal(c.Data, wc.Data) || !reflect.DeepEqual(c.Ints, wc.Ints) {
				t.Fatalf("block %d column %d decodes differently", i, j)
			}
		}
	}
}
