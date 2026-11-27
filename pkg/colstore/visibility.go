package colstore

import (
	"bytes"
	"context"
	"slices"
	"sync"

	"github.com/lynxbase/lynxdb/pkg/colstore/part"
	"github.com/lynxbase/lynxdb/pkg/colstore/shred"
)

// EventRange describes the rows still in memory and their time bounds in Unix nanoseconds.
type EventRange struct {
	Rows        uint64
	First, Last int64
}

func (r *EventRange) add(o EventRange) {
	if o.Rows == 0 {
		return
	}
	if r.Rows == 0 || o.First < r.First {
		r.First = o.First
	}
	if r.Rows == 0 || o.Last > r.Last {
		r.Last = o.Last
	}
	r.Rows += o.Rows
}

func (r *EventRange) addTimes(times []int64) {
	if len(times) == 0 {
		return
	}
	first, last := times[0], times[0]
	for _, t := range times[1:] {
		first, last = min(first, t), max(last, t)
	}
	r.add(EventRange{uint64(len(times)), first, last})
}

// StatsSnapshot pins the disk parts and counts the rows that have yet to reach them.
func (s *Store) StatsSnapshot() (*Snapshot, EventRange) {
	s.takeMu.Lock()
	defer s.takeMu.Unlock()
	var events EventRange
	for _, rec := range s.reading {
		events.add(rec.events)
	}
	s.lockShards()
	for i := range s.shards {
		sh := &s.shards[i]
		events.add(sh.events)
		sh.mu.Unlock()
	}
	return s.Table().Snapshot(), events
}

// lockShards fixes one append boundary across shards. The caller holds takeMu
// and releases each shard once its rows have been captured.
func (s *Store) lockShards() {
	for i := range s.shards {
		s.shards[i].mu.Lock()
	}
}

// readBlock owns a copy of the rows. Concurrent readers share its encoding,
// which can finish after the original batch has been sorted or recycled.
type readBlock func() (*Handle, error)

type batchView struct {
	rows   int
	blocks []readBlock
}

func (s *Store) copyBlocks(dst []readBlock, b *shred.Batch, from int) []readBlock {
	for from < b.Rows {
		end := from + min(s.opts.BlockRows, b.Rows-from)
		block := shred.Get(shred.Options{})
		b.CopyRows(block, from, end)
		dst = append(dst, sync.OnceValues(func() (*Handle, error) {
			defer shred.Put(block)
			return memoryBlock(block, s.opts.SortKey)
		}))
		from = end
	}
	return dst
}

func memoryBlock(b *shred.Batch, key []string) (*Handle, error) {
	var buf bytes.Buffer
	b.SortBy(key)
	opts := part.Options{Ordered: true, SortKey: key, NoTokenFilter: true, NoPacking: true, Codec: part.CodecNone, Level: part.LevelFastest}
	pw, _ := runWriters.Get().(*part.Writer)
	if pw == nil {
		pw = part.NewWriter(&buf, opts)
	} else {
		pw.Reset(&buf, opts)
	}
	defer runWriters.Put(pw)
	if err := pw.WriteBatch(b); err != nil {
		return nil, err
	}
	if err := pw.Close(); err != nil {
		return nil, err
	}
	p, err := part.Open(buf.Bytes())
	if err != nil {
		return nil, err
	}
	return NewHandle(p, nil), nil
}

// publishFlush exchanges the buffers for their disk parts under the same
// lock as pinBuffers. File and manifest I/O precede the exchange.
func (s *Store) publishFlush(rec *flushRecord, files []*PartFile) error {
	_, err := s.dir.commit(files, nil, func(add, remove []*Handle) error {
		s.takeMu.Lock()
		defer s.takeMu.Unlock()
		if err := s.Table().Apply(add, remove); err != nil {
			return err
		}
		s.forgetFlush(rec)
		return nil
	})
	return err
}

func (s *Store) forgetFlush(rec *flushRecord) {
	i := slices.Index(s.reading, rec)
	s.reading = slices.Delete(s.reading, i, i+1)
	rec.batches, rec.view = nil, nil
}

// ReadSnapshot pins the acknowledged rows of the store across flushes and merges.
func (s *Store) ReadSnapshot(ctx context.Context) (*Snapshot, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s.written.Load() == s.appended.Load() {
		return s.Table().Snapshot(), nil
	}
	disk, blocks, err := s.pinBuffers(ctx)
	if err != nil {
		return nil, err
	}
	if len(blocks) == 0 {
		return disk, nil
	}
	defer disk.Release()
	parts := make([]*Handle, 0, len(disk.Parts())+len(blocks))
	parts = append(parts, disk.Parts()...)
	for _, block := range blocks {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		h, err := block()
		if err != nil {
			return nil, err
		}
		parts = append(parts, h)
	}
	return keep(parts), nil
}

func (s *Store) pinBuffers(ctx context.Context) (*Snapshot, []readBlock, error) {
	s.takeMu.Lock()
	defer s.takeMu.Unlock()
	var blocks []readBlock
	for _, rec := range s.reading {
		if rec.view == nil {
			select {
			case <-ctx.Done():
				return nil, nil, ctx.Err()
			case <-rec.sorted:
			}
			for _, b := range rec.batches {
				rec.view = s.copyBlocks(rec.view, b, 0)
			}
		}
		blocks = append(blocks, rec.view...)
	}
	s.lockShards()
	for i := range s.shards {
		sh := &s.shards[i]
		if sh.batch != nil {
			sh.view.blocks = s.copyBlocks(sh.view.blocks, sh.batch, sh.view.rows)
			sh.view.rows = sh.batch.Rows
			blocks = append(blocks, sh.view.blocks...)
		}
		sh.mu.Unlock()
	}
	return s.Table().Snapshot(), blocks, nil
}
