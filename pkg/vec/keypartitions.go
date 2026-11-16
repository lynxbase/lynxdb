package vec

import (
	"encoding/binary"

	"github.com/lynxbase/lynxdb/pkg/mem"
)

const (
	minKeyChunk = 1 << 10
	maxKeyChunk = 1 << 16
	// A record is its hash, the length of its payload and the payload.
	keyRecordHead = 12
)

// KeyPartitions is an append-only log of hashed byte records split by the top
// bits of the hash, so a later pass can take one partition at a time and keep
// its working set in cache.
type KeyPartitions struct {
	arena *mem.Arena
	shift uint
	parts []keyPartition
}

type keyPartition struct {
	cur  []byte
	full [][]byte
	n    int
}

// NewKeyPartitions returns a log with 1<<bits partitions that allocates
// from a.
func NewKeyPartitions(a *mem.Arena, bits int) *KeyPartitions {
	return &KeyPartitions{arena: a, shift: uint(64 - bits), parts: make([]keyPartition, 1<<bits)}
}

// Partitions returns the number of partitions.
func (p *KeyPartitions) Partitions() int { return len(p.parts) }

// Len returns the number of records in partition i.
func (p *KeyPartitions) Len(i int) int { return p.parts[i].n }

// Add appends rec to the partition that the top bits of h select.
func (p *KeyPartitions) Add(h uint64, rec []byte) {
	kp := &p.parts[h>>p.shift]
	need := keyRecordHead + len(rec)
	if cap(kp.cur)-len(kp.cur) < need {
		p.grow(kp, need)
	}
	n := len(kp.cur)
	kp.cur = kp.cur[:n+need]
	binary.LittleEndian.PutUint64(kp.cur[n:], h)
	binary.LittleEndian.PutUint32(kp.cur[n+8:], uint32(len(rec)))
	copy(kp.cur[n+keyRecordHead:], rec)
	kp.n++
}

func (p *KeyPartitions) grow(kp *keyPartition, need int) {
	size := min(max(2*cap(kp.cur), minKeyChunk), maxKeyChunk)
	if len(kp.cur) != 0 {
		kp.full = append(kp.full, kp.cur)
	}
	kp.cur = mem.Raw[byte](p.arena, 0, max(size, need))
}

// Each calls fn with the hash and payload of every record of partition i in
// insertion order; the payload is valid until the arena is reset.
func (p *KeyPartitions) Each(i int, fn func(h uint64, rec []byte)) {
	kp := &p.parts[i]
	for _, chunk := range kp.full {
		eachKeyRecord(chunk, fn)
	}
	eachKeyRecord(kp.cur, fn)
}

func eachKeyRecord(chunk []byte, fn func(h uint64, rec []byte)) {
	for len(chunk) >= keyRecordHead {
		end := keyRecordHead + int(binary.LittleEndian.Uint32(chunk[8:]))
		fn(binary.LittleEndian.Uint64(chunk), chunk[keyRecordHead:end:end])
		chunk = chunk[end:]
	}
}
