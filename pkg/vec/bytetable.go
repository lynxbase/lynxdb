package vec

import (
	"encoding/binary"
	"math/bits"

	"github.com/lynxbase/lynxdb/pkg/mem"
)

const (
	// tableChunkBits is the number of ids, as a power of two, whose hashes
	// and key places share a chunk; chunks never move, so a table grows
	// without copying what it holds.
	tableChunkBits = 12
	tableChunk     = 1 << tableChunkBits
	// tablePageBits is the size of a page of keys as a power of two.
	tablePageBits = 16
	tablePage     = 1 << tablePageBits
	tableMinSlots = 256
	// tableWideSlots is the number of slots from which a table grows by
	// doubling.
	tableWideSlots = 1 << 18
	// tableFingerprint is the most bits of a hash that a slot keeps beside
	// an id, and tablePartition the bits below them that PartitionOf reads;
	// the bits that pick a slot are the lowest.
	tableFingerprint = 16
	tablePartition   = 16
	// tableMaxKeys is the number of keys a ByteTable holds at most.
	tableMaxKeys = 1 << 30
	// tableMaxPages is the number of pages that the place of a key can
	// name.
	tableMaxPages = 1 << (32 - tablePageBits)
)

// tableSlots holds the slots of tables between their uses, and tableRows the
// buffers of their lookups by batch.
var (
	tableSlots mem.Sized[uint64]
	tableRows  mem.Sized[uint32]
)

// ByteTable assigns dense ids, in insertion order, to distinct byte keys, so
// per-group state can live in caller-owned slices indexed by id. Keys,
// their hashes and their places come from an arena in chunks that stay
// where they are; the slots come from a pool that Release returns them to.
// Nothing in it holds a pointer.
type ByteTable struct {
	arena *mem.Arena
	// A slot holds the place of a key in its high half, and in the low one
	// the id of the key plus one in idBits bits with the top bits of the
	// key's hash above them; zero is an empty slot. The place beside the
	// id lets a lookup go from the slot to the key.
	slots   []uint64
	idBits  uint
	fpShift uint
	n       int
	// hashes and locs hold, per id, the hash of the key and its place: the
	// page in the bits above tablePageBits and the offset in it below.
	hashes [][]uint64
	locs   [][]uint32
	// pages hold the keys, each after its length as a varint; page is the
	// one being filled, at offset at.
	pages [][]byte
	page  int
	at    int
	// cand and pending are the buffers of a lookup by batch.
	cand    []uint64
	pending []uint32
	// touched takes the reads that only warm the cache, so that they are
	// not dropped.
	touched uint64
}

// NewByteTable returns a table that allocates from a.
func NewByteTable(a *mem.Arena) *ByteTable {
	t := &ByteTable{arena: a}
	t.setSlots(tableMinSlots)

	return t
}

func (t *ByteTable) setSlots(n int) {
	tableSlots.Put(t.slots)
	t.slots = tableSlots.Get(n)
	clear(t.slots)
	t.idBits = uint(bits.TrailingZeros(uint(n)))
	t.fpShift = 64 - min(tableFingerprint, 32-t.idBits)
}

// SetArena makes a the arena that the table allocates from, for a table
// that goroutines take turns to add keys to, each from an arena of its own.
// What the table holds stays where it is, so every arena it was given must
// outlive it.
func (t *ByteTable) SetArena(a *mem.Arena) { t.arena = a }

// Clear removes every key and keeps the table's memory.
func (t *ByteTable) Clear() {
	clear(t.slots)
	t.n, t.page, t.at = 0, 0, 0
}

// Release returns the memory that does not belong to the arena; the table
// must not be used afterwards.
func (t *ByteTable) Release() {
	tableSlots.Put(t.slots)
	tableSlots.Put(t.cand)
	tableRows.Put(t.pending)
	t.slots, t.cand, t.pending = nil, nil, nil
}

// Retained returns the bytes the table holds outside its arena.
func (t *ByteTable) Retained() int {
	return 8*(cap(t.slots)+cap(t.cand)) + 4*cap(t.pending)
}

// Len returns the number of distinct keys.
func (t *ByteTable) Len() int { return t.n }

// Key returns the key with the given id.
func (t *ByteTable) Key(id int) []byte {
	return t.keyAt(t.locs[id>>tableChunkBits][id&(tableChunk-1)])
}

func (t *ByteTable) keyAt(loc uint32) []byte {
	p := t.pages[loc>>tablePageBits][loc&(tablePage-1):]
	if n := int(p[0]); n < 0x80 {
		return p[1 : 1+n]
	}

	return longKey(p)
}

// longKey returns the key at the start of p whose length takes more than a
// byte.
func longKey(p []byte) []byte {
	n, used := binary.Uvarint(p)

	return p[used : used+int(n)]
}

// KeyHash returns the hash of the key with the given id.
func (t *ByteTable) KeyHash(id int) uint64 { return t.hashes[id>>tableChunkBits][id&(tableChunk-1)] }

// ID returns the id of key, adding it when absent.
func (t *ByteTable) ID(key []byte) int { return t.IDHashed(key, Hash(key)) }

// IDHashed is ID for a caller that already has Hash(key).
func (t *ByteTable) IDHashed(key []byte, h uint64) int {
	mask := uint64(len(t.slots) - 1)
	idMask := uint32(1)<<t.idBits - 1
	want := uint32(h>>t.fpShift) << t.idBits
	i := h & mask
	for ; ; i = (i + 1) & mask {
		s := t.slots[i]
		if s == 0 {
			break
		}
		if uint32(s)&^idMask != want {
			continue
		}
		p := t.pages[s>>(32+tablePageBits)][s>>32&(tablePage-1):]
		switch n := int(p[0]); {
		case n >= 0x80:
			if string(longKey(p)) != string(key) {
				continue
			}
		case n != len(key) || string(p[1:1+n]) != string(key):
			continue
		}

		return int(uint32(s)&idMask) - 1
	}

	return t.add(i, h, key)
}

// add stores a key that the table does not hold, whose probe ended at the
// empty slot i, and returns its id.
func (t *ByteTable) add(i, h uint64, key []byte) int {
	size := len(key)
	need := size + 1
	if size >= 0x80 {
		need = size + binary.MaxVarintLen32
	}
	id := t.n
	if id >= tableMaxKeys {
		panic("vec: a byte table holds no more keys")
	}
	if t.page >= len(t.pages) || t.at+need > len(t.pages[t.page]) {
		t.nextPage(need)
	}
	p := t.pages[t.page]
	at := t.at
	loc := uint32(t.page)<<tablePageBits | uint32(at)
	if size < 0x80 {
		p[at] = byte(size)
		at++
	} else {
		at += binary.PutUvarint(p[at:], uint64(size))
	}
	t.at = at + copy(p[at:], key)
	chunk, in := id>>tableChunkBits, id&(tableChunk-1)
	if chunk == len(t.hashes) {
		t.hashes = append(t.hashes, mem.Raw[uint64](t.arena, tableChunk, tableChunk))
		t.locs = append(t.locs, mem.Raw[uint32](t.arena, tableChunk, tableChunk))
	}
	t.hashes[chunk][in], t.locs[chunk][in] = h, loc
	t.slots[i] = t.slot(h, loc, id)
	t.n++
	if 2*t.n > len(t.slots) {
		t.grow()
	}

	return id
}

func (t *ByteTable) slot(h uint64, loc uint32, id int) uint64 {
	return uint64(loc)<<32 | uint64(uint32(h>>t.fpShift)<<t.idBits|uint32(id+1))
}

// nextPage makes the page being filled one with room for need bytes: one
// that the table kept through a Clear, or a new one, which a key longer than
// a page has to itself.
func (t *ByteTable) nextPage(need int) {
	if len(t.pages) != 0 {
		t.page++
	}
	t.at = 0
	for i := t.page; i < len(t.pages); i++ {
		if len(t.pages[i]) >= need {
			t.pages[t.page], t.pages[i] = t.pages[i], t.pages[t.page]

			return
		}
	}
	if len(t.pages) >= tableMaxPages {
		panic("vec: a byte table holds no more pages of keys")
	}
	size := max(need, tablePage)
	t.pages = append(t.pages, mem.Raw[byte](t.arena, size, size))
	last := len(t.pages) - 1
	t.pages[t.page], t.pages[last] = t.pages[last], t.pages[t.page]
}

// grow gives the keys more slots once they take half of them, and tells
// every key its new one: four times the slots while those are few, which
// keeps most keys in the slot their hash starts at, and twice as many once
// the slots are most of the table's memory.
func (t *ByteTable) grow() {
	if len(t.slots) < tableWideSlots {
		t.setSlots(4 * len(t.slots))
	} else {
		t.setSlots(2 * len(t.slots))
	}
	slots := t.slots
	mask := uint64(len(slots) - 1)
	id := 0
	for c, chunk := range t.hashes {
		locs := t.locs[c]
		for in, h := range chunk[:min(tableChunk, t.n-id)] {
			i := h & mask
			for slots[i] != 0 {
				i = (i + 1) & mask
			}
			slots[i] = t.slot(h, locs[in], id)
			id++
		}
		if id == t.n {
			break
		}
	}
}

// Keys are the keys of a batch: key j is the bytes of Data from
// Offsets[r] to Offsets[r+1], where r is Rows[j], or j when Rows is nil.
type Keys struct {
	Data    []byte
	Offsets []uint32
	Rows    []uint32
}

func (k *Keys) at(j int) []byte {
	r := j
	if k.Rows != nil {
		r = int(k.Rows[j])
	}

	return k.Data[k.Offsets[r]:k.Offsets[r+1]]
}

// IDs sets ids[j] to the id of key j of keys, whose hash is hashes[j], and
// adds the keys the table does not hold in the order of the batch. It looks
// the keys up side by side, so that a table too large for the cache answers
// several at once; with warm set it also reads warm[id*stride] for every key
// that it finds, which leaves the caller's state of those keys in the cache.
func (t *ByteTable) IDs(ids []uint32, hashes []uint64, keys *Keys, warm []int64, stride int) {
	n := len(ids)
	if n == 0 {
		return
	}
	if cap(t.cand) < n {
		tableSlots.Put(t.cand)
		tableRows.Put(t.pending)
		t.cand, t.pending = tableSlots.Get(n), tableRows.Get(n)
	}
	cand, pending := t.cand[:n], t.pending[:0]
	hashes = hashes[:n]
	slots := t.slots
	mask := uint64(len(slots) - 1)
	// Each pass reads what the one before made known: the slot that a hash
	// starts at, then the key that the slots name for the hash and the
	// caller's state of it. Within a pass no read waits for another.
	for j, h := range hashes {
		cand[j] = slots[h&mask]
	}
	idMask := uint32(1)<<t.idBits - 1
	idBits, fpShift := t.idBits, t.fpShift
	touched := t.touched
	for j, h := range hashes {
		s, want := cand[j], uint32(h>>fpShift)<<idBits
		for i := h & mask; s != 0 && uint32(s)&^idMask != want; s = slots[i] {
			i = (i + 1) & mask
		}
		cand[j] = s
		if s == 0 {
			continue
		}
		loc := uint32(s >> 32)
		touched ^= uint64(t.pages[loc>>tablePageBits][loc&(tablePage-1)])
		if warm != nil {
			touched ^= uint64(warm[int(uint32(s)&idMask-1)*stride])
		}
	}
	t.touched = touched
	// A key that the table lacks, or whose hash names another key first, is
	// left to the lookup one by one.
	for j, s := range cand {
		if s != 0 && string(t.keyAt(uint32(s>>32))) == string(keys.at(j)) {
			ids[j] = uint32(s)&idMask - 1

			continue
		}
		pending = append(pending, uint32(j))
	}
	for _, j := range pending {
		ids[j] = uint32(t.IDHashed(keys.at(int(j)), hashes[j]))
	}
}
