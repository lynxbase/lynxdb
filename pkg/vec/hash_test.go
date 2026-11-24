package vec

import (
	"fmt"
	"math/rand"
	"testing"
)

func TestHashReadsEveryByteOfEveryLength(t *testing.T) {
	r := rand.New(rand.NewSource(11))
	seen := map[uint64]string{}
	for n := 0; n <= 200; n++ {
		key := make([]byte, n)
		r.Read(key)
		h := Hash(key)
		if other, ok := seen[h]; ok {
			t.Fatalf("keys %x and %s share a hash", key, other)
		}
		seen[h] = fmt.Sprintf("%x", key)
		for i := range key {
			key[i] ^= 0x40
			if Hash(key) == h {
				t.Fatalf("a key of %d bytes keeps its hash when byte %d changes", n, i)
			}
			key[i] ^= 0x40
		}
		if Hash(append(key, 0)) == h {
			t.Fatalf("a key of %d bytes keeps its hash with a zero byte more", n)
		}
	}
}

// TestHashSpreadsTheBitsThatTablesRead counts keys that differ in a few
// digits by the bits that pick a slot, a partition and a fingerprint: every
// value of each must take about its share.
func TestHashSpreadsTheBitsThatTablesRead(t *testing.T) {
	const keys, buckets = 1 << 20, 256
	var slot, partition, fingerprint [buckets]int
	for i := 0; i < keys; i++ {
		h := Hash([]byte(fmt.Sprintf("did:plc:%024d", i)))
		slot[h%buckets]++
		partition[h>>(64-tableFingerprint-tablePartition)%buckets]++
		fingerprint[h>>56]++
	}
	for name, counts := range map[string][buckets]int{"slot": slot, "partition": partition, "fingerprint": fingerprint} {
		for b, n := range counts {
			if want := keys / buckets; n < want*9/10 || n > want*11/10 {
				t.Fatalf("%s bits: value %d has %d keys, want about %d", name, b, n, want)
			}
		}
	}
}

func BenchmarkHash(b *testing.B) {
	for _, n := range []int{8, 32, 100, 1000} {
		key := make([]byte, n)
		rand.New(rand.NewSource(1)).Read(key)
		b.Run(fmt.Sprintf("bytes=%d", n), func(b *testing.B) {
			b.SetBytes(int64(n))
			var x uint64
			for i := 0; i < b.N; i++ {
				x ^= Hash(key)
			}
			_ = x
		})
	}
}
