package part

import (
	"math/rand"
	"testing"
)

func BenchmarkPackedIdentifiers(b *testing.B) {
	r := rand.New(rand.NewSource(1))
	const base32 = "abcdefghijklmnopqrstuvwxyz234567"
	var payload []byte
	for i := 0; i < 16384; i++ {
		payload = append(payload, "bafyrei"...)
		for j := 0; j < 52; j++ {
			payload = append(payload, base32[r.Intn(32)])
		}
	}
	var s packScratch
	body, ok := appendSymbols(nil, payload, 59, &s)
	if !ok {
		b.Fatal("not packed")
	}
	b.Run("pack", func(b *testing.B) {
		b.SetBytes(int64(len(payload)))
		for i := 0; i < b.N; i++ {
			body, _ = appendSymbols(body[:0], payload, 59, &s)
		}
	})
	out := make([]byte, 0, len(payload))
	b.Run("unpack", func(b *testing.B) {
		b.SetBytes(int64(len(payload)))
		for i := 0; i < b.N; i++ {
			out, _ = readSymbols(out[:0], body, len(payload), &s)
		}
	})
	var zbuf, raw []byte
	bc := blobCodec{codec: CodecZstd, level: LevelFast}
	blob := bc.appendBlob(nil, payload, &zbuf)
	b.Run("zstd encode", func(b *testing.B) {
		b.SetBytes(int64(len(payload)))
		for i := 0; i < b.N; i++ {
			blob = bc.appendBlob(blob[:0], payload, &zbuf)
		}
	})
	b.Run("zstd decode", func(b *testing.B) {
		b.SetBytes(int64(len(payload)))
		for i := 0; i < b.N; i++ {
			if _, _, _, err := readBlob(blob, &raw); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Logf("raw %d bytes, packed %d, zstd %d", len(payload), len(body), len(blob))
}
