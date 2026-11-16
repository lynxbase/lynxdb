package tokenfilter

import (
	"fmt"
	"math/rand"
	"testing"
)

func hashesOf(s string) []uint64 { return AppendHashes(nil, []byte(s)) }

func TestTokensFoldCaseAndSplitOnPunctuation(t *testing.T) {
	a := hashesOf("GET /api/v1/Users?id=42 at://did:plc:AbC")
	b := hashesOf("get api v1 users id 42 at did plc abc")
	if len(a) != 10 || len(b) != len(a) {
		t.Fatalf("token counts %d and %d, want 10", len(a), len(b))
	}
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("token %d hashes differently across case and separators", i)
		}
	}
	if x, y := hashesOf("Привет, МИР! 東京"), hashesOf("привет мир 東京"); len(x) != 3 || x[0] != y[0] || x[1] != y[1] || x[2] != y[2] {
		t.Fatalf("non-ASCII tokens do not fold: %v %v", x, y)
	}
	if got := hashesOf("  --- \xff\xfe ... "); len(got) != 0 {
		t.Fatalf("separators produced %d tokens", len(got))
	}
	if x, y := hashesOf("ab"), hashesOf("a b"); len(x) != 1 || len(y) != 2 || x[0] == y[0] {
		t.Fatal("token boundaries are not part of the hash")
	}
}

func TestSetDeduplicates(t *testing.T) {
	var s Set
	s.Add([]uint64{5, 0, 5, 7, 0})
	if len(s.Keys()) != 3 {
		t.Fatalf("Keys = %v", s.Keys())
	}
	for i := 0; i < 5000; i++ {
		s.Add([]uint64{uint64(i) * 2654435761})
	}
	n := len(s.Keys())
	for i := 0; i < 5000; i++ {
		s.Add([]uint64{uint64(i) * 2654435761})
	}
	if len(s.Keys()) != n {
		t.Fatal("re-adding changed the set")
	}
	s.Reset()
	if len(s.Keys()) != 0 {
		t.Fatal("Reset left keys")
	}
	s.Add([]uint64{5})
	if len(s.Keys()) != 1 {
		t.Fatal("set unusable after Reset")
	}
}

func TestFilterHasNoFalseNegativesAndFewFalsePositives(t *testing.T) {
	for _, n := range []int{0, 1, 10, 1000, 50000} {
		var set Set
		for i := 0; i < n; i++ {
			set.Add(hashesOf(fmt.Sprintf("token%d", i)))
		}
		filter := Append([]byte("prefix"), set.Keys())[len("prefix"):]
		for i := 0; i < n; i++ {
			if !MayContain(filter, hashesOf(fmt.Sprintf("TOKEN%d", i))[0]) {
				t.Fatalf("n=%d: token %d missing", n, i)
			}
		}
		if n < 1000 {
			continue
		}
		r := rand.New(rand.NewSource(int64(n)))
		const probes = 200000
		hits := 0
		for i := 0; i < probes; i++ {
			if MayContain(filter, mix(r.Uint64())) {
				hits++
			}
		}
		if rate := float64(hits) / probes; rate > 0.012 {
			t.Errorf("n=%d: false-positive rate %.4f at %.1f bits per token", n, rate, float64(len(filter)*8)/float64(n))
		} else {
			t.Logf("n=%d: false-positive rate %.4f, %d bytes", n, rate, len(filter))
		}
	}
	if !MayContain(nil, 123) {
		t.Fatal("an absent filter must not exclude anything")
	}
}

func BenchmarkAppendHashes(b *testing.B) {
	text := []byte(`at://did:plc:6tr6tuzlx2db3rduzr2d6r24/app.bsky.feed.post/3lbhqo2rtys2z aaaaah. LIght shines in a corner of WTF bafyreibfglofvqou2yiqvwzk4rcgkhhxrbunyemshdjledgwymimqkg24e`)
	var dst []uint64
	b.SetBytes(int64(len(text)))
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		dst = AppendHashes(dst[:0], text)
	}
}
