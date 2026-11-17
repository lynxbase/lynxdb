package tokenfilter

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"unicode"
	"unicode/utf8"
)

// mix scrambles a value into a probe that looks nothing like a token hash.
func mix(h uint64) uint64 {
	h ^= h >> 33
	h *= 0xff51afd7ed558ccd
	h ^= h >> 33
	h *= 0xc4ceb9fe1a85ec53
	h ^= h >> 33

	return h
}

// referenceHashes is the definition of AppendHashes written for clarity: it
// collects the case-folded bytes of each token and hashes them word by word.
func referenceHashes(text []byte) []uint64 {
	var out []uint64
	var token []byte
	flush := func() {
		if len(token) == 0 {
			return
		}
		h := uint64(hashSeed)
		rest := token
		for ; len(rest) >= 8; rest = rest[8:] {
			h = mum(h^binary.LittleEndian.Uint64(rest), hashMul)
		}
		var tail [8]byte
		copy(tail[:], rest)
		out = append(out, mum(h^binary.LittleEndian.Uint64(tail[:]), hashMul))
		token = token[:0]
	}
	for i := 0; i < len(text); {
		r, size := utf8.DecodeRune(text[i:])
		i += size
		if r == utf8.RuneError && size <= 1 || !unicode.IsLetter(r) && !unicode.IsDigit(r) {
			flush()

			continue
		}
		token = utf8.AppendRune(token, unicode.ToLower(r))
	}
	flush()

	return out
}

func checkAgainstReference(t *testing.T, text []byte) {
	t.Helper()
	want := referenceHashes(text)
	got := AppendHashes(nil, text)
	if len(got) != len(want) {
		t.Fatalf("%q: %d tokens, reference has %d", text, len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("%q: token %d hashes differently from the reference", text, i)
		}
	}
}

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

// Folding must not depend on how many bytes a rune or its lowercase form
// takes: the Kelvin sign folds to an ASCII k, and U+023A grows by a byte.
func TestFoldingAcrossEncodedLengths(t *testing.T) {
	pairs := [][2]string{
		{"K", "k"}, {"İ", "i"}, {"Ⱥb", "ⱥb"},
		{"ÀÉÎ", "àéî"}, {"ΑΘΗΝΑ", "αθηνα"}, {"ԱՐԱՐԱՏ", "արարատ"},
		{"XKY", "xky"}, {"abcdefgK", "abcdefgk"},
	}
	for _, p := range pairs {
		x, y := hashesOf(p[0]), hashesOf(p[1])
		if len(x) != 1 || len(y) != 1 || x[0] != y[0] {
			t.Errorf("%q and %q do not hash alike", p[0], p[1])
		}
	}
}

// A token must hash the same wherever it sits in the text, which the
// word-at-a-time scanner could get wrong at any of its eight alignments.
func TestHashIsIndependentOfPosition(t *testing.T) {
	tokens := []string{"a", "ab", "abcdefg", "abcdefgh", "abcdefghi", "abcdefghijklmnop", "abcdefghijklmnopqrstuvwxyz0123456789", "я", "токен", "東京", "mixedЯзык東"}
	for _, tok := range tokens {
		want := hashesOf(tok)
		if len(want) != 1 {
			t.Fatalf("%q is not one token", tok)
		}
		for pad := 0; pad < 20; pad++ {
			for _, sep := range []string{" ", "—", "\xff"} {
				text := strings.Repeat(sep, pad) + tok + sep + strings.Repeat("x", pad)
				got := hashesOf(text)
				if len(got) == 0 || got[0] != want[0] {
					t.Fatalf("%q hashes differently after %d separators %q", tok, pad, sep)
				}
			}
		}
	}
}

func TestScannerMatchesReference(t *testing.T) {
	cases := []string{
		"", "a", " ", "a b", "abcdefgh", "abcdefghi", " abcdefgh ", "aaaaaaaa bbbbbbbb cccccccc",
		"GET /api/v1/Users?id=42 at://did:plc:AbC",
		"Привет, МИР! 東京", "«привет» — мир", "café au lait, GASTON",
		"ab\xffcd ef\xfegh", "\xe0\x80\x80x", "\xed\xa0\x80y", "\xf0\x9f\x98\x80z", "\xc3", "ab\xe6\x9d", "\xf4\x90\x80\x80",
		"one-two_three.four/five:six=seven",
		strings.Repeat("token ", 100), strings.Repeat("a", 200), strings.Repeat("ba", 100), strings.Repeat("я", 37),
		"𝒳𝒴𝒵 𐐀𐐨 astral letters",
	}
	for _, text := range cases {
		checkAgainstReference(t, []byte(text))
	}

	r := rand.New(rand.NewSource(7))
	alphabets := [][]byte{
		[]byte("abzAZ09 \t,./:=?-_!\"'()[]{}|&*#@~^;+<>\\\x00\x7f"),
		[]byte("abzAZ09 ,.\xc3\xa9\xd0\x9f\xd0\xbf\xe6\x9d\xb1\xff\x80\xc2\xe2\x84\xaa\xc8\xba\xf0\x9d\x92\xb3"),
		[]byte("abcdefghijklmnopqrstuvwxyz0123456789    "),
	}
	for i := 0; i < 60000; i++ {
		alphabet := alphabets[i%len(alphabets)]
		b := make([]byte, r.Intn(48))
		for j := range b {
			b[j] = alphabet[r.Intn(len(alphabet))]
		}
		checkAgainstReference(t, b)
	}
	for i := 0; i < 20000; i++ {
		b := make([]byte, r.Intn(24))
		r.Read(b)
		checkAgainstReference(t, b)
	}
}

func TestEveryRuneMatchesReference(t *testing.T) {
	var buf []byte
	for r := rune(0); r <= unicode.MaxRune; r++ {
		if r >= 0xD800 && r <= 0xDFFF {
			continue
		}
		buf = utf8.AppendRune(append(buf[:0], "ab"...), r)
		buf = append(buf, "cd"...)
		want := referenceHashes(buf)
		got := AppendHashes(nil, buf)
		if len(got) != len(want) {
			t.Fatalf("U+%04X: %d tokens, reference has %d", r, len(got), len(want))
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("U+%04X: token %d hashes differently from the reference", r, i)
			}
		}
	}
}

func FuzzAppendHashes(f *testing.F) {
	for _, s := range []string{"GET /api/v1 200", "Привет МИР", "a\xffb", "Kİ", "abcdefghijklmnopq rst"} {
		f.Add([]byte(s))
	}
	f.Fuzz(func(t *testing.T, text []byte) {
		want := referenceHashes(text)
		got := AppendHashes(nil, text)
		if len(got) != len(want) {
			t.Fatalf("%q: %d tokens, reference has %d", text, len(got), len(want))
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("%q: token %d hashes differently from the reference", text, i)
			}
		}
	})
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
		filter := Append([]byte("prefix"), set.Keys(), 1)[len("prefix"):]
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

// Tokens that differ in a character or two are what real columns hold, and
// a weak hash would let them collide in the filter.
func TestFilterSeparatesSimilarTokens(t *testing.T) {
	families := map[string]func(i int) string{
		"decimal numbers": func(i int) string { return fmt.Sprint(i) },
		"hex ids":         func(i int) string { return fmt.Sprintf("%08x", uint32(i)*2654435761) },
		"long shared prefix": func(i int) string {
			return fmt.Sprintf("bafyreibfglofvqou2yiqvwzk4rcgkhhxrbunyemshdjledgwymimqk%05d", i)
		},
		"four letters": func(i int) string {
			return string([]byte{byte('a' + i%26), byte('a' + i/26%26), byte('a' + i/676%26), byte('a' + i/17576%26)})
		},
	}
	for name, gen := range families {
		const n = 15000
		var set Set
		for i := 0; i < n; i++ {
			set.Add(hashesOf(gen(i)))
		}
		if len(set.Keys()) != n {
			t.Errorf("%s: %d of %d tokens have distinct hashes", name, len(set.Keys()), n)
		}
		filter := Append(nil, set.Keys(), 1)
		hits := 0
		for i := n; i < 2*n; i++ {
			if MayContain(filter, hashesOf(gen(i))[0]) {
				hits++
			}
		}
		if rate := float64(hits) / n; rate > 0.015 {
			t.Errorf("%s: false-positive rate %.4f", name, rate)
		}
	}
}

// Filters of any sizes can be cut into the same number of pieces and a hash
// looked up in the same piece of each.
func TestHashFallsInTheSamePieceOfFiltersOfAnySize(t *testing.T) {
	r := rand.New(rand.NewSource(1))
	hashes := []uint64{0, 1, 1<<32 - 1, 1 << 32, 1<<63 - 1, 1 << 63, ^uint64(0), 0xffffffff << 32}
	for len(hashes) < 200000 {
		hashes = append(hashes, r.Uint64())
	}
	for _, h := range hashes {
		pieces, per := 1+r.Intn(300), 1+r.Intn(5000)
		if got, want := Block(h, pieces*per)/per, Block(h, pieces); got != want {
			t.Fatalf("hash %x falls in piece %d of %d in a filter of %d bloom blocks and in bloom block %d of %d",
				h, got, pieces, pieces*per, want, pieces)
		}
	}
}

func TestAppendRoundsFiltersUpToTheUnit(t *testing.T) {
	for _, n := range []int{0, 1, 21, 22, 700, 30000} {
		var set Set
		for i := 0; i < n; i++ {
			set.Add(hashesOf(fmt.Sprintf("token%d", i)))
		}
		natural := len(Append(nil, set.Keys(), 1))
		for _, unit := range []int{1, 2, 7, 16, 128} {
			filter := Append(nil, set.Keys(), unit)
			if step := unit * BlockBytes; len(filter)%step != 0 || len(filter) < natural || len(filter) >= natural+step {
				t.Fatalf("%d tokens, unit %d: %d bytes for a natural size of %d", n, unit, len(filter), natural)
			}
			for i := 0; i < n; i++ {
				if h := hashesOf(fmt.Sprintf("token%d", i))[0]; !MayContain(filter, h) || !BlockMayContain(filter[Block(h, len(filter)/BlockBytes)*BlockBytes:], h) {
					t.Fatalf("%d tokens, unit %d: token %d missing", n, unit, i)
				}
			}
		}
	}
}

func benchmarkText(b *testing.B, text string) {
	value := []byte(text)
	var dst []uint64
	b.SetBytes(int64(len(value)))
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		dst = AppendHashes(dst[:0], value)
	}
}

func BenchmarkAppendHashes(b *testing.B) {
	texts := map[string]string{
		"long":     `at://did:plc:6tr6tuzlx2db3rduzr2d6r24/app.bsky.feed.post/3lbhqo2rtys2z aaaaah. LIght shines in a corner of WTF bafyreibfglofvqou2yiqvwzk4rcgkhhxrbunyemshdjledgwymimqkg24e`,
		"cell":     "api-gateway-eu-west-1",
		"tiny":     "GET",
		"ident":    "did:plc:yj3sjq3blzpynh27cumnp5ks",
		"logline":  `Dec 10 06:55:46 LabSZ sshd[24200]: Invalid user webmaster from 173.234.31.186 port 38926 ssh2`,
		"cyrillic": "привет мир — это тест токенизации на кириллице, пока, друг",
		"cjk":      "東京都渋谷区のログ解析システム、エラー検出 2024年",
	}
	for _, name := range []string{"long", "cell", "tiny", "ident", "logline", "cyrillic", "cjk"} {
		b.Run(name, func(b *testing.B) { benchmarkText(b, texts[name]) })
	}
}

// BenchmarkBlockTokens hashes a block's worth of short values into the set,
// the work the writer does for every string column.
func BenchmarkBlockTokens(b *testing.B) {
	values := make([][]byte, 0, 1024)
	levels := []string{"info", "warn", "error", "debug"}
	services := []string{"api-gateway", "billing", "ingest", "query-frontend"}
	size := 0
	for i := 0; i < 1024; i++ {
		v := levels[i%len(levels)]
		if i%2 == 1 {
			v = services[i%len(services)]
		}
		values = append(values, []byte(v))
		size += len(v)
	}
	var (
		dst []uint64
		set Set
	)
	b.SetBytes(int64(size))
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		set.Reset()
		for _, v := range values {
			dst = AppendHashes(dst[:0], v)
			set.Add(dst)
		}
	}
}
