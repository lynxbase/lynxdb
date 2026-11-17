package part

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"
)

func packStrings(vals []string) (payload []byte, stride int) {
	for _, v := range vals {
		payload = append(payload, v...)
	}

	return payload, len(vals[0])
}

func TestPackedRoundTripsIdentifierShapes(t *testing.T) {
	r := rand.New(rand.NewSource(7))
	random := func(n int, alphabet string) string {
		b := make([]byte, n)
		for i := range b {
			b[i] = alphabet[r.Intn(len(alphabet))]
		}

		return string(b)
	}
	const base32, hex = "abcdefghijklmnopqrstuvwxyz234567", "0123456789abcdef"
	shapes := map[string]func() string{
		"did": func() string { return "did:plc:" + random(24, base32) },
		"cid": func() string { return "bafyrei" + random(52, base32) },
		"uuid": func() string {
			return random(8, hex) + "-" + random(4, hex) + "-4" + random(3, hex) + "-" + random(4, hex) + "-" + random(12, hex)
		},
		"sha":      func() string { return random(40, hex) },
		"one bit":  func() string { return "flag=" + random(1, "01") },
		"64 chars": func() string { return random(11, "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_") },
	}
	var s packScratch
	for name, gen := range shapes {
		for _, count := range []int{16, 17, 1000, 4097} {
			vals := make([]string, count)
			for i := range vals {
				vals[i] = gen()
			}
			payload, stride := packStrings(vals)
			body, ok := appendSymbols([]byte("head"), payload, stride, &s)
			if !ok {
				t.Fatalf("%s x%d: not packed", name, count)
			}
			body = body[4:]
			if count >= 1000 && len(body) > len(payload)*7/8 {
				t.Errorf("%s x%d: %d bytes packed into %d", name, count, len(payload), len(body))
			}
			got, err := readSymbols([]byte("pre"), body, len(payload), &s)
			if err != nil || !bytes.Equal(got[3:], payload) || string(got[:3]) != "pre" {
				t.Fatalf("%s x%d: round trip failed: %v", name, count, err)
			}
			// Damage must fail or decode to something, never panic.
			for trial := 0; trial < 200; trial++ {
				bad := append([]byte(nil), body...)
				bad[r.Intn(len(bad))] ^= byte(1 + r.Intn(255))
				_, _ = readSymbols(nil, bad, len(payload), &s)
				_, _ = readSymbols(nil, bad[:r.Intn(len(bad))], len(payload), &s)
			}
		}
		if name == "uuid" {
			vals := make([]string, 2000)
			for i := range vals {
				vals[i] = gen()
			}
			payload, stride := packStrings(vals)
			body, _ := appendSymbols(nil, payload, stride, &s)
			// 31 hexadecimal digits vary; the dashes and the version do not.
			if want := 2000 * 31 * 4 / 8; len(body) > want+150 {
				t.Errorf("uuids packed into %d bytes, want about %d", len(body), want)
			}
		}
	}
}

func TestPackedSpendsFewBitsWhereAPositionVariesLittle(t *testing.T) {
	// Sortable base32 timestamps: the leading characters barely change and
	// the trailing ones take every value.
	const alphabet = "234567abcdefghijklmnopqrstuvwxyz"
	var vals []string
	for i := 0; i < 4000; i++ {
		v := uint64(1732206349000000+i*3301) << 10
		b := make([]byte, 13)
		for j := 12; j >= 0; j-- {
			b[j] = alphabet[v&31]
			v >>= 5
		}
		vals = append(vals, string(b))
	}
	payload, stride := packStrings(vals)
	var s packScratch
	body, ok := appendSymbols(nil, payload, stride, &s)
	if !ok {
		t.Fatal("not packed")
	}
	// About 24 bits of the 65 change over this span, against 13 bytes raw.
	if len(body) > 4000*7/2 {
		t.Fatalf("%d strings of 13 bytes packed into %d bytes", len(vals), len(body))
	}
	got, err := readSymbols(nil, body, len(payload), &s)
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("round trip failed: %v", err)
	}
}

func TestPackedLeavesOtherDataToTheGeneralCodec(t *testing.T) {
	r := rand.New(rand.NewSource(9))
	var s packScratch
	text := func() string {
		b := make([]byte, 20)
		for i := range b {
			b[i] = byte(32 + r.Intn(95))
		}

		return string(b)
	}
	cases := map[string][]string{}
	for i := 0; i < 500; i++ {
		cases["text"] = append(cases["text"], text())
		cases["equal"] = append(cases["equal"], "same-value-here")
		// Digits that are mostly zero carry far less than four bits each.
		skewed := "000000000000"
		if r.Intn(10) == 0 {
			skewed = fmt.Sprintf("%012d", r.Int63n(1e12))
		}
		cases["skewed"] = append(cases["skewed"], skewed)
	}
	cases["few"] = []string{"abc1", "abc2", "abc3"}
	for name, vals := range cases {
		payload, stride := packStrings(vals)
		if _, ok := appendSymbols(nil, payload, stride, &s); ok {
			t.Errorf("%s was packed", name)
		}
	}
	if _, ok := appendSymbols(nil, []byte("abcdefg"), 3, &s); ok {
		t.Error("a payload that is no multiple of its stride was packed")
	}
	if _, ok := appendSymbols(nil, []byte("abcdefg"), 0, &s); ok {
		t.Error("a payload without a stride was packed")
	}
}
