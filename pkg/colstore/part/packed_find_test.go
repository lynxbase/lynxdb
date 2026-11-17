package part

import (
	"bytes"
	"math/rand"
	"slices"
	"testing"
)

func TestFindPackedAgreesWithUnpacking(t *testing.T) {
	r := rand.New(rand.NewSource(17))
	alphabets := []string{"0123456789abcdef", "abcdefghijklmnopqrstuvwxyz234567", "01", "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"}
	var scratch packScratch
	for round := 0; round < 300; round++ {
		alphabet := alphabets[r.Intn(len(alphabets))]
		stride := 1 + r.Intn(70)
		count := minPackedStrings + r.Intn(600)
		// A constant prefix, as identifiers of one scheme have, and positions
		// that take few of the symbols.
		prefix := r.Intn(stride)
		narrow := 1 + r.Intn(len(alphabet))
		payload := make([]byte, 0, stride*count)
		for i := 0; i < count; i++ {
			for p := 0; p < stride; p++ {
				switch {
				case p < prefix:
					payload = append(payload, alphabet[p%len(alphabet)])
				case p%3 == 0:
					payload = append(payload, alphabet[r.Intn(narrow)])
				default:
					payload = append(payload, alphabet[r.Intn(len(alphabet))])
				}
			}
		}
		// Repeats, so that a string is found at several numbers.
		for i := 0; i < count/10; i++ {
			copy(payload[r.Intn(count)*stride:][:stride], payload[r.Intn(count)*stride:][:stride])
		}
		body, ok := appendSymbols(nil, payload, stride, &scratch)
		if !ok {
			continue
		}
		back, err := readSymbols(nil, body, len(payload), &scratch)
		if err != nil || !bytes.Equal(back, payload) {
			t.Fatalf("round %d: the section does not unpack to its strings: %v", round, err)
		}
		needles := [][]byte{nil, payload[:stride-1], append(slices.Clone(payload[:stride]), 'x')}
		for i := 0; i < 40; i++ {
			at := r.Intn(count) * stride
			needle := slices.Clone(payload[at : at+stride])
			switch i % 4 {
			case 1:
				needle[r.Intn(stride)] = alphabet[r.Intn(len(alphabet))]
			case 2:
				needle[stride-1] ^= 0x80
			case 3:
				needle[r.Intn(stride)] = '~'
			}
			needles = append(needles, needle)
		}
		needles = append(needles, payload[(count-1)*stride:], payload[:stride])
		for _, needle := range needles {
			var want []uint32
			for i := 0; i < count; i++ {
				if bytes.Equal(payload[i*stride:(i+1)*stride], needle) {
					want = append(want, uint32(i))
				}
			}
			got, err := findPacked(nil, body, len(payload), needle)
			if err != nil || !slices.Equal(got, want) {
				t.Fatalf("round %d, %d strings of %d bytes: %q found at %v, it is at %v (%v)", round, count, stride, needle, got, want, err)
			}
		}
		// A body cut short is refused, never read past its end.
		for cut := 0; cut < len(body); cut += 1 + len(body)/40 {
			if _, err := findPacked(nil, body[:cut], len(payload), payload[:stride]); err != ErrCorrupt {
				t.Fatalf("round %d: a body of %d bytes cut to %d: %v", round, len(body), cut, err)
			}
		}
	}
}
