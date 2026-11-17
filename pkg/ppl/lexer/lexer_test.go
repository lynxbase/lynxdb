package lexer

import "testing"

func TestEveryWhiteSpaceSeparatesTokens(t *testing.T) {
	for _, src := range []string{"a b", "a\vb", "a b", "a b", "a\u0085b"} {
		toks, errs := Tokenize(src)
		if len(errs) > 0 || len(toks) != 3 {
			t.Fatalf("%q: tokens %v, errors %v", src, toks, errs)
		}
		if !toks[1].SpaceBefore {
			t.Errorf("%q: the second token is not preceded by space", src)
		}
	}
}
