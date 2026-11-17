package tokenfilter

import (
	"math/rand"
	"regexp"
	"regexp/syntax"
	"strings"
	"testing"
)

func automatonOf(t *testing.T, expr string) *automaton {
	t.Helper()
	tree, err := syntax.Parse(expr, syntax.Perl)
	if err != nil {
		t.Fatalf("%s: %v", expr, err)
	}

	return newAutomaton(tree)
}

func TestAutomatonAgreesWithTheLibrary(t *testing.T) {
	exprs := []string{
		`\d{5}`, `[Gg]ood (morning|night)`, `(?i)crypto\w*`, `^abc`, `abc$`, `^abc$`, `^$`, `a.c`, `(?s)a.c`, `a.*c`, `x*`,
		`(ab|cd)+e`, `[^a-c]x`, `\p{Greek}+z`, `(?i)stra\x{df}e`, `(?i)k`, `(?i)s1`, `\x{e9}+a`, `a{2,4}b`, `(a|b)*abb`,
		`[\x{4e00}-\x{9fff}]{2}`, `\x{fffd}`, `a|^b|c$`, `(^a|b)c`, `a(b$|c)`, `(?i)[k-s]+z`, `\w+@\w+\.com`, `\s\S\s`,
		`^(a|b)$`, `(a$)|(^b)`, `(?U)a+b`, `a*?b`, `[[:upper:]][[:lower:]]+ `, `(|a)b`, `()`, `a()b`, `\Aab\z`,
	}
	pieces := []string{
		"a", "b", "c", "d", "e", "x", "z", "G", "g", "K", "k", "K", "ß", "é", "É", "α", "Ω", "中", "文",
		"\xff", "\xc3", "\n", " ", "1", "5", "12345", "@", ".", "com", "abc", "good morning", "Good night", "Crypto9", "s", "S", "ſ", "Straße", "�",
	}
	r := rand.New(rand.NewSource(11))
	texts := make([][]byte, 20000)
	for i := range texts {
		var text strings.Builder
		for j, n := 0, 1+r.Intn(9); j < n; j++ {
			text.WriteString(pieces[r.Intn(len(pieces))])
		}
		texts[i] = []byte(text.String())
	}
	for _, expr := range exprs {
		a := automatonOf(t, expr)
		if a == nil {
			t.Fatalf("%s has no automaton", expr)
		}
		re := regexp.MustCompile(expr)
		c := newStates(a)
		if _, decided := c.match(nil); decided {
			t.Fatalf("%s: the empty text was decided", expr)
		}
		matches := 0
		for _, text := range texts {
			got, decided := c.match(text)
			if want := re.Match(text); !decided || got != want {
				t.Fatalf("%s on %q: %v (decided %v), the library says %v", expr, text, got, decided, want)
			}
			if got {
				matches++
			}
		}
		if len(c.sets) >= c.limit {
			t.Errorf("%s filled the cache with %d states", expr, len(c.sets))
		}
		t.Logf("%-28s %5d of %d texts match; %d states, %d classes of runes", expr, matches, len(texts), len(c.sets)-1, c.classes)
	}
}

func TestAutomatonLeavesOtherAssertionsToTheLibrary(t *testing.T) {
	for _, expr := range []string{`\bfoo\b`, `(?m)^foo`, `(?m)foo$`, `a\Bb`} {
		if automatonOf(t, expr) != nil {
			t.Errorf("%s has an automaton", expr)
		}
		p, err := Regexp(expr)
		if err != nil {
			t.Fatal(err)
		}
		re := regexp.MustCompile(expr)
		for _, text := range []string{"foo", "a foo b", "afoob", "x\nfoo\ny", "ab", "a b", ""} {
			if got, want := p.Match([]byte(text)), re.MatchString(text); got != want {
				t.Errorf("%s on %q: %v, the library says %v", expr, text, got, want)
			}
		}
	}
}

func TestRegexpAnswersWhenTheAutomatonIsFull(t *testing.T) {
	const expr = `(a|b)*a(a|b){14}c`
	p, err := Regexp(expr)
	if err != nil {
		t.Fatal(err)
	}
	re := regexp.MustCompile(expr)
	r := rand.New(rand.NewSource(3))
	undecided := 0
	c := p.states()
	defer p.release(c)
	for range 400 {
		text := make([]byte, 300)
		for i := range text {
			text[i] = "ab"[r.Intn(2)]
		}
		if r.Intn(2) == 0 {
			text[100+r.Intn(100)] = 'c'
		}
		if _, decided := c.match(text); !decided {
			undecided++
		}
		if got, want := p.Match(text), re.Match(text); got != want {
			t.Fatalf("%q: %v, the library says %v", text, got, want)
		}
	}
	if undecided == 0 || len(c.sets) != c.limit {
		t.Fatalf("the automaton decided every text with %d states, so the test met no full cache", len(c.sets))
	}
}

func BenchmarkRegexpWithoutLiteral(b *testing.B) {
	text := []byte(strings.Repeat("the quick brown fox jumps over the lazy dog 1234 times, said no one éver ", 4))
	for _, expr := range []string{`\d{5}`, `[A-Z][a-z]+ [A-Z][a-z]+`, `(?i)(error|warn)ing\d`} {
		p, err := Regexp(expr)
		if err != nil {
			b.Fatal(err)
		}
		re := regexp.MustCompile(expr)
		b.Run("automaton "+expr, func(b *testing.B) {
			b.SetBytes(int64(len(text)))
			for b.Loop() {
				if p.Match(text) {
					b.Fatal("match")
				}
			}
		})
		b.Run("library "+expr, func(b *testing.B) {
			b.SetBytes(int64(len(text)))
			for b.Loop() {
				if re.Match(text) {
					b.Fatal("match")
				}
			}
		})
	}
}

func TestAutomatonBoundsItsTablesForManyClassesOfRunes(t *testing.T) {
	a := automatonOf(t, `(\p{L}|\p{N})+\p{Greek}{3}\p{Han}`)
	if a == nil {
		t.Fatal("no automaton")
	}
	c := newStates(a)
	if c.classes < 500 {
		t.Fatalf("the expression tells only %d classes of runes apart", c.classes)
	}
	if bytes := 4 * c.limit * (1<<7 + c.classes); c.limit < 8 || bytes > maxTableBytes {
		t.Fatalf("%d states of %d classes may take %d bytes", c.limit, c.classes, bytes)
	}
}
