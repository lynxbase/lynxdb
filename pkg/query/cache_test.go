package query

import (
	"container/list"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/ir"
)

func TestPlanCacheReturnsIndependentResults(t *testing.T) {
	const q = "source=logs | where status >= 500 | stats count() by host"
	a, err := Compile(q)
	if err != nil {
		t.Fatal(err)
	}
	b, err := Compile(q)
	if err != nil {
		t.Fatal(err)
	}
	if a == b || a.Plan == b.Plan {
		t.Fatal("cached compiles share a mutable result")
	}
	if ir.Render(a.Plan) != ir.Render(b.Plan) || ir.Render(a.Logical) != ir.Render(b.Logical) {
		t.Fatal("cached plan differs")
	}
	a.Plan.Root = &ir.Values{}
	a.Sources = append(a.Sources, "other")
	c, err := Compile(q)
	if err != nil {
		t.Fatal(err)
	}
	if ir.Render(c.Plan) != ir.Render(b.Plan) || len(c.Sources) != len(b.Sources) {
		t.Fatal("changing a result changed the cache")
	}
}

func TestPlanCacheEvictsLeastRecentlyUsed(t *testing.T) {
	c := &planCache{entries: map[planKey]*list.Element{}, max: 2}
	for _, src := range []string{"a", "b", "a", "c"} {
		if _, ok := c.get(planKey{src: src}); !ok {
			c.put(planKey{src: src}, &Compiled{Source: src})
		}
	}
	for src, want := range map[string]bool{"a": true, "b": false, "c": true} {
		if _, ok := c.get(planKey{src: src}); ok != want {
			t.Errorf("%s cached = %v, want %v", src, ok, want)
		}
	}
	c.put(planKey{src: strings.Repeat("x", planCacheMaxQuery+1)}, &Compiled{})
	if c.order.Len() != 2 {
		t.Errorf("a long query was cached")
	}
}

// TestPlanCacheKeysByOptions pins that a plan compiled with a default source
// is not reused for the same text without one, nor the other way around.
func TestPlanCacheKeysByOptions(t *testing.T) {
	const q = "where status >= 500 | stats count()"
	for i := 0; i < 2; i++ {
		if _, err := Compile(q); err == nil {
			t.Fatalf("%s compiled without a default source", q)
		}
		c, err := CompileWith(q, Options{DefaultSource: "main"})
		if err != nil {
			t.Fatal(err)
		}
		if len(c.Sources) != 1 || c.Sources[0] != "main" {
			t.Fatalf("sources = %v, want [main]", c.Sources)
		}
	}
}

func TestPlanCacheConcurrentCompiles(t *testing.T) {
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				q := "source=logs | head " + strconv.Itoa((g*7+i)%50+1)
				if _, err := Compile(q); err != nil {
					t.Error(err)
					return
				}
			}
		}(g)
	}
	wg.Wait()
}
