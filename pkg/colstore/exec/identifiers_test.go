package exec_test

import (
	"context"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/colstore/exec"
)

// TestEqualityOnIdentifiersFindsTheRows requires an equality test on a
// column of identifiers to give exactly the rows that hold the value,
// whatever form the column is stored in: values of one length that are
// distinct, that repeat, that come from a few, and that some rows lack, in
// blocks small and large.
func TestEqualityOnIdentifiersFindsTheRows(t *testing.T) {
	const rows = 9000
	rng := rand.New(rand.NewPCG(11, 12))
	id := func(prefix string, n int) string {
		return fmt.Sprintf("%s%020d", prefix, uint64(n)*0x9E3779B97F4A7C15%1e18)
	}
	type doc struct{ uid, user, tag string }
	docs := make([]doc, rows)
	raw := make([][]byte, rows)
	for i := range docs {
		d := &docs[i]
		fields := []string{fmt.Sprintf(`"n":%d,"ts":%d`, i, 1700000000000000+i)}
		if i%10 != 3 {
			d.uid = id("u", i)
			fields = append(fields, fmt.Sprintf(`"uid":%q`, d.uid))
		}
		if i%7 != 5 {
			d.user = id("did:plc:", rng.IntN(rows*6/10))
			fields = append(fields, fmt.Sprintf(`"user":%q`, d.user))
		}
		if i%2 == 0 {
			d.tag = id("t", rng.IntN(30))
			fields = append(fields, fmt.Sprintf(`"tag":%q`, d.tag))
		}
		raw[i] = []byte("{" + strings.Join(fields, ",") + "}")
	}
	field := map[string]func(d *doc) string{
		"uid":  func(d *doc) string { return d.uid },
		"user": func(d *doc) string { return d.user },
		"tag":  func(d *doc) string { return d.tag },
	}
	for _, l := range layouts {
		t.Run(l.name, func(t *testing.T) {
			tbl := buildTable(t, raw, "ts", l)
			snap := tbl.Snapshot()
			defer snap.Release()
			found := func(filter *exec.Expr) []string {
				plan := &exec.Project{Input: &exec.Scan{Filter: filter}, Exprs: []exec.Named{named("n", num("n"))}}
				res, err := exec.Run(context.Background(), snap, plan, exec.Options{Workers: 3, Forgetful: true})
				if err != nil {
					t.Fatal(err)
				}
				got := namedRows(res)
				slices.Sort(got)

				return got
			}
			for name, get := range field {
				wanted := []string{"absent", id("u", rows+7), get(&docs[0]), get(&docs[rows/2]), get(&docs[rows-1]), get(&docs[rows/3+1])}
				for _, want := range wanted {
					if want == "" {
						continue
					}
					var expect []string
					for i := range docs {
						if get(&docs[i]) == want {
							expect = append(expect, fmt.Sprintf("n=%d", i))
						}
					}
					slices.Sort(expect)
					if got := found(exec.Eq(anyOf(name), exec.Str(want))); !slices.Equal(got, expect) {
						t.Fatalf("%s = %s: rows %v, want %v", name, want, got, expect)
					}
					// With a second test on another column of the kind.
					other := docs[rows/2].tag
					expect = expect[:0]
					for i := range docs {
						if get(&docs[i]) == want && docs[i].tag == other {
							expect = append(expect, fmt.Sprintf("n=%d", i))
						}
					}
					slices.Sort(expect)
					both := exec.And(exec.Eq(anyOf(name), exec.Str(want)), exec.Eq(anyOf("tag"), exec.Str(other)))
					if got := found(both); !slices.Equal(got, expect) {
						t.Fatalf("%s = %s and tag = %s: rows %v, want %v", name, want, other, got, expect)
					}
				}
			}
		})
	}
}
