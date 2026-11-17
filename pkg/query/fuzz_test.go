package query

import (
	"testing"

	"github.com/lynxbase/lynxdb/pkg/ir"
	"github.com/lynxbase/lynxdb/pkg/ppl"
)

// FuzzCompile requires that compiling any text either fails with an error
// or yields a valid plan; it must never panic.
func FuzzCompile(f *testing.F) {
	for _, q := range corpusQueries(f) {
		f.Add(q)
	}
	for _, q := range typicalQueries {
		f.Add(q.ppl)
	}
	f.Add("sourCe=0|stAts perCentile(0,A)")
	f.Fuzz(func(t *testing.T, src string) {
		c, err := Compile(src)
		if err != nil {
			if _, ok := ppl.AsError(err); !ok {
				t.Fatalf("%q: error without a kind: %v", src, err)
			}
			return
		}
		if err := ir.Validate(c.Plan); err != nil {
			t.Fatalf("%q: invalid plan: %v", src, err)
		}
		_ = Explain(c)
	})
}
