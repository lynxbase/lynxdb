package exec_test

import (
	"context"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/colstore/exec"
)

// withFilter returns plan with the filter of its scan replaced by what f
// makes of it.
func withFilter(n exec.Node, f func(*exec.Expr) *exec.Expr) exec.Node {
	switch n := n.(type) {
	case *exec.Sort:
		c := *n
		c.Input = withFilter(n.Input, f)

		return &c
	case *exec.Project:
		c := *n
		c.Input = withFilter(n.Input, f)

		return &c
	case *exec.Filter:
		c := *n
		c.Input = withFilter(n.Input, f)

		return &c
	case *exec.Aggregate:
		c := *n
		c.Input = withFilter(n.Input, f)

		return &c
	case *exec.Scan:
		c := *n
		c.Filter = f(n.Filter)

		return &c
	}

	return n
}

// hidden returns e or false, which holds where e does and tells the scan
// nothing it can narrow by.
func hidden(e *exec.Expr) *exec.Expr { return exec.Or(e, exec.Bool(false)) }

// hideLists hides every disjunction among the conjuncts of a filter.
func hideLists(e *exec.Expr) *exec.Expr {
	switch {
	case e == nil:
		return nil
	case e.Op == exec.OpOr:
		return hidden(e)
	case e.Op != exec.OpAnd:
		return e
	}
	args := make([]*exec.Expr, len(e.Args))
	for i, a := range e.Args {
		args[i] = hideLists(a)
	}

	return exec.And(args...)
}

func TestKeyListsNarrowTheScanToTheRowsTheyHold(t *testing.T) {
	s := timeCountStore(t)
	defer s.Close()
	snap := s.Table().Snapshot()
	defer snap.Release()
	created := `kind = 'commit' and commit.operation = 'create'`
	cases := []struct {
		name, where string
		// narrows is set when the list leaves out blocks that the
		// equalities before it keep.
		narrows bool
	}{
		{"the third query's list", created + ` and commit.collection in ('app.bsky.feed.post', 'app.bsky.feed.repost', 'app.bsky.feed.like')`, true},
		{"values that do not occur", created + ` and commit.collection in ('app.bsky.feed.like', 'no.such.collection', 'app.bsky.feed.post')`, true},
		{"values out of order and twice", created + ` and commit.collection in ('app.bsky.graph.follow', 'app.bsky.feed.like', 'app.bsky.graph.follow', 'app.bsky.feed.like')`, true},
		{"equalities joined by or", created + ` and (commit.collection = 'app.bsky.feed.repost' or commit.collection = 'app.bsky.graph.block')`, true},
		{"a list after one equality", `kind = 'commit' and commit.operation in ('update', 'delete')`, true},
		{"a list on the first key path", `kind in ('identity', 'account')`, true},
		{"a list on the first key path of values that do not occur", `kind in ('none', 'nothing')`, true},
		{"a list on the last key path", created + ` and commit.collection = 'app.bsky.feed.like' and did in ('did:plc:none', 'did:plc:untimed')`, true},
		{"one value in a list", created + ` and commit.collection in ('app.bsky.feed.repost')`, true},
		// Lists that do not follow the equalities, or are not lists, leave
		// the scan as the equalities make it.
		{"a list after a gap", `kind = 'commit' and commit.collection in ('app.bsky.feed.post', 'app.bsky.feed.like')`, false},
		{"a list beside another path", created + ` and (commit.collection = 'app.bsky.feed.post' or did = 'did:plc:untimed')`, false},
		{"values left out", created + ` and commit.collection not in ('app.bsky.feed.post', 'app.bsky.feed.like')`, false},
	}
	run := func(t *testing.T, plan exec.Node) *exec.Result {
		t.Helper()
		res, err := exec.Run(context.Background(), snap, plan, exec.Options{Forgetful: true})
		if err != nil {
			t.Fatal(err)
		}

		return res
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			text := `source=x | where ` + c.where + ` | stats count() as n, dc(did) as users by kind, commit.operation, commit.collection | sort kind, commit.operation, commit.collection`
			plan, err := pplPlan(text, time.Now())
			if err != nil {
				t.Fatal(err)
			}
			got := run(t, plan)
			want := run(t, withFilter(plan, hidden))
			if g, w := rendered(got), rendered(want); g != w {
				t.Fatalf("answer\n %s\nwant\n %s", g, w)
			}
			prefixed := run(t, withFilter(plan, hideLists))
			if rendered(prefixed) != rendered(want) {
				t.Fatal("the answer without lists is not that of the rows")
			}
			if st := got.Stats; st.Blocks > prefixed.Stats.Blocks || (st.Blocks < prefixed.Stats.Blocks) != c.narrows {
				t.Fatalf("%d blocks opened, %d without the list, %d without narrowing", st.Blocks, prefixed.Stats.Blocks, want.Stats.Blocks)
			}
		})
	}
}
