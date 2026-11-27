package columnar

import (
	"context"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/ppl"
)

func TestSubqueryAndOuterScanKeepTheirSource(t *testing.T) {
	db, err := Open(testConfig(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := appendTo(db, "web", lines("web", 0, 10)); err != nil {
		t.Fatal(err)
	}
	compiled, err := ppl.Compile("source=web | where host in [ source=web | fields host ] | stats count() as n")
	if err != nil {
		t.Fatal(err)
	}
	sources, warnings := db.querySources(compiled.Plan, "")
	defer sources.release()
	if len(warnings) != 0 {
		t.Fatal(warnings)
	}
	if removed, err := db.DropBefore(testNow.AddDate(0, 0, 1)); err != nil || removed != 10 {
		t.Fatalf("removed %d events: %v", removed, err)
	}
	res, err := Run(context.Background(), compiled.Plan, sources, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if got := render(res.Maps()); got != "n=10" {
		t.Fatalf("pinned query saw %s", got)
	}
	if got := ask(t, db, "source=web | stats count() as n"); got != "n=0" {
		t.Fatalf("new query saw %s", got)
	}
}
