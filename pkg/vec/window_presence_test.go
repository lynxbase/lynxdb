package vec

import "testing"

func TestWindowOfPresence(t *testing.T) {
	const rows = 300
	c := Column{Kind: KindString, Layout: Flat, Rows: rows}
	valid := c.UseValid(rows)
	for i := 0; i < rows; i++ {
		if i%7 != 0 && (i < 100 || i >= 200) {
			valid.Set(i)
		}
	}
	var w Window
	for _, span := range [][2]int{{0, rows}, {3, 70}, {64, 128}, {99, 201}, {130, 190}, {201, 204}} {
		from, to := span[0], span[1]
		w.SetPresence(&c, from, to)
		if w.Rows != to-from {
			t.Fatalf("rows %d to %d: the window has %d rows", from, to, w.Rows)
		}
		for i := from; i < to; i++ {
			if got, want := w.IsValid(i-from), valid.Get(i); got != want {
				t.Fatalf("rows %d to %d: row %d holds a value: %v, want %v", from, to, i, got, want)
			}
		}
	}
	// Rows that all hold a value need no bitmap.
	if w.SetPresence(&c, 204, 210); w.Valid != nil {
		t.Fatalf("a window of rows that all hold a value has a bitmap")
	}
}
