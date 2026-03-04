package unpack

import (
	"testing"

	"github.com/lynxbase/lynxdb/pkg/event"
)

func TestLEEFParser_V1Basic(t *testing.T) {
	input := "LEEF:1.0|IBM|QRadar|7.0|PortScan|src=10.0.0.1\tdst=192.168.1.1\tproto=TCP"

	p := &LEEFParser{}
	fields := make(map[string]event.Value)
	err := p.Parse(input, func(key string, val event.Value) bool {
		fields[key] = val
		return true
	})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	expect := map[string]string{
		"device_vendor":  "IBM",
		"device_product": "QRadar",
		"device_version": "7.0",
		"event_id":       "PortScan",
		"src":            "10.0.0.1",
		"dst":            "192.168.1.1",
		"proto":          "TCP",
	}
	for k, want := range expect {
		got := fields[k].String()
		if got != want {
			t.Errorf("%s: got %q, want %q", k, got, want)
		}
	}

	if fields["leef_version"].AsFloat() != 1.0 {
		t.Errorf("leef_version: got %v, want 1.0", fields["leef_version"])
	}
}

func TestLEEFParser_V2CustomDelim(t *testing.T) {
	input := "LEEF:2.0|Vendor|Product|1.0|Event|^|src=10.0.0.1^dst=192.168.1.1^dpt=80"

	p := &LEEFParser{}
	fields := make(map[string]event.Value)
	err := p.Parse(input, func(key string, val event.Value) bool {
		fields[key] = val
		return true
	})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	if fields["src"].String() != "10.0.0.1" {
		t.Errorf("src: got %q, want %q", fields["src"].String(), "10.0.0.1")
	}
	if fields["dst"].String() != "192.168.1.1" {
		t.Errorf("dst: got %q, want %q", fields["dst"].String(), "192.168.1.1")
	}
	if fields["dpt"].AsInt() != 80 {
		t.Errorf("dpt: got %v, want 80", fields["dpt"])
	}
}

func TestLEEFParser_V2HexDelim(t *testing.T) {
	input := "LEEF:2.0|V|P|1.0|E|0x09|src=10.0.0.1\tdst=192.168.1.1"

	p := &LEEFParser{}
	fields := make(map[string]event.Value)
	err := p.Parse(input, func(key string, val event.Value) bool {
		fields[key] = val
		return true
	})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	if fields["src"].String() != "10.0.0.1" {
		t.Errorf("src: got %q, want %q", fields["src"].String(), "10.0.0.1")
	}
}

func TestLEEFParser_NoExtension(t *testing.T) {
	input := "LEEF:1.0|V|P|1.0|Alert|"

	p := &LEEFParser{}
	fields := make(map[string]event.Value)
	err := p.Parse(input, func(key string, val event.Value) bool {
		fields[key] = val
		return true
	})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	if fields["event_id"].String() != "Alert" {
		t.Errorf("event_id: got %q, want %q", fields["event_id"].String(), "Alert")
	}
}

func TestLEEFParser_Empty(t *testing.T) {
	p := &LEEFParser{}
	err := p.Parse("", func(key string, val event.Value) bool {
		t.Fatalf("should not emit on empty input")
		return true
	})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
}

func TestLEEFParser_NotLEEF(t *testing.T) {
	p := &LEEFParser{}
	called := false
	err := p.Parse("not a LEEF line", func(key string, val event.Value) bool {
		called = true
		return true
	})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if called {
		t.Error("should not emit for non-LEEF input")
	}
}

func TestLEEFParser_Name(t *testing.T) {
	p := &LEEFParser{}
	if p.Name() != "leef" {
		t.Errorf("Name: got %q, want %q", p.Name(), "leef")
	}
}
