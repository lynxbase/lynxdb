package unpack

import (
	"testing"

	"github.com/lynxbase/lynxdb/pkg/event"
)

func TestCEFParser_Basic(t *testing.T) {
	input := `CEF:0|SecurityVendor|Firewall|1.0|100|Connection Refused|5|src=10.0.0.1 dst=192.168.1.1 dpt=80`

	p := &CEFParser{}
	fields := make(map[string]event.Value)
	err := p.Parse(input, func(key string, val event.Value) bool {
		fields[key] = val
		return true
	})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	expect := map[string]string{
		"device_vendor":  "SecurityVendor",
		"device_product": "Firewall",
		"device_version": "1.0",
		"signature_id":   "100",
		"name":           "Connection Refused",
		"src":            "10.0.0.1",
		"dst":            "192.168.1.1",
	}
	for k, want := range expect {
		got := fields[k].String()
		if got != want {
			t.Errorf("%s: got %q, want %q", k, got, want)
		}
	}

	if fields["cef_version"].AsInt() != 0 {
		t.Errorf("cef_version: got %v, want 0", fields["cef_version"])
	}
	if fields["severity"].AsInt() != 5 {
		t.Errorf("severity: got %v, want 5", fields["severity"])
	}
	if fields["dpt"].AsInt() != 80 {
		t.Errorf("dpt: got %v, want 80", fields["dpt"])
	}
}

func TestCEFParser_EscapedPipe(t *testing.T) {
	input := `CEF:0|Vendor\|Inc|Product|1.0|200|Test|3|msg=hello world`

	p := &CEFParser{}
	fields := make(map[string]event.Value)
	err := p.Parse(input, func(key string, val event.Value) bool {
		fields[key] = val
		return true
	})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	if fields["device_vendor"].String() != "Vendor|Inc" {
		t.Errorf("device_vendor: got %q, want %q", fields["device_vendor"].String(), "Vendor|Inc")
	}
}

func TestCEFParser_NoExtension(t *testing.T) {
	input := `CEF:0|V|P|1.0|300|Alert|8|`

	p := &CEFParser{}
	fields := make(map[string]event.Value)
	err := p.Parse(input, func(key string, val event.Value) bool {
		fields[key] = val
		return true
	})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	if fields["name"].String() != "Alert" {
		t.Errorf("name: got %q, want %q", fields["name"].String(), "Alert")
	}
}

func TestCEFParser_Empty(t *testing.T) {
	p := &CEFParser{}
	err := p.Parse("", func(key string, val event.Value) bool {
		t.Fatalf("should not emit on empty input")
		return true
	})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
}

func TestCEFParser_NotCEF(t *testing.T) {
	p := &CEFParser{}
	called := false
	err := p.Parse("not a CEF line", func(key string, val event.Value) bool {
		called = true
		return true
	})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if called {
		t.Error("should not emit for non-CEF input")
	}
}

func TestCEFParser_Name(t *testing.T) {
	p := &CEFParser{}
	if p.Name() != "cef" {
		t.Errorf("Name: got %q, want %q", p.Name(), "cef")
	}
}
