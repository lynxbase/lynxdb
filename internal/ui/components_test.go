package ui

import (
	"bytes"
	"strings"
	"testing"
)

func TestPrintSuccess(t *testing.T) {
	var buf bytes.Buffer
	theme := NewTheme(&buf, true)

	theme.PrintSuccess(false, "done %d", 42)
	got := buf.String()

	if !strings.Contains(got, "done 42") {
		t.Errorf("PrintSuccess output should contain 'done 42', got: %q", got)
	}

	if !strings.Contains(got, "\u2714") {
		t.Errorf("PrintSuccess output should contain check mark, got: %q", got)
	}
}

func TestPrintSuccess_Quiet(t *testing.T) {
	var buf bytes.Buffer
	theme := NewTheme(&buf, true)

	theme.PrintSuccess(true, "should not appear")

	if buf.Len() != 0 {
		t.Errorf("PrintSuccess with quiet=true should produce no output, got: %q", buf.String())
	}
}

func TestPrintWarning(t *testing.T) {
	var buf bytes.Buffer
	theme := NewTheme(&buf, true)

	theme.PrintWarning(false, "caution: %s", "hot")
	got := buf.String()

	if !strings.Contains(got, "caution: hot") {
		t.Errorf("PrintWarning output should contain message, got: %q", got)
	}
}

func TestPrintNextSteps(t *testing.T) {
	var buf bytes.Buffer
	theme := NewTheme(&buf, true)

	theme.PrintNextSteps(false, "step one", "step two")
	got := buf.String()

	if !strings.Contains(got, "Next steps:") {
		t.Errorf("PrintNextSteps should contain 'Next steps:', got: %q", got)
	}

	if !strings.Contains(got, "step one") {
		t.Errorf("PrintNextSteps should contain 'step one', got: %q", got)
	}
}

func TestPrintNextSteps_Quiet(t *testing.T) {
	var buf bytes.Buffer
	theme := NewTheme(&buf, true)

	theme.PrintNextSteps(true, "should not appear")

	if buf.Len() != 0 {
		t.Errorf("PrintNextSteps with quiet=true should produce no output, got: %q", buf.String())
	}
}

func TestIconOK(t *testing.T) {
	var buf bytes.Buffer
	theme := NewTheme(&buf, true)

	got := theme.IconOK()
	if got != "\u2714" {
		t.Errorf("IconOK with noColor should return plain check mark, got: %q", got)
	}
}

func TestIconError(t *testing.T) {
	var buf bytes.Buffer
	theme := NewTheme(&buf, true)

	got := theme.IconError()
	if got != "\u2716" {
		t.Errorf("IconError with noColor should return plain cross mark, got: %q", got)
	}
}

func TestHRule(t *testing.T) {
	var buf bytes.Buffer
	theme := NewTheme(&buf, true)

	got := theme.HRule(10)
	if got != strings.Repeat("\u2500", 10) {
		t.Errorf("HRule(10) with noColor = %q, want 10 dashes", got)
	}
}

func TestHRule_DefaultWidth(t *testing.T) {
	var buf bytes.Buffer
	theme := NewTheme(&buf, true)

	got := theme.HRule(0)
	if len([]rune(got)) != 50 {
		t.Errorf("HRule(0) should default to 50 chars, got %d", len([]rune(got)))
	}
}

func TestRenderError(t *testing.T) {
	var buf bytes.Buffer
	theme := NewTheme(&buf, true)

	theme.RenderError(nil) // should be a no-op
	if buf.Len() != 0 {
		t.Errorf("RenderError(nil) should produce no output, got: %q", buf.String())
	}
}

func TestRenderConnectionError(t *testing.T) {
	var buf bytes.Buffer
	theme := NewTheme(&buf, true)

	theme.RenderConnectionError("http://localhost:3100")
	got := buf.String()

	if !strings.Contains(got, "Cannot connect to http://localhost:3100") {
		t.Errorf("RenderConnectionError should contain server address, got: %q", got)
	}

	if !strings.Contains(got, "lynxdb server") {
		t.Errorf("RenderConnectionError should contain suggestion, got: %q", got)
	}
}

func TestRenderRequiredFlagError_SingleFlag(t *testing.T) {
	var buf bytes.Buffer
	theme := NewTheme(&buf, true)

	theme.RenderRequiredFlagError([]string{"name"}, "lynxdb auth create [flags]", "  lynxdb auth create --name web-01")
	got := buf.String()

	if !strings.Contains(got, "missing required flag: --name") {
		t.Errorf("should contain 'missing required flag: --name', got: %q", got)
	}

	if !strings.Contains(got, "Usage:") {
		t.Errorf("should contain 'Usage:', got: %q", got)
	}

	if !strings.Contains(got, "lynxdb auth create [flags]") {
		t.Errorf("should contain usage line, got: %q", got)
	}

	if !strings.Contains(got, "Examples:") {
		t.Errorf("should contain 'Examples:', got: %q", got)
	}
}

func TestRenderRequiredFlagError_MultipleFlags(t *testing.T) {
	var buf bytes.Buffer
	theme := NewTheme(&buf, true)

	theme.RenderRequiredFlagError([]string{"name", "query"}, "lynxdb mv create [flags]", "")
	got := buf.String()

	if !strings.Contains(got, "missing required flags: --name, --query") {
		t.Errorf("should contain 'missing required flags: --name, --query', got: %q", got)
	}

	// No example provided, should not contain Examples header.
	if strings.Contains(got, "Examples:") {
		t.Errorf("should not contain 'Examples:' when example is empty, got: %q", got)
	}
}

func TestRenderRequiredFlagError_NoUsageOrExample(t *testing.T) {
	var buf bytes.Buffer
	theme := NewTheme(&buf, true)

	theme.RenderRequiredFlagError([]string{"url"}, "", "")
	got := buf.String()

	if !strings.Contains(got, "missing required flag: --url") {
		t.Errorf("should contain 'missing required flag: --url', got: %q", got)
	}

	if strings.Contains(got, "Usage:") {
		t.Errorf("should not contain 'Usage:' when usage line is empty, got: %q", got)
	}

	if strings.Contains(got, "Examples:") {
		t.Errorf("should not contain 'Examples:' when example is empty, got: %q", got)
	}
}

func TestRenderQueryError(t *testing.T) {
	var buf bytes.Buffer
	theme := NewTheme(&buf, true)

	theme.RenderQueryError("| stats count by", 17, 2, "unexpected end", "add a field name")
	got := buf.String()

	if !strings.Contains(got, "INVALID_QUERY") {
		t.Errorf("RenderQueryError should contain 'INVALID_QUERY', got: %q", got)
	}

	if !strings.Contains(got, "Did you mean:") {
		t.Errorf("RenderQueryError should contain suggestion, got: %q", got)
	}
}
