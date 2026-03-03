// Package ui provides a unified TUI theme and reusable rendering components
// for the LynxDB CLI. All styles are defined in one place; individual CLI
// commands import this package instead of using raw ANSI escape codes.
//
// The package exposes a single Env that is initialized once via InitEnv().
// Two backward-compatible Theme instances (Stdout, Stderr) are kept so that
// existing callers (components, JSON highlighter, confirm, table, etc.) work
// without modification. Under the hood they both delegate to the shared Env
// for color decisions.
//
// All lipgloss.Color() calls are centralized here — no other file in the
// codebase should construct colors directly.
package ui

import (
	"image/color"
	"io"
	"os"

	"charm.land/lipgloss/v2"
	"github.com/charmbracelet/colorprofile"
)

// Env — centralized terminal environment

// Env holds terminal detection results that are computed once and shared.
type Env struct {
	// Profile is the resolved color profile (TrueColor, ANSI256, ANSI, Ascii, NoTTY).
	Profile colorprofile.Profile
	// HasDarkBG is true when the terminal background is dark (or detection failed).
	HasDarkBG bool
	// lightDark picks between two colors based on background brightness.
	lightDark func(light, dark color.Color) color.Color
}

// Term is the package-level Env, initialized by InitEnv.
var Term Env

// InitEnv detects the terminal environment and initializes the package-level
// Env and backward-compatible Theme instances.
//
// Pass noColor=true to force Ascii profile (--no-color flag).
func InitEnv(noColor bool) {
	profile := colorprofile.Detect(os.Stdout, os.Environ())

	// Respect --no-color and NO_COLOR / TERM=dumb.
	if noColor || os.Getenv("NO_COLOR") != "" || os.Getenv("TERM") == "dumb" {
		profile = colorprofile.Ascii
	}

	// CLICOLOR_FORCE=1: force at least ANSI colors even when not a TTY,
	// unless --no-color was explicitly set.
	if !noColor && os.Getenv("CLICOLOR_FORCE") == "1" && profile < colorprofile.ANSI {
		profile = colorprofile.ANSI
	}

	hasDark := lipgloss.HasDarkBackground(os.Stdin, os.Stdout)

	Term = Env{
		Profile:   profile,
		HasDarkBG: hasDark,
		lightDark: lipgloss.LightDark(hasDark),
	}

	// Build backward-compatible Theme instances.
	Stdout = newThemeFromEnv(os.Stdout)
	Stderr = newThemeFromEnv(os.Stderr)
}

// Init is the legacy entry point. It delegates to InitEnv.
func Init(noColor bool) { InitEnv(noColor) }

// Adaptive color palette — all lipgloss.Color() calls live here

// ColorSuccess returns the semantic "success / ok" green.
func ColorSuccess() color.Color {
	return Term.lightDark(lipgloss.Color("#16a34a"), lipgloss.Color("#4ade80"))
}

// ColorWarning returns the semantic "warning" yellow/orange.
func ColorWarning() color.Color {
	return Term.lightDark(lipgloss.Color("#d97706"), lipgloss.Color("#fbbf24"))
}

// ColorError returns the semantic "error" red.
func ColorError() color.Color {
	return Term.lightDark(lipgloss.Color("#dc2626"), lipgloss.Color("#f87171"))
}

// ColorInfo returns the semantic "info / hint" cyan.
func ColorInfo() color.Color {
	return Term.lightDark(lipgloss.Color("#0284c7"), lipgloss.Color("#38bdf8"))
}

// ColorAccent returns the accent pink.
func ColorAccent() color.Color {
	return Term.lightDark(lipgloss.Color("#db2777"), lipgloss.Color("#f472b6"))
}

// ColorGray returns a medium gray for labels.
func ColorGray() color.Color {
	return Term.lightDark(lipgloss.Color("#6b7280"), lipgloss.Color("#9ca3af"))
}

// ColorWhite returns a bright foreground for values.
func ColorWhite() color.Color {
	return Term.lightDark(lipgloss.Color("#111827"), lipgloss.Color("#f9fafb"))
}

// ColorDim returns a muted foreground.
func ColorDim() color.Color {
	return Term.lightDark(lipgloss.Color("#9ca3af"), lipgloss.Color("#6b7280"))
}

// ColorDark returns a dark foreground for rules/separators.
func ColorDark() color.Color {
	return Term.lightDark(lipgloss.Color("#d1d5db"), lipgloss.Color("#4b5563"))
}

// ColorJSONStr returns green for JSON string values.
func ColorJSONStr() color.Color {
	return Term.lightDark(lipgloss.Color("#15803d"), lipgloss.Color("#86efac"))
}

// ColorJSONNum returns yellow for JSON number values.
func ColorJSONNum() color.Color {
	return Term.lightDark(lipgloss.Color("#b45309"), lipgloss.Color("#fde68a"))
}

// ColorDuration returns a duration-conditional color.
// < 1s → green, < 10s → yellow, >= 10s → red.
func ColorDuration(ms float64) color.Color {
	switch {
	case ms < 1000:
		return ColorSuccess()
	case ms < 10000:
		return ColorWarning()
	default:
		return ColorError()
	}
}

// Theme — backward-compatible struct used by components, table, JSON, confirm

// Theme holds all lipgloss styles for consistent CLI rendering.
// A Theme is bound to a specific io.Writer. Colors are sourced from the
// centralized Env so they adapt to light/dark backgrounds automatically.
type Theme struct {
	w io.Writer

	// Text styles.
	Bold  lipgloss.Style
	Dim   lipgloss.Style
	Faint lipgloss.Style

	// Semantic colors.
	Success lipgloss.Style
	Warning lipgloss.Style
	Error   lipgloss.Style
	Info    lipgloss.Style
	Accent  lipgloss.Style

	// UI structure.
	Label lipgloss.Style
	Value lipgloss.Style
	Rule  lipgloss.Style
	Muted lipgloss.Style

	// JSON syntax highlighting.
	JSONKey   lipgloss.Style
	JSONStr   lipgloss.Style
	JSONNum   lipgloss.Style
	JSONBool  lipgloss.Style
	JSONNull  lipgloss.Style
	JSONBrace lipgloss.Style
	JSONPunct lipgloss.Style
	JSONIdx   lipgloss.Style

	// Status icons (pre-rendered strings are returned by Icon* methods).
	StatusOK      lipgloss.Style
	StatusWarn    lipgloss.Style
	StatusErr     lipgloss.Style
	StatusRunning lipgloss.Style

	// Log levels (tail).
	LevelError lipgloss.Style
	LevelWarn  lipgloss.Style
	LevelInfo  lipgloss.Style
	LevelDebug lipgloss.Style

	// Table.
	TableHeader lipgloss.Style
	TableRule   lipgloss.Style

	// Diff.
	DiffUp   lipgloss.Style
	DiffDown lipgloss.Style
	DiffNew  lipgloss.Style
}

// Writer returns the io.Writer this theme renders to.
func (t *Theme) Writer() io.Writer { return t.w }

// Package-level theme instances, initialized by InitEnv().
var (
	// Stdout is the theme for data output (query results, tables, JSON).
	Stdout *Theme
	// Stderr is the theme for status messages (hints, warnings, errors).
	Stderr *Theme
)

// newThemeFromEnv builds a Theme whose styles use the centralized Env colors.
func newThemeFromEnv(w io.Writer) *Theme {
	t := &Theme{w: w}

	// Text styles.
	t.Bold = lipgloss.NewStyle().Bold(true)
	t.Dim = lipgloss.NewStyle().Foreground(ColorDim())
	t.Faint = lipgloss.NewStyle().Faint(true)

	// Semantic colors.
	t.Success = lipgloss.NewStyle().Foreground(ColorSuccess())
	t.Warning = lipgloss.NewStyle().Foreground(ColorWarning())
	t.Error = lipgloss.NewStyle().Foreground(ColorError())
	t.Info = lipgloss.NewStyle().Foreground(ColorInfo())
	t.Accent = lipgloss.NewStyle().Foreground(ColorAccent())

	// UI structure.
	t.Label = lipgloss.NewStyle().Foreground(ColorGray())
	t.Value = lipgloss.NewStyle().Foreground(ColorWhite()).Bold(true)
	t.Rule = lipgloss.NewStyle().Foreground(ColorDark())
	t.Muted = lipgloss.NewStyle().Foreground(ColorDim())

	// JSON syntax.
	t.JSONKey = lipgloss.NewStyle().Foreground(ColorInfo())
	t.JSONStr = lipgloss.NewStyle().Foreground(ColorJSONStr())
	t.JSONNum = lipgloss.NewStyle().Foreground(ColorJSONNum())
	t.JSONBool = lipgloss.NewStyle().Foreground(ColorWarning())
	t.JSONNull = lipgloss.NewStyle().Foreground(ColorDim()).Faint(true)
	t.JSONBrace = lipgloss.NewStyle().Foreground(ColorWhite())
	t.JSONPunct = lipgloss.NewStyle().Foreground(ColorGray())
	t.JSONIdx = lipgloss.NewStyle().Foreground(ColorDim()).Faint(true)

	// Status icons.
	t.StatusOK = lipgloss.NewStyle().Foreground(ColorSuccess()).Bold(true)
	t.StatusWarn = lipgloss.NewStyle().Foreground(ColorWarning()).Bold(true)
	t.StatusErr = lipgloss.NewStyle().Foreground(ColorError()).Bold(true)
	t.StatusRunning = lipgloss.NewStyle().Foreground(ColorAccent())

	// Log levels.
	t.LevelError = lipgloss.NewStyle().Foreground(ColorError())
	t.LevelWarn = lipgloss.NewStyle().Foreground(ColorWarning())
	t.LevelInfo = lipgloss.NewStyle().Foreground(ColorSuccess())
	t.LevelDebug = lipgloss.NewStyle().Foreground(ColorDim())

	// Table.
	t.TableHeader = lipgloss.NewStyle().Bold(true)
	t.TableRule = lipgloss.NewStyle().Foreground(ColorDark())

	// Diff.
	t.DiffUp = lipgloss.NewStyle().Foreground(ColorError())
	t.DiffDown = lipgloss.NewStyle().Foreground(ColorSuccess())
	t.DiffNew = lipgloss.NewStyle().Foreground(ColorSuccess())

	return t
}

// NewTheme creates a Theme bound to w. When noColor is true, all styles
// produce plain text. This is the backward-compatible constructor used by
// tests and the output formatter.
func NewTheme(w io.Writer, noColor bool) *Theme {
	// When called from tests or format detection, we need a minimal Env.
	// If InitEnv hasn't run yet, build a temporary one.
	if Term.lightDark == nil {
		// Fallback: assume dark bg, detect profile.
		p := colorprofile.Detect(os.Stdout, os.Environ())
		if noColor || os.Getenv("NO_COLOR") != "" || os.Getenv("TERM") == "dumb" {
			p = colorprofile.Ascii
		}
		Term = Env{
			Profile:   p,
			HasDarkBG: true,
			lightDark: lipgloss.LightDark(true),
		}
	}

	// If noColor is requested, override with a stripped-down Env.
	if noColor {
		saved := Term
		Term = Env{
			Profile:   colorprofile.Ascii,
			HasDarkBG: saved.HasDarkBG,
			lightDark: func(_, dark color.Color) color.Color { return dark },
		}
		t := newThemeFromEnv(w)
		// Force all styles to produce no ANSI by stripping foreground/background.
		// Under Ascii profile, lipgloss styles with Color values still emit
		// plain text because the Ascii profile ignores colors. But we ensure
		// the profile is set on the writer via colorprofile.
		*t = *newNoColorTheme(w)
		Term = saved

		return t
	}

	return newThemeFromEnv(w)
}

// newNoColorTheme builds a theme where every style is a plain no-op.
func newNoColorTheme(w io.Writer) *Theme {
	s := lipgloss.NewStyle() // plain style — no colors, no bold, no ANSI

	return &Theme{
		w:             w,
		Bold:          s,
		Dim:           s,
		Faint:         s,
		Success:       s,
		Warning:       s,
		Error:         s,
		Info:          s,
		Accent:        s,
		Label:         s,
		Value:         s,
		Rule:          s,
		Muted:         s,
		JSONKey:       s,
		JSONStr:       s,
		JSONNum:       s,
		JSONBool:      s,
		JSONNull:      s,
		JSONBrace:     s,
		JSONPunct:     s,
		JSONIdx:       s,
		StatusOK:      s,
		StatusWarn:    s,
		StatusErr:     s,
		StatusRunning: s,
		LevelError:    s,
		LevelWarn:     s,
		LevelInfo:     s,
		LevelDebug:    s,
		TableHeader:   s,
		TableRule:     s,
		DiffUp:        s,
		DiffDown:      s,
		DiffNew:       s,
	}
}

// Style constructors — semantic helpers for callers outside this package

// StyleTableHeader returns the style for table column headers.
func StyleTableHeader() lipgloss.Style { return lipgloss.NewStyle().Bold(true) }

// StyleTableSeparator returns the style for table separator rules.
func StyleTableSeparator() lipgloss.Style { return lipgloss.NewStyle().Foreground(ColorDark()) }

// Separator constant

// Separator is the short 3-char rule printed before the stats block.
const Separator = "\u2500\u2500\u2500"

// HRuleSep returns a styled short separator for stats output.
func HRuleSep() string {
	return lipgloss.NewStyle().Foreground(ColorDark()).Render(Separator)
}
