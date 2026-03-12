package spl2

import "strings"

// CompatHint describes a Splunk-to-LynxDB compatibility suggestion.
type CompatHint struct {
	Pattern     string // the Splunk pattern detected
	Suggestion  string // the LynxDB equivalent or info message
	Unsupported bool   // true if the feature is not supported
}

// unsupportedCommands maps Splunk-only commands to their hint messages.
var unsupportedCommands = map[string]string{
	// Note: "lookup" is NOT listed here — it is a valid Lynx Flow command (desugars to left join).
	"inputlookup":  "inputlookup is not yet supported. Use FROM $dataset with CTE syntax.",
	"outputlookup": "outputlookup is not yet supported.",
	"collect":      "collect is not yet supported.",
	"sendemail":    "sendemail is not yet supported.",
	"map":          "map is not yet supported.",
	"tstats":       "tstats is not yet supported. Use stats with time-filtered queries.",
	"datamodel":    "datamodel is not yet supported.",
	"makemv":       "makemv is not yet supported. Use mvappend() eval function instead.",
	"addinfo":      "addinfo is not yet supported.",
	"return":       "return is not yet supported. Use head or fields to limit output.",
	"format":       "format is not yet supported.",
	// Note: "bucket" is NOT listed here — it is a valid Lynx Flow command (desugars to bin).
	"sistats":      "sistats is not yet supported. Use stats instead.",
}

// translationHints maps Splunk patterns to LynxDB suggestions.
var translationHints = map[string]string{
	"chart":                   "chart is similar to timechart or stats in LynxDB. Try: | timechart or | stats ... BY field",
	"access_combined":         "sourcetype=access_combined is a Splunk default. In LynxDB, use sourcetype=nginx or sourcetype=clf",
	"access_combined_wcookie": "sourcetype=access_combined_wcookie → use sourcetype=nginx in LynxDB",
	"syslog":                  "sourcetype=syslog works in LynxDB if you have syslog-formatted data ingested",
}

// DetectCompatHints scans a query string for common Splunk-only patterns
// and returns hints about LynxDB equivalents.
func DetectCompatHints(query string) []CompatHint {
	var hints []CompatHint
	lower := strings.ToLower(query)
	seen := make(map[string]bool)

	for cmd, msg := range unsupportedCommands {
		pattern := "| " + cmd
		if strings.Contains(lower, pattern) || strings.Contains(lower, "|"+cmd) {
			if !seen[cmd] {
				seen[cmd] = true
				hints = append(hints, CompatHint{
					Pattern:     cmd,
					Suggestion:  msg,
					Unsupported: true,
				})
			}
		}
	}

	// Check for "| chart" (without "timechart").
	if strings.Contains(lower, "| chart") && !strings.Contains(lower, "| timechart") {
		if !seen["chart"] {
			seen["chart"] = true
			hints = append(hints, CompatHint{
				Pattern:    "chart",
				Suggestion: translationHints["chart"],
			})
		}
	}

	// Note: "| bucket" is a valid Lynx Flow command (desugars to bin).
	// No compat hint needed — both "bucket" and "bin" are supported.

	// Check for Splunk inline time modifiers (earliest=/latest=).
	if strings.Contains(lower, "earliest=") || strings.Contains(lower, "latest=") {
		if !seen["earliest/latest"] {
			seen["earliest/latest"] = true
			hints = append(hints, CompatHint{
				Pattern:    "earliest=/latest=",
				Suggestion: "Inline time modifiers are not supported. Use CLI flags: --since 1h, or --from/--to.",
			})
		}
	}

	// Check for index= usage — explain that it maps to _source in LynxDB.
	if strings.Contains(lower, "index=") && !seen["index"] {
		seen["index"] = true
		hints = append(hints, CompatHint{
			Pattern:    "index=",
			Suggestion: "In LynxDB, 'index=' maps to the '_source' field. Both 'index=main' and '_source=main' work identically. Use FROM for multi-source: FROM a, b, c.",
		})
	}

	// Check for index=_internal or index=_audit — LynxDB has no internal indexes.
	for _, internal := range []string{"_internal", "_audit", "_introspection"} {
		if strings.Contains(lower, "index="+internal) || strings.Contains(lower, "index=\""+internal+"\"") {
			key := "index=" + internal
			if !seen[key] {
				seen[key] = true
				hints = append(hints, CompatHint{
					Pattern:    key,
					Suggestion: "LynxDB does not have Splunk internal indexes (" + internal + "). All data lives in one storage engine with _source as the logical partition.",
				})
			}
		}
	}

	// Check for Splunk-specific sourcetype values.
	for _, st := range []string{"access_combined_wcookie", "access_combined"} {
		if strings.Contains(lower, "sourcetype="+st) || strings.Contains(lower, "sourcetype=\""+st+"\"") {
			if !seen[st] {
				seen[st] = true
				if msg, ok := translationHints[st]; ok {
					hints = append(hints, CompatHint{
						Pattern:    "sourcetype=" + st,
						Suggestion: msg,
					})
				}
			}
		}
	}

	return hints
}

// DetectScopeHint returns a hint when the query has no explicit source/index
// scope and there are many sources available. This helps users coming from
// Splunk (where default index is "main") understand that LynxDB searches
// all sources by default.
func DetectScopeHint(query string, sourceCount int) *CompatHint {
	if sourceCount <= 5 {
		return nil
	}

	lower := strings.ToLower(query)

	// Skip if query already has a scope selector.
	if strings.HasPrefix(lower, "from ") ||
		strings.Contains(lower, "source=") ||
		strings.Contains(lower, "index=") ||
		strings.Contains(lower, "source ") ||
		strings.Contains(lower, "index ") {
		return nil
	}

	// Only suggest for simple searches (no pipes = keyword search).
	if strings.Contains(query, "|") {
		return nil
	}

	return &CompatHint{
		Pattern:    "implicit_all_sources",
		Suggestion: "LynxDB searches all sources by default. Use '_source=nginx' or 'index=nginx' to narrow scope.",
	}
}

// lynxFlowHints maps SPL2 command types to Lynx Flow alternatives.
// Both syntaxes are first-class citizens — these are advisory suggestions only.
var lynxFlowHints = map[string]string{
	"eval":           "Lynx Flow: | let <field> = <expr>",
	"stats":          "Lynx Flow: | group by <fields> compute <aggs>",
	"fields":         "Lynx Flow: | keep <fields> (include) or | omit <fields> (exclude)",
	"fields -":       "Lynx Flow: | omit <fields>",
	"streamstats":    "Lynx Flow: | running [window=N] <aggs> [by <fields>]",
	"eventstats":     "Lynx Flow: | enrich <aggs> [by <fields>]",
	"rex":            "Lynx Flow: | parse regex(<field>, \"<pattern>\")",
	"unpack_json":    "Lynx Flow: | parse json(<field>) [as <ns>] [extract (<fields>)]",
	"unpack_logfmt":  "Lynx Flow: | parse logfmt(<field>)",
	"unpack_syslog":  "Lynx Flow: | parse syslog(<field>)",
	"head":           "Lynx Flow: | take <N>",
	"bin":            "Lynx Flow: | bucket <field> span=<dur> [as <alias>]",
	"timechart":      "Lynx Flow: | every <span> [by <field>] compute <aggs>",
	"sort":           "Lynx Flow: | order by <field> [asc|desc]",
	"unroll":         "Lynx Flow: | explode <field> [as <alias>]",
	"pack_json":      "Lynx Flow: | pack <fields> into <target>",
}

// DetectLynxFlowHints examines a parsed query's commands and returns advisory
// hints suggesting Lynx Flow equivalents for SPL2 commands. Both syntaxes are
// first-class citizens — these hints are informational only.
func DetectLynxFlowHints(cmds []Command) []CompatHint {
	var hints []CompatHint
	seen := make(map[string]bool)

	for _, cmd := range cmds {
		var key string
		switch c := cmd.(type) {
		case *EvalCommand:
			key = "eval"
		case *StatsCommand:
			key = "stats"
		case *FieldsCommand:
			if c.Remove {
				key = "fields -"
			} else {
				key = "fields"
			}
		case *StreamstatsCommand:
			key = "streamstats"
		case *EventstatsCommand:
			key = "eventstats"
		case *RexCommand:
			key = "rex"
		case *UnpackCommand:
			switch c.Format {
			case "json":
				key = "unpack_json"
			case "logfmt":
				key = "unpack_logfmt"
			case "syslog":
				key = "unpack_syslog"
			default:
				// Other formats have no specific Lynx Flow mapping hint.
			}
		case *HeadCommand:
			key = "head"
		case *BinCommand:
			key = "bin"
		case *TimechartCommand:
			key = "timechart"
		case *SortCommand:
			key = "sort"
		case *UnrollCommand:
			key = "unroll"
		case *PackJsonCommand:
			key = "pack_json"
		}

		if key != "" && !seen[key] {
			if suggestion, ok := lynxFlowHints[key]; ok {
				seen[key] = true
				hints = append(hints, CompatHint{
					Pattern:    key,
					Suggestion: suggestion,
				})
			}
		}
	}

	return hints
}

// FormatCompatHints returns a formatted multi-line string of compatibility hints.
func FormatCompatHints(hints []CompatHint) string {
	if len(hints) == 0 {
		return ""
	}
	var b strings.Builder
	for _, h := range hints {
		if h.Unsupported {
			b.WriteString("Warning: ")
		} else {
			b.WriteString("Info: ")
		}
		b.WriteString(h.Suggestion)
		b.WriteByte('\n')
	}

	return b.String()
}
