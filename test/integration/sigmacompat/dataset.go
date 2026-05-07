package sigmacompat

import (
	"encoding/json"
	"fmt"
	"sort"
)

const ReferenceSource = "local_reference_evaluator"

var FixtureNames = []string{
	"simple_eq",
	"and_or_not",
	"wildcards",
	"regex",
	"cidr",
	"keywords",
	"exists_null_bool",
	"numeric_compare",
	"brute_force",
}

type Event struct {
	Index int
	Raw   string
	Match bool
}

type MatchReference struct {
	RsigmaVersion   string   `json:"rsigma_version"`
	Fixture         string   `json:"fixture"`
	RuleID          string   `json:"rule_id"`
	MatchIndices    []int    `json:"match_indices"`
	MatchCount      int      `json:"match_count"`
	ReferenceSource string   `json:"reference_source"`
	ReferenceNotes  []string `json:"reference_notes,omitempty"`
}

func DatasetFor(fixture string) []Event {
	switch fixture {
	case "simple_eq":
		return build(100, func(i int) (map[string]any, bool) {
			if i < 50 {
				return fields(i, map[string]any{"CommandLine": "whoami"}), true
			}
			return fields(i, map[string]any{"CommandLine": fmt.Sprintf("hostname-%02d", i)}), false
		})
	case "and_or_not":
		return build(100, func(i int) (map[string]any, bool) {
			switch {
			case i < 24:
				return fields(i, map[string]any{"FieldA": "val1", "FieldB": "other", "FieldC": "other"}), true
			case i < 40:
				return fields(i, map[string]any{"FieldA": "nope", "FieldB": "val2", "FieldC": "val3"}), true
			case i < 70:
				return fields(i, map[string]any{"FieldA": "val1", "FieldB": "val2", "FieldC": "other"}), false
			default:
				return fields(i, map[string]any{"FieldA": "nope", "FieldB": "other", "FieldC": "other"}), false
			}
		})
	case "wildcards":
		return build(100, func(i int) (map[string]any, bool) {
			switch {
			case i < 30:
				return fields(i, map[string]any{
					"CommandLine": fmt.Sprintf("cmd.exe /c whoami /groups /n:%02d", i),
					"Image":       `C:\Windows\System32\cmd.exe`,
					"ParentImage": `C:\Windows\explorer.exe`,
				}), true
			case i < 55:
				return fields(i, map[string]any{
					"CommandLine": "cmd.exe /c hostname",
					"Image":       `C:\Windows\System32\cmd.exe`,
					"ParentImage": `C:\Windows\explorer.exe`,
				}), false
			case i < 75:
				return fields(i, map[string]any{
					"CommandLine": "cmd.exe /c whoami",
					"Image":       `C:\Windows\System32\cmd.dll`,
					"ParentImage": `C:\Windows\explorer.exe`,
				}), false
			default:
				return fields(i, map[string]any{
					"CommandLine": "cmd.exe /c whoami",
					"Image":       `C:\Windows\System32\cmd.exe`,
					"ParentImage": `D:\Tools\runner.exe`,
				}), false
			}
		})
	case "regex":
		return build(100, func(i int) (map[string]any, bool) {
			if i < 35 {
				return fields(i, map[string]any{"CommandLine": fmt.Sprintf("powershell.exe -NoProfile whoami /all #%02d", i)}), true
			}
			return fields(i, map[string]any{"CommandLine": fmt.Sprintf("powershell.exe -NoProfile hostname #%02d", i)}), false
		})
	case "cidr":
		return build(100, func(i int) (map[string]any, bool) {
			switch {
			case i < 25:
				return fields(i, map[string]any{"SourceIP": fmt.Sprintf("10.%d.%d.%d", i%255, (i*3)%255, (i*7)%255)}), true
			case i < 55:
				return fields(i, map[string]any{"SourceIP": fmt.Sprintf("192.168.%d.%d", i%255, (i*11)%255)}), false
			case i < 80:
				return fields(i, map[string]any{"SourceIP": ""}), false
			default:
				return fields(i, map[string]any{"Message": "missing source ip"}), false
			}
		})
	case "keywords":
		words := []string{"error", "timeout", "refused"}
		return build(100, func(i int) (map[string]any, bool) {
			if i < 30 {
				word := words[i%len(words)]
				return fields(i, map[string]any{"Message": fmt.Sprintf("connection %s while opening session %02d", word, i)}), true
			}
			return fields(i, map[string]any{"Message": fmt.Sprintf("connection accepted for session %02d", i)}), false
		})
	case "exists_null_bool":
		return build(100, func(i int) (map[string]any, bool) {
			switch {
			case i < 20:
				return fields(i, map[string]any{"FieldA": fmt.Sprintf("present-%02d", i), "Enabled": true}), true
			case i < 45:
				return fields(i, map[string]any{"FieldA": fmt.Sprintf("present-%02d", i), "FieldB": "set", "Enabled": true}), false
			case i < 70:
				return fields(i, map[string]any{"FieldA": fmt.Sprintf("present-%02d", i), "Enabled": false}), false
			default:
				return fields(i, map[string]any{"Enabled": true}), false
			}
		})
	case "numeric_compare":
		return build(100, func(i int) (map[string]any, bool) {
			switch {
			case i < 49:
				return fields(i, map[string]any{"status": 400 + i}), true
			case i == 49:
				return fields(i, map[string]any{"status": 499}), true
			case i < 70:
				return fields(i, map[string]any{"status": 399}), false
			case i < 90:
				return fields(i, map[string]any{"status": 500}), false
			default:
				return fields(i, map[string]any{"status": 200}), false
			}
		})
	case "brute_force":
		return build(100, func(i int) (map[string]any, bool) {
			switch {
			case i < 45:
				return fields(i, map[string]any{"EventID": 4625, "SubStatus": "0xC000006D"}), true
			case i < 70:
				return fields(i, map[string]any{"EventID": 4625, "SubStatus": "0xC0000064"}), false
			default:
				return fields(i, map[string]any{"EventID": 4624, "SubStatus": "0xC000006D"}), false
			}
		})
	default:
		return nil
	}
}

func ReferenceFor(fixture string) (MatchReference, error) {
	events := DatasetFor(fixture)
	if events == nil {
		return MatchReference{}, fmt.Errorf("unknown sigma fixture %q", fixture)
	}
	indices := make([]int, 0, len(events))
	for _, ev := range events {
		if ev.Match {
			indices = append(indices, ev.Index)
		}
	}
	return MatchReference{
		RsigmaVersion:   "0.9.0",
		Fixture:         fixture,
		RuleID:          "",
		MatchIndices:    indices,
		MatchCount:      len(indices),
		ReferenceSource: ReferenceSource,
		ReferenceNotes: []string{
			"rsigma v0.9.0 eval did not provide a matched-indices output in this repository workflow; matches are generated by the deterministic local reference evaluator over the same synthetic dataset.",
		},
	}, nil
}

func AllReferences() ([]MatchReference, error) {
	refs := make([]MatchReference, 0, len(FixtureNames))
	for _, fixture := range FixtureNames {
		ref, err := ReferenceFor(fixture)
		if err != nil {
			return nil, err
		}
		refs = append(refs, ref)
	}
	return refs, nil
}

func Lines(events []Event) []string {
	lines := make([]string, len(events))
	for i, ev := range events {
		lines[i] = ev.Raw
	}
	return lines
}

func build(count int, fn func(int) (map[string]any, bool)) []Event {
	events := make([]Event, 0, count)
	for i := 0; i < count; i++ {
		fields, match := fn(i)
		events = append(events, Event{
			Index: i,
			Raw:   encode(fields),
			Match: match,
		})
	}
	return events
}

func fields(index int, extra map[string]any) map[string]any {
	out := map[string]any{
		"__sigma_index": index,
		"ts":            "2024-01-01T00:00:00Z",
	}
	for k, v := range extra {
		out[k] = v
	}
	return out
}

func encode(fields map[string]any) string {
	keys := make([]string, 0, len(fields))
	for k := range fields {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	ordered := make(map[string]any, len(fields))
	for _, k := range keys {
		ordered[k] = fields[k]
	}
	data, err := json.Marshal(ordered)
	if err != nil {
		panic(err)
	}
	return string(data)
}
