package spl2

import (
	"fmt"
	"strings"
)

// FragmentResolver resolves fragment names to parsed command lists.
type FragmentResolver interface {
	Resolve(name string) ([]Command, error)
}

// ExpandFragments replaces UseCommand nodes in a Program with the commands
// from their resolved fragments. Cycle detection prevents infinite expansion.
func ExpandFragments(prog *Program, resolver FragmentResolver) error {
	if resolver == nil {
		return nil
	}

	visited := make(map[string]bool)

	expanded, err := expandCommands(prog.Main.Commands, resolver, visited)
	if err != nil {
		return err
	}
	prog.Main.Commands = expanded

	for i := range prog.Datasets {
		visited := make(map[string]bool)
		expanded, err := expandCommands(prog.Datasets[i].Query.Commands, resolver, visited)
		if err != nil {
			return fmt.Errorf("dataset $%s: %w", prog.Datasets[i].Name, err)
		}
		prog.Datasets[i].Query.Commands = expanded
	}

	return nil
}

// expandCommands recursively expands UseCommand nodes in a command list.
func expandCommands(commands []Command, resolver FragmentResolver, visited map[string]bool) ([]Command, error) {
	var result []Command
	for _, cmd := range commands {
		use, ok := cmd.(*UseCommand)
		if !ok {
			result = append(result, cmd)
			continue
		}

		name := use.Name
		if visited[name] {
			return nil, fmt.Errorf("spl2: circular fragment reference %q", name)
		}
		visited[name] = true

		fragment, err := resolver.Resolve(name)
		if err != nil {
			return nil, fmt.Errorf("spl2: use %q: %w", name, err)
		}

		// Recursively expand nested use commands.
		expanded, err := expandCommands(fragment, resolver, visited)
		if err != nil {
			return nil, err
		}

		result = append(result, expanded...)
	}

	return result, nil
}

// BuiltinFragments returns the built-in @stdlib/* fragment definitions.
func BuiltinFragments() map[string]string {
	return map[string]string{
		"@stdlib/parse_combined": "| unpack_combined",
		"@stdlib/parse_syslog":   "| unpack_syslog",
		"@stdlib/parse_nginx":    "| parse combined(_raw) | let duration_s = duration_ms / 1000",
		"@stdlib/parse_json":     "| unpack_json",
		"@stdlib/parse_logfmt":   "| unpack_logfmt",
		"@stdlib/errors_only":    `| where level="error" OR level="ERROR" OR severity="error"`,
		"@stdlib/last_hour":      "| where _time >= now() - 1h",
		"@stdlib/top_sources":    "| stats count by source | sort -count | head 20",
		"@stdlib/slow_requests":  "| where duration_ms > 1000 | sort -duration_ms | head 50",
	}
}

// isBuiltinFragment checks if a name refers to a built-in @stdlib fragment.
func isBuiltinFragment(name string) bool {
	return strings.HasPrefix(name, "@stdlib/")
}
