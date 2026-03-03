package planner

import (
	"fmt"
	"strings"

	"github.com/OrlovEvgeny/Lynxdb/pkg/spl2"
)

// TailValidationError is returned when a query contains commands not supported
// for live tail streaming.
type TailValidationError struct {
	Unsupported []string // command names that are not tail-compatible
}

func (e *TailValidationError) Error() string {
	return fmt.Sprintf("unsupported commands for tail: %s (aggregation and stateful commands require full materialization)",
		strings.Join(e.Unsupported, ", "))
}

// ValidateForTail checks that all commands in the program are tail-compatible
// (streaming, event-by-event). Returns TailValidationError listing any
// unsupported commands.
func ValidateForTail(prog *spl2.Program) error {
	// CTEs require materialization — reject.
	if len(prog.Datasets) > 0 {
		return &TailValidationError{Unsupported: []string{"CTE ($dataset)"}}
	}

	if prog.Main == nil {
		return nil
	}

	var unsupported []string
	for _, cmd := range prog.Main.Commands {
		if !isTailCompatible(cmd) {
			unsupported = append(unsupported, commandName(cmd))
		}
	}
	if len(unsupported) > 0 {
		return &TailValidationError{Unsupported: unsupported}
	}

	return nil
}

// isTailCompatible returns true if the command can process events one-by-one
// in streaming mode.
func isTailCompatible(cmd spl2.Command) bool {
	switch cmd.(type) {
	case *spl2.SearchCommand,
		*spl2.WhereCommand,
		*spl2.EvalCommand,
		*spl2.FieldsCommand,
		*spl2.TableCommand,
		*spl2.RenameCommand,
		*spl2.RexCommand,
		*spl2.FillnullCommand,
		*spl2.HeadCommand,
		*spl2.BinCommand:
		return true
	default:
		return false
	}
}

func commandName(cmd spl2.Command) string {
	switch cmd.(type) {
	case *spl2.StatsCommand:
		return "stats"
	case *spl2.SortCommand:
		return "sort"
	case *spl2.TailCommand:
		return "tail"
	case *spl2.TopNCommand:
		return "topn"
	case *spl2.DedupCommand:
		return "dedup"
	case *spl2.JoinCommand:
		return "join"
	case *spl2.AppendCommand:
		return "append"
	case *spl2.MultisearchCommand:
		return "multisearch"
	case *spl2.EventstatsCommand:
		return "eventstats"
	case *spl2.StreamstatsCommand:
		return "streamstats"
	case *spl2.TimechartCommand:
		return "timechart"
	case *spl2.TopCommand:
		return "top"
	case *spl2.RareCommand:
		return "rare"
	case *spl2.TransactionCommand:
		return "transaction"
	case *spl2.XYSeriesCommand:
		return "xyseries"
	case *spl2.MaterializeCommand:
		return "materialize"
	case *spl2.FromCommand:
		return "from"
	case *spl2.ViewsCommand:
		return "views"
	case *spl2.DropviewCommand:
		return "dropview"
	default:
		return fmt.Sprintf("%T", cmd)
	}
}
