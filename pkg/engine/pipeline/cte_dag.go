package pipeline

import (
	"context"
	"fmt"
	"strings"

	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/memgov"
	"github.com/lynxbase/lynxdb/pkg/spl2"
)

// CTEExecutionPlan describes the order in which CTEs should be materialized.
// Independent CTEs are grouped into levels; all CTEs within a level can be
// materialized in parallel because they have no mutual data dependencies.
//
// Example: $a = ...; $b = ...; $c = FROM $a JOIN [$b]
//
//	Level 0: [$a, $b]   (independent — run in parallel)
//	Level 1: [$c]       (depends on $a and $b — run after level 0)
type CTEExecutionPlan struct {
	// Levels holds groups of CTE indices (into the original DatasetDef slice).
	// Each level can be executed in parallel; levels must be executed sequentially.
	Levels [][]int
}

// BuildCTEExecutionPlan constructs a topological execution plan from CTE
// definitions. It detects variable references in each CTE's source and
// subquery commands to build a dependency graph, then produces a level-order
// schedule using Kahn's algorithm.
//
// Returns an error if a dependency cycle is detected.
func BuildCTEExecutionPlan(datasets []spl2.DatasetDef) (*CTEExecutionPlan, error) {
	n := len(datasets)
	if n == 0 {
		return &CTEExecutionPlan{}, nil
	}

	// Map CTE name → index in datasets slice.
	nameToIdx := make(map[string]int, n)
	for i, ds := range datasets {
		nameToIdx[ds.Name] = i
	}

	// Build adjacency list: adj[i] = list of CTEs that depend on datasets[i].
	// inDegree[j] = number of CTEs that j depends on.
	adj := make([][]int, n)
	inDegree := make([]int, n)

	for i, ds := range datasets {
		deps := collectVariableRefs(ds.Query)
		for _, dep := range deps {
			depIdx, ok := nameToIdx[dep]
			if !ok {
				// Reference to a CTE not in this program — could be an error
				// or a forward reference. Let pipeline.buildQuery handle it.
				continue
			}
			if depIdx == i {
				return nil, fmt.Errorf("cte_dag.BuildCTEExecutionPlan: CTE $%s references itself", ds.Name)
			}
			adj[depIdx] = append(adj[depIdx], i)
			inDegree[i]++
		}
	}

	// Kahn's algorithm: BFS level-order topological sort.
	var queue []int
	for i := 0; i < n; i++ {
		if inDegree[i] == 0 {
			queue = append(queue, i)
		}
	}

	var levels [][]int
	processed := 0

	for len(queue) > 0 {
		// All nodes in the current queue have in-degree 0 — they form one level.
		level := make([]int, len(queue))
		copy(level, queue)
		levels = append(levels, level)
		processed += len(level)

		var nextQueue []int
		for _, node := range queue {
			for _, neighbor := range adj[node] {
				inDegree[neighbor]--
				if inDegree[neighbor] == 0 {
					nextQueue = append(nextQueue, neighbor)
				}
			}
		}
		queue = nextQueue
	}

	if processed != n {
		// Cycle detected — find the involved CTEs for a helpful error message.
		var cycleNames []string
		for i := 0; i < n; i++ {
			if inDegree[i] > 0 {
				cycleNames = append(cycleNames, "$"+datasets[i].Name)
			}
		}

		return nil, fmt.Errorf("cte_dag.BuildCTEExecutionPlan: dependency cycle among CTEs: %s",
			strings.Join(cycleNames, ", "))
	}

	return &CTEExecutionPlan{Levels: levels}, nil
}

// collectVariableRefs returns the set of variable names referenced by a query.
// It inspects the Source clause (IsVariable) and recurses into subquery commands
// (APPEND, JOIN, MULTISEARCH) to find transitive variable references.
func collectVariableRefs(q *spl2.Query) []string {
	if q == nil {
		return nil
	}

	seen := make(map[string]bool)
	collectVariableRefsRecurse(q, seen)

	refs := make([]string, 0, len(seen))
	for name := range seen {
		refs = append(refs, name)
	}

	return refs
}

func collectVariableRefsRecurse(q *spl2.Query, seen map[string]bool) {
	if q == nil {
		return
	}

	if q.Source != nil && q.Source.IsVariable {
		if !seen[q.Source.Index] {
			seen[q.Source.Index] = true
		}
	}

	// Check commands for subqueries that may reference variables.
	for _, cmd := range q.Commands {
		switch c := cmd.(type) {
		case *spl2.AppendCommand:
			collectVariableRefsRecurse(c.Subquery, seen)
		case *spl2.JoinCommand:
			collectVariableRefsRecurse(c.Subquery, seen)
		case *spl2.MultisearchCommand:
			for _, sub := range c.Searches {
				collectVariableRefsRecurse(sub, seen)
			}
		}
	}
}

// materializeCTEs executes CTE materialization using the DAG-based execution
// plan. Independent CTEs at the same dependency level are materialized in
// parallel when parallelism is enabled. The results are stored in qc.datasets.
func (qc *queryContext) materializeCTEs(ctx context.Context, datasets []spl2.DatasetDef) error {
	if len(datasets) == 0 {
		return nil
	}

	plan, err := BuildCTEExecutionPlan(datasets)
	if err != nil {
		return err
	}

	for _, level := range plan.Levels {
		if len(level) == 1 || !qc.parallelCfg.Enabled {
			// Sequential materialization: either single CTE or parallelism disabled.
			for _, idx := range level {
				ds := datasets[idx]
				iter, iterErr := qc.buildQuery(ctx, ds.Query)
				if iterErr != nil {
					return fmt.Errorf("dataset $%s: %w", ds.Name, iterErr)
				}
				rows, collectErr := CollectAll(ctx, iter)
				if collectErr != nil {
					return fmt.Errorf("dataset $%s: %w", ds.Name, collectErr)
				}
				qc.datasets[ds.Name] = rows
			}
		} else {
			// Parallel materialization: split budget evenly across CTEs.
			if err := qc.materializeCTEsParallel(ctx, datasets, level); err != nil {
				return err
			}
		}
	}

	return nil
}

// cteResult carries the materialized rows (or error) from a parallel CTE goroutine.
type cteResult struct {
	name string
	rows []map[string]event.Value
	err  error
}

// materializeCTEsParallel runs multiple independent CTEs concurrently.
// Each CTE gets a proportional share of the remaining memory budget.
func (qc *queryContext) materializeCTEsParallel(ctx context.Context, datasets []spl2.DatasetDef, level []int) error {
	ch := make(chan cteResult, len(level))

	// Compute per-CTE memory budget from remaining pool capacity.
	var perCTELimit int64
	if qc.govBudget != nil && qc.govBudget.Limit() > 0 {
		// Governor path: use per-query BudgetAdapter for remaining capacity.
		remaining := qc.govBudget.Limit() - qc.govBudget.CurAllocated()
		if remaining > 0 {
			perCTELimit = remaining / int64(len(level))
		}
	}

	// Snapshot datasets BEFORE launching goroutines. Goroutines need to read
	// previously materialized CTEs, but the main goroutine writes to
	// qc.datasets as results arrive. Without this snapshot, the map iteration
	// in withMonitor races with the map writes below.
	snapshot := make(map[string][]map[string]event.Value, len(qc.datasets))
	for k, v := range qc.datasets {
		snapshot[k] = v
	}

	for _, idx := range level {
		ds := datasets[idx]
		go func(ds spl2.DatasetDef) {
			// Governor path: CTEs share the same governor. Each CTE
			// gets its own BudgetAdapter with a per-CTE limit for
			// local budget splitting, but enforcement is global.
			var cteAdapter *memgov.BudgetAdapter
			if qc.govBudget != nil && perCTELimit > 0 {
				cteBudget := memgov.NewQueryBudget(qc.govBudget.Governor(), "cte-"+ds.Name)
				cteAdapter = memgov.NewBudgetAdapterWithLimit(cteBudget, qc.govBudget.Governor(), perCTELimit)
			}
			cteQC := qc.withGovBudget(cteAdapter, snapshot)
			iter, err := cteQC.buildQuery(ctx, ds.Query)
			if err != nil {
				ch <- cteResult{name: ds.Name, err: err}
				return
			}
			rows, err := CollectAll(ctx, iter)
			if cteAdapter != nil {
				cteAdapter.Close()
			}
			ch <- cteResult{name: ds.Name, rows: rows, err: err}
		}(ds)
	}

	// Collect results in main goroutine (safe map writes — no concurrent map access).
	for range level {
		r := <-ch
		if r.err != nil {
			return fmt.Errorf("dataset $%s: %w", r.name, r.err)
		}
		qc.datasets[r.name] = r.rows
	}

	return nil
}

// withGovBudget returns a shallow copy of the queryContext with a replaced
// governor BudgetAdapter for CTE sub-budgets. If adapter is nil, the parent's
// govBudget is inherited.
func (qc *queryContext) withGovBudget(adapter *memgov.BudgetAdapter, datasetSnapshot map[string][]map[string]event.Value) *queryContext {
	cp := *qc // shallow copy
	if adapter != nil {
		cp.govBudget = adapter
	}
	cp.datasets = datasetSnapshot
	return &cp
}
