package pipeline

import (
	"sync"
	"sync/atomic"

	"github.com/lynxbase/lynxdb/pkg/spl2"
	"github.com/lynxbase/lynxdb/pkg/stats"
)

// Reservation defaults: minimum useful memory for each spillable operator type.
// An operator's sub-limit never goes below its reservation, ensuring it can
// always hold at least one working unit (batch, partition, etc.) after a spill.
const (
	reservationSort       int64 = 256 * 1024 // 256KB — one batch of materialized rows
	reservationAggregate  int64 = 128 * 1024 // 128KB — one batch of group entries
	reservationJoin       int64 = 256 * 1024 // 256KB — one partition hash table
	reservationEventStats int64 = 256 * 1024 // 256KB — row buffer before spill
	reservationDedup      int64 = 1 << 20    // 1MB   — bloom filter minimum
)

// OperatorPhase tracks the lifecycle phase of a spillable operator.
// The coordinator uses this to reclaim memory from completed operators
// and redistribute it to active ones.
type OperatorPhase int32

const (
	// PhaseIdle means the operator is registered but has not started processing.
	PhaseIdle OperatorPhase = iota
	// PhaseBuilding means the operator is accumulating data (sort build, hash table build).
	PhaseBuilding
	// PhaseProbing means the operator is producing output (merge, probe).
	PhaseProbing
	// PhaseComplete means the operator has finished and its memory can be reclaimed.
	PhaseComplete
)

// SpillNotifier is optionally implemented by MemoryAccounts that support
// spill coordination. Operators call NotifySpilled() after spilling to disk
// to allow the coordinator to redistribute freed capacity to other operators.
type SpillNotifier interface {
	NotifySpilled()
}

// PhaseNotifier is optionally implemented by MemoryAccounts that support
// phase-aware lifecycle tracking. Operators call SetPhase at key transitions
// (Building -> Probing -> Complete) to enable the coordinator to reclaim
// memory from completed operators and redistribute it to active ones.
type PhaseNotifier interface {
	SetPhase(phase OperatorPhase)
}

// MemoryCoordinator distributes a per-query memory budget among spillable
// operators. When an operator spills, its sub-limit is reduced to its minimum
// reservation and the freed capacity is redistributed equally to non-spilled
// operators.
//
// Thread-safe: mu protects redistribution. CoordinatedAccounts themselves are
// NOT thread-safe (same contract as BoundAccount — one goroutine per operator).
type MemoryCoordinator struct {
	mu          sync.Mutex
	slots       []*operatorSlot
	totalBudget int64
	headroom    int64 // reserved for non-spillable operators (scan, tail, topn, etc.)
}

// operatorSlot tracks a single spillable operator's budget allocation within
// the coordinator.
type operatorSlot struct {
	label       string
	softLimit   atomic.Int64 // current sub-limit (read atomically, written under mu)
	reservation int64        // minimum useful memory (never goes below this)
	spilled     atomic.Bool  // one-way ratchet: once true, stays true
	phase       atomic.Int32 // OperatorPhase stored as int32
	acct        *CoordinatedAccount
}

// CoordinatedAccount wraps a MemoryAccount with a coordinator-managed sub-limit.
// Implements stats.MemoryAccount. NOT thread-safe (same contract as BoundAccount).
type CoordinatedAccount struct {
	inner       stats.MemoryAccount
	coordinator *MemoryCoordinator
	slot        *operatorSlot
	used        int64
	maxUsed     int64
}

// CoordinatorSlotStats provides per-slot observability data.
type CoordinatorSlotStats struct {
	Label       string
	SoftLimit   int64
	Reservation int64
	Used        int64
	MaxUsed     int64
	Spilled     bool
	Phase       OperatorPhase
}

// NewMemoryCoordinator creates a coordinator for the given total budget.
// HeadroomFraction reserves a fraction of the budget for non-spillable
// operators (clamped to [1MB, 10MB], max 25% of total).
func NewMemoryCoordinator(totalBudget int64, headroomFraction float64) *MemoryCoordinator {
	headroom := int64(float64(totalBudget) * headroomFraction)
	// Clamp headroom to [1MB, 10MB] and max 25% of total.
	const minHeadroom = 1 << 20  // 1MB
	const maxHeadroom = 10 << 20 // 10MB
	const maxFraction = 0.25

	maxAllowed := int64(float64(totalBudget) * maxFraction)
	if headroom < minHeadroom {
		headroom = minHeadroom
	}
	if headroom > maxHeadroom {
		headroom = maxHeadroom
	}
	if headroom > maxAllowed {
		headroom = maxAllowed
	}
	// Don't let headroom exceed total budget.
	if headroom > totalBudget {
		headroom = totalBudget
	}

	return &MemoryCoordinator{
		totalBudget: totalBudget,
		headroom:    headroom,
	}
}

// RegisterOperator registers a spillable operator with the coordinator.
// Returns a CoordinatedAccount that wraps the inner account with a
// coordinator-managed sub-limit. Must be called before Finalize().
func (mc *MemoryCoordinator) RegisterOperator(label string, inner stats.MemoryAccount, reservation int64) *CoordinatedAccount {
	slot := &operatorSlot{
		label:       label,
		reservation: reservation,
	}

	acct := &CoordinatedAccount{
		inner:       inner,
		coordinator: mc,
		slot:        slot,
	}
	slot.acct = acct

	mc.mu.Lock()
	mc.slots = append(mc.slots, slot)
	mc.mu.Unlock()

	return acct
}

// Finalize computes initial sub-limits after all operators have registered.
// The available budget (total - headroom - sum of reservations) is split
// equally among operators, with each operator's reservation added back.
//
// If the available budget after reservations is zero or negative, each
// operator gets exactly its reservation.
func (mc *MemoryCoordinator) Finalize() {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	if len(mc.slots) == 0 {
		return
	}

	// Sum reservations.
	var sumReservations int64
	for _, s := range mc.slots {
		sumReservations += s.reservation
	}

	// Available budget after headroom and reservations.
	remaining := mc.totalBudget - mc.headroom - sumReservations
	if remaining < 0 {
		remaining = 0
	}

	// Equal split of remaining among all operators.
	perOp := remaining / int64(len(mc.slots))

	for _, s := range mc.slots {
		s.softLimit.Store(s.reservation + perOp)
	}
}

// NotifySpilled is called by a CoordinatedAccount when its operator spills
// to disk. The spilled operator's sub-limit is reduced to its reservation,
// and the freed capacity is redistributed equally among non-spilled operators.
//
// One-way ratchet: once an operator is marked as spilled, subsequent calls
// are idempotent.
func (mc *MemoryCoordinator) NotifySpilled(slot *operatorSlot) {
	// Fast path: already spilled — no redistribution needed.
	if slot.spilled.Load() {
		return
	}

	mc.mu.Lock()
	defer mc.mu.Unlock()

	// Double-check under lock.
	if slot.spilled.Load() {
		return
	}
	slot.spilled.Store(true)

	// Compute freed capacity: current soft limit minus reservation.
	currentLimit := slot.softLimit.Load()
	freed := currentLimit - slot.reservation
	if freed <= 0 {
		// Already at or below reservation — nothing to redistribute.
		slot.softLimit.Store(slot.reservation)

		return
	}

	// Reduce spilled operator to reservation.
	slot.softLimit.Store(slot.reservation)

	// Count active (non-spilled) operators.
	var activeSlots []*operatorSlot
	for _, s := range mc.slots {
		if !s.spilled.Load() {
			activeSlots = append(activeSlots, s)
		}
	}

	if len(activeSlots) == 0 {
		// All operators spilled — freed capacity goes to headroom (implicit).
		return
	}

	// Distribute freed capacity equally among active operators.
	perActive := freed / int64(len(activeSlots))
	remainder := freed % int64(len(activeSlots))

	for i, s := range activeSlots {
		bonus := perActive
		if int64(i) < remainder {
			bonus++ // distribute remainder evenly
		}
		s.softLimit.Add(bonus)
	}
}

// reclaimFromCompleted reclaims all capacity from a completed operator and
// redistributes it to active (non-spilled, non-complete) operators.
// Called under mc.mu by SetPhase when an operator transitions to PhaseComplete.
//
// Unlike NotifySpilled which reduces to reservation, completion reduces the
// soft limit to zero — a completed operator will never allocate again.
func (mc *MemoryCoordinator) reclaimFromCompleted(slot *operatorSlot) {
	freed := slot.softLimit.Load()
	if freed <= 0 {
		slot.softLimit.Store(0)

		return
	}

	// Reduce completed operator to zero.
	slot.softLimit.Store(0)

	// Find active slots: not spilled, not complete.
	var activeSlots []*operatorSlot
	for _, s := range mc.slots {
		if s == slot {
			continue
		}
		phase := OperatorPhase(s.phase.Load())
		if phase != PhaseComplete && !s.spilled.Load() {
			activeSlots = append(activeSlots, s)
		}
	}

	if len(activeSlots) == 0 {
		// No active operators — freed capacity returns to headroom (implicit).
		return
	}

	// Distribute freed capacity equally among active operators.
	perActive := freed / int64(len(activeSlots))
	remainder := freed % int64(len(activeSlots))

	for i, s := range activeSlots {
		bonus := perActive
		if int64(i) < remainder {
			bonus++ // distribute remainder evenly
		}
		s.softLimit.Add(bonus)
	}
}

// Stats returns per-slot statistics for observability.
func (mc *MemoryCoordinator) Stats() []CoordinatorSlotStats {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	result := make([]CoordinatorSlotStats, len(mc.slots))
	for i, s := range mc.slots {
		result[i] = CoordinatorSlotStats{
			Label:       s.label,
			SoftLimit:   s.softLimit.Load(),
			Reservation: s.reservation,
			Used:        s.acct.used,
			MaxUsed:     s.acct.maxUsed,
			Spilled:     s.spilled.Load(),
			Phase:       OperatorPhase(s.phase.Load()),
		}
	}

	return result
}

// CoordinatedAccount implements stats.MemoryAccount

// Grow requests n bytes. First checks the coordinator-managed sub-limit
// (fast atomic load, no lock). If the sub-limit would be exceeded, returns
// a *stats.BudgetExceededError. Otherwise delegates to the inner account
// for total budget enforcement.
func (ca *CoordinatedAccount) Grow(n int64) error {
	if n <= 0 {
		return nil
	}

	// Check coordinator sub-limit (fast path: one atomic load).
	if ca.used+n > ca.slot.softLimit.Load() {
		return &stats.BudgetExceededError{
			Monitor:   "coordinator",
			Account:   ca.slot.label,
			Requested: n,
			Current:   ca.used,
			Limit:     ca.slot.softLimit.Load(),
		}
	}

	// Delegate to inner account for total budget enforcement.
	if err := ca.inner.Grow(n); err != nil {
		return err
	}

	ca.used += n
	if ca.used > ca.maxUsed {
		ca.maxUsed = ca.used
	}

	return nil
}

// Shrink releases n bytes.
func (ca *CoordinatedAccount) Shrink(n int64) {
	if n <= 0 {
		return
	}
	if n > ca.used {
		n = ca.used
	}
	ca.inner.Shrink(n)
	ca.used -= n
}

// Close releases all remaining bytes.
func (ca *CoordinatedAccount) Close() {
	if ca.used > 0 {
		ca.inner.Shrink(ca.used)
		ca.used = 0
	}
	ca.inner.Close()
}

// Used returns the current tracked byte count.
func (ca *CoordinatedAccount) Used() int64 {
	return ca.used
}

// MaxUsed returns the peak tracked byte count.
func (ca *CoordinatedAccount) MaxUsed() int64 {
	return ca.maxUsed
}

// NotifySpilled notifies the coordinator that this operator has spilled to
// disk, triggering budget redistribution.
func (ca *CoordinatedAccount) NotifySpilled() {
	if ca.coordinator != nil {
		ca.coordinator.NotifySpilled(ca.slot)
	}
}

// SetPhase updates this operator's lifecycle phase. When the phase transitions
// to PhaseComplete, the coordinator reclaims all remaining capacity from this
// operator and redistributes it to active operators.
func (ca *CoordinatedAccount) SetPhase(phase OperatorPhase) {
	old := OperatorPhase(ca.slot.phase.Load())
	if old == phase {
		return // no-op
	}
	ca.slot.phase.Store(int32(phase))

	if phase == PhaseComplete && ca.coordinator != nil {
		ca.coordinator.mu.Lock()
		defer ca.coordinator.mu.Unlock()
		ca.coordinator.reclaimFromCompleted(ca.slot)
	}
}

// AST pre-scan for spillable operator count

// countSpillableOps counts the number of spillable operators in a Program,
// including CTEs and subqueries. Used to determine whether a coordinator
// should be created (requires 2+).
func countSpillableOps(prog *spl2.Program) int {
	if prog == nil {
		return 0
	}

	count := 0

	// Count in CTE datasets.
	for _, ds := range prog.Datasets {
		if ds.Query != nil {
			count += countSpillableInQuery(ds.Query)
		}
	}

	// Count in main query.
	if prog.Main != nil {
		count += countSpillableInQuery(prog.Main)
	}

	return count
}

// countSpillableInQuery counts spillable operators in a single Query,
// recursing into subqueries (JOIN, APPEND, MULTISEARCH).
func countSpillableInQuery(query *spl2.Query) int {
	if query == nil {
		return 0
	}

	count := 0
	for _, cmd := range query.Commands {
		switch c := cmd.(type) {
		case *spl2.SortCommand:
			count++
		case *spl2.StatsCommand:
			count++
		case *spl2.TimechartCommand:
			count++
		case *spl2.EventstatsCommand:
			count++
		case *spl2.DedupCommand:
			count++
		case *spl2.JoinCommand:
			count++
			if c.Subquery != nil {
				count += countSpillableInQuery(c.Subquery)
			}
		case *spl2.AppendCommand:
			if c.Subquery != nil {
				count += countSpillableInQuery(c.Subquery)
			}
		case *spl2.MultisearchCommand:
			for _, search := range c.Searches {
				count += countSpillableInQuery(search)
			}
		}
	}

	return count
}
