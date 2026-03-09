package rebalance

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"
)

// mockMetaStateProvider implements MetaStateProvider for testing.
type mockMetaStateProvider struct {
	mu           sync.Mutex
	version      uint64
	plan         *RebalancePlan
	appliedPlans []*RebalancePlan
	applyErr     error
}

func (m *mockMetaStateProvider) StateVersion() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.version
}

func (m *mockMetaStateProvider) ComputeRebalancePlan(vPartCount uint32, rf int) *RebalancePlan {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.plan != nil {
		return m.plan
	}

	return &RebalancePlan{}
}

func (m *mockMetaStateProvider) ApplyRebalancePlan(_ context.Context, plan *RebalancePlan) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.applyErr != nil {
		return m.applyErr
	}

	m.appliedPlans = append(m.appliedPlans, plan)

	return nil
}

func (m *mockMetaStateProvider) PromoteISRReplica(_ context.Context, _ string) (int, error) {
	return 0, nil
}

func (m *mockMetaStateProvider) setVersion(v uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.version = v
}

func (m *mockMetaStateProvider) setPlan(p *RebalancePlan) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.plan = p
}

func (m *mockMetaStateProvider) getAppliedPlans() []*RebalancePlan {
	m.mu.Lock()
	defer m.mu.Unlock()

	result := make([]*RebalancePlan, len(m.appliedPlans))
	copy(result, m.appliedPlans)

	return result
}

func TestRebalancer_NoChangeNoRebalance(t *testing.T) {
	provider := &mockMetaStateProvider{version: 1}
	cfg := DefaultRebalancerConfig()
	cfg.CheckInterval = 10 * time.Millisecond
	cfg.CooldownPeriod = 0 // No cooldown for test.

	r := NewRebalancer(cfg, provider, slog.Default())

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	// Set version to match last version so no change detected.
	r.lastVersion = 1

	_ = r.Run(ctx)

	plans := provider.getAppliedPlans()
	if len(plans) != 0 {
		t.Errorf("expected no rebalance, got %d plans applied", len(plans))
	}
}

func TestRebalancer_VersionChangeTriggersRebalance(t *testing.T) {
	plan := &RebalancePlan{
		Epoch: 2,
		Moves: []ShardMove{
			{ShardKey: "p0", Partition: 0, OldPrimary: "node-1", NewPrimary: "node-2"},
		},
	}

	provider := &mockMetaStateProvider{
		version: 2,
		plan:    plan,
	}
	cfg := DefaultRebalancerConfig()
	cfg.CheckInterval = 10 * time.Millisecond
	cfg.CooldownPeriod = 0

	r := NewRebalancer(cfg, provider, slog.Default())
	r.lastVersion = 1 // Stale version.

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	_ = r.Run(ctx)

	plans := provider.getAppliedPlans()
	if len(plans) < 1 {
		t.Fatalf("expected at least 1 plan applied, got %d", len(plans))
	}
	if len(plans[0].Moves) != 1 {
		t.Errorf("expected 1 move, got %d", len(plans[0].Moves))
	}
}

func TestRebalancer_EmptyPlanSkipped(t *testing.T) {
	provider := &mockMetaStateProvider{
		version: 5,
		plan:    &RebalancePlan{Epoch: 6}, // Empty plan.
	}
	cfg := DefaultRebalancerConfig()
	cfg.CheckInterval = 10 * time.Millisecond
	cfg.CooldownPeriod = 0

	r := NewRebalancer(cfg, provider, slog.Default())
	r.lastVersion = 1

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_ = r.Run(ctx)

	plans := provider.getAppliedPlans()
	if len(plans) != 0 {
		t.Errorf("expected no plans applied for empty plan, got %d", len(plans))
	}
}

func TestRebalancer_CooldownEnforced(t *testing.T) {
	plan := &RebalancePlan{
		Epoch: 2,
		Moves: []ShardMove{
			{ShardKey: "p0", Partition: 0, OldPrimary: "node-1", NewPrimary: "node-2"},
		},
	}

	provider := &mockMetaStateProvider{version: 10, plan: plan}
	cfg := DefaultRebalancerConfig()
	cfg.CheckInterval = 10 * time.Millisecond
	cfg.CooldownPeriod = 1 * time.Hour // Very long cooldown.

	r := NewRebalancer(cfg, provider, slog.Default())
	r.lastVersion = 1
	r.lastRebalance = time.Now() // Just rebalanced.

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_ = r.Run(ctx)

	plans := provider.getAppliedPlans()
	if len(plans) != 0 {
		t.Errorf("expected no plans during cooldown, got %d", len(plans))
	}
}
