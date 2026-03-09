package rebalance

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"
)

// mockSplitStateProvider implements SplitStateProvider for testing.
type mockSplitStateProvider struct {
	mu     sync.Mutex
	loads  []PartitionLoad
	splits []splitProposal
}

type splitProposal struct {
	parent, childA, childB uint32
	splitBit               uint8
}

func (m *mockSplitStateProvider) PartitionLoads() []PartitionLoad {
	m.mu.Lock()
	defer m.mu.Unlock()

	result := make([]PartitionLoad, len(m.loads))
	copy(result, m.loads)

	return result
}

func (m *mockSplitStateProvider) ProposeSplit(_ context.Context, parent, childA, childB uint32, splitBit uint8) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.splits = append(m.splits, splitProposal{parent, childA, childB, splitBit})

	return nil
}

func (m *mockSplitStateProvider) getSplits() []splitProposal {
	m.mu.Lock()
	defer m.mu.Unlock()

	result := make([]splitProposal, len(m.splits))
	copy(result, m.splits)

	return result
}

func TestSplitter_NoHotPartitions(t *testing.T) {
	provider := &mockSplitStateProvider{
		loads: []PartitionLoad{
			{Partition: 0, IngestRateEPS: 1000},
			{Partition: 1, IngestRateEPS: 2000},
		},
	}
	cfg := DefaultSplitterConfig()
	cfg.CheckInterval = 10 * time.Millisecond
	cfg.HotPartitionThresholdEPS = 100000

	s := NewSplitter(cfg, provider, slog.Default())

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_ = s.Run(ctx)

	splits := provider.getSplits()
	if len(splits) != 0 {
		t.Errorf("expected no splits, got %d", len(splits))
	}
}

func TestSplitter_HotPartitionSplit(t *testing.T) {
	provider := &mockSplitStateProvider{
		loads: []PartitionLoad{
			{Partition: 0, IngestRateEPS: 1000},
			{Partition: 5, IngestRateEPS: 100000, CurrentBit: 0}, // Hot.
			{Partition: 10, IngestRateEPS: 2000},
		},
	}
	cfg := DefaultSplitterConfig()
	cfg.CheckInterval = 10 * time.Millisecond
	cfg.CooldownPeriod = 0
	cfg.HotPartitionThresholdEPS = 50000

	s := NewSplitter(cfg, provider, slog.Default())

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	_ = s.Run(ctx)

	splits := provider.getSplits()
	if len(splits) < 1 {
		t.Fatalf("expected at least 1 split, got %d", len(splits))
	}

	if splits[0].parent != 5 {
		t.Errorf("expected split for partition 5, got %d", splits[0].parent)
	}
	if splits[0].splitBit != 0 {
		t.Errorf("expected split_bit=0, got %d", splits[0].splitBit)
	}
}

func TestSplitter_MaxDepthRespected(t *testing.T) {
	provider := &mockSplitStateProvider{
		loads: []PartitionLoad{
			{Partition: 5, IngestRateEPS: 100000, CurrentBit: 10}, // At max depth.
		},
	}
	cfg := DefaultSplitterConfig()
	cfg.CheckInterval = 10 * time.Millisecond
	cfg.CooldownPeriod = 0
	cfg.HotPartitionThresholdEPS = 50000
	cfg.MaxSplitDepth = 10

	s := NewSplitter(cfg, provider, slog.Default())

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_ = s.Run(ctx)

	splits := provider.getSplits()
	if len(splits) != 0 {
		t.Errorf("expected no splits at max depth, got %d", len(splits))
	}
}

func TestSplitter_CooldownEnforced(t *testing.T) {
	provider := &mockSplitStateProvider{
		loads: []PartitionLoad{
			{Partition: 5, IngestRateEPS: 100000, CurrentBit: 0},
		},
	}
	cfg := DefaultSplitterConfig()
	cfg.CheckInterval = 10 * time.Millisecond
	cfg.CooldownPeriod = 1 * time.Hour
	cfg.HotPartitionThresholdEPS = 50000

	s := NewSplitter(cfg, provider, slog.Default())
	s.lastSplit = time.Now() // Just split.

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_ = s.Run(ctx)

	splits := provider.getSplits()
	if len(splits) != 0 {
		t.Errorf("expected no splits during cooldown, got %d", len(splits))
	}
}

func TestSplitter_PicksHottestPartition(t *testing.T) {
	provider := &mockSplitStateProvider{
		loads: []PartitionLoad{
			{Partition: 1, IngestRateEPS: 60000},
			{Partition: 2, IngestRateEPS: 90000}, // Hottest.
			{Partition: 3, IngestRateEPS: 70000},
		},
	}
	cfg := DefaultSplitterConfig()
	cfg.CheckInterval = 10 * time.Millisecond
	cfg.CooldownPeriod = 0
	cfg.HotPartitionThresholdEPS = 50000

	s := NewSplitter(cfg, provider, slog.Default())

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	_ = s.Run(ctx)

	splits := provider.getSplits()
	if len(splits) < 1 {
		t.Fatalf("expected at least 1 split, got %d", len(splits))
	}
	if splits[0].parent != 2 {
		t.Errorf("expected split for partition 2 (hottest), got %d", splits[0].parent)
	}
}
