package stats

import (
	"sync"
	"testing"
)

func BenchmarkGrowWithoutParent(b *testing.B) {
	mon := NewBudgetMonitor("bench", 0)
	acct := mon.NewAccount("scan")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = acct.Grow(100)
		acct.Shrink(100)
	}
}

func BenchmarkGrowWithParent(b *testing.B) {
	parent := NewRootMonitor("pool", 0)
	mon := NewBudgetMonitorWithParent("bench", 0, parent)
	acct := mon.NewAccount("scan")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = acct.Grow(100)
		acct.Shrink(100)
	}
}

func BenchmarkRootMonitor_Contended(b *testing.B) {
	rm := NewRootMonitor("pool", 0)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = rm.Reserve(100)
			rm.Release(100)
		}
	})
}

func BenchmarkResize(b *testing.B) {
	mon := NewBudgetMonitor("bench", 0)
	acct := mon.NewAccount("hash")

	// Pre-allocate some base.
	_ = acct.Grow(1000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = acct.Resize(1000, 2000)
		_ = acct.Resize(2000, 1000)
	}
}

func BenchmarkRootMonitor_MultiChild(b *testing.B) {
	parent := NewRootMonitor("pool", 0)
	const children = 10

	monitors := make([]*BudgetMonitor, children)
	accounts := make([]*BoundAccount, children)
	for i := 0; i < children; i++ {
		monitors[i] = NewBudgetMonitorWithParent("q", 0, parent)
		accounts[i] = monitors[i].NewAccount("scan")
	}

	b.ResetTimer()

	var wg sync.WaitGroup
	wg.Add(children)
	for i := 0; i < children; i++ {
		go func(acct *BoundAccount) {
			defer wg.Done()
			ops := b.N / children
			for j := 0; j < ops; j++ {
				_ = acct.Grow(100)
				acct.Shrink(100)
			}
		}(accounts[i])
	}
	wg.Wait()
}
