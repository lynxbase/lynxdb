package simulation

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

// NetworkFault describes an injected network fault between two nodes.
type NetworkFault struct {
	// Partitioned means messages between the two nodes are dropped.
	Partitioned bool
	// LatencyMin and LatencyMax define the range of injected latency.
	LatencyMin time.Duration
	LatencyMax time.Duration
	// DropRate is the probability [0.0, 1.0] of dropping a message.
	DropRate float64
}

// SimNetwork simulates an unreliable network between cluster nodes.
// It supports partition injection, latency injection, and random
// message dropping. All methods are safe for concurrent use.
type SimNetwork struct {
	mu     sync.RWMutex
	faults map[string]*NetworkFault // key: "nodeA->nodeB"
	rng    *rand.Rand
}

// NewSimNetwork creates a new simulated network with no faults.
func NewSimNetwork(seed int64) *SimNetwork {
	return &SimNetwork{
		faults: make(map[string]*NetworkFault),
		rng:    rand.New(rand.NewSource(seed)),
	}
}

// faultKey returns a directional key for a fault between two nodes.
func faultKey(from, to string) string {
	return fmt.Sprintf("%s->%s", from, to)
}

// PartitionNode isolates a node from all other nodes (bidirectional).
func (n *SimNetwork) PartitionNode(nodeID string, allNodes []string) {
	n.mu.Lock()
	defer n.mu.Unlock()

	for _, other := range allNodes {
		if other == nodeID {
			continue
		}
		n.faults[faultKey(nodeID, other)] = &NetworkFault{Partitioned: true}
		n.faults[faultKey(other, nodeID)] = &NetworkFault{Partitioned: true}
	}
}

// HealNode removes all faults involving a node.
func (n *SimNetwork) HealNode(nodeID string) {
	n.mu.Lock()
	defer n.mu.Unlock()

	for key := range n.faults {
		if len(key) > len(nodeID)+2 {
			// Check if key starts with or ends with nodeID.
			prefix := nodeID + "->"
			suffix := "->" + nodeID
			if len(key) >= len(prefix) && key[:len(prefix)] == prefix {
				delete(n.faults, key)
			} else if len(key) >= len(suffix) && key[len(key)-len(suffix):] == suffix {
				delete(n.faults, key)
			}
		}
	}
}

// AddLatency injects latency between two nodes (bidirectional).
func (n *SimNetwork) AddLatency(nodeA, nodeB string, min, max time.Duration) {
	n.mu.Lock()
	defer n.mu.Unlock()

	fault := &NetworkFault{LatencyMin: min, LatencyMax: max}
	n.faults[faultKey(nodeA, nodeB)] = fault
	n.faults[faultKey(nodeB, nodeA)] = fault
}

// SetDropRate sets the message drop probability between two nodes.
func (n *SimNetwork) SetDropRate(nodeA, nodeB string, rate float64) {
	n.mu.Lock()
	defer n.mu.Unlock()

	for _, key := range []string{faultKey(nodeA, nodeB), faultKey(nodeB, nodeA)} {
		f, ok := n.faults[key]
		if ok {
			f.DropRate = rate
		} else {
			n.faults[key] = &NetworkFault{DropRate: rate}
		}
	}
}

// HealAll removes all network faults.
func (n *SimNetwork) HealAll() {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.faults = make(map[string]*NetworkFault)
}

// ShouldDeliver checks whether a message from 'from' to 'to' should be
// delivered. Returns (deliver bool, delay time.Duration).
// If deliver is false, the message should be dropped.
func (n *SimNetwork) ShouldDeliver(from, to string) (bool, time.Duration) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	fault, ok := n.faults[faultKey(from, to)]
	if !ok {
		return true, 0
	}

	if fault.Partitioned {
		return false, 0
	}

	if fault.DropRate > 0 && n.rng.Float64() < fault.DropRate {
		return false, 0
	}

	var delay time.Duration
	if fault.LatencyMax > 0 {
		diff := fault.LatencyMax - fault.LatencyMin
		if diff > 0 {
			delay = fault.LatencyMin + time.Duration(n.rng.Int63n(int64(diff)))
		} else {
			delay = fault.LatencyMin
		}
	}

	return true, delay
}

// IsPartitioned reports whether from and to are partitioned.
func (n *SimNetwork) IsPartitioned(from, to string) bool {
	n.mu.RLock()
	defer n.mu.RUnlock()

	f, ok := n.faults[faultKey(from, to)]

	return ok && f.Partitioned
}
