package meta

import (
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"

	"github.com/lynxbase/lynxdb/pkg/config"
)

// SetupRaft creates and configures a Raft instance for the meta service.
// It sets up TCP transport, BoltDB log/stable store, and file snapshot store.
// If no existing state is found and bootstrap is true, the node bootstraps
// itself as the initial cluster leader.
func SetupRaft(cfg config.ClusterConfig, fsm raft.FSM, dataDir string, logger *slog.Logger) (*raft.Raft, error) {
	raftDir := filepath.Join(dataDir, "raft")
	if err := os.MkdirAll(raftDir, 0o755); err != nil {
		return nil, fmt.Errorf("meta.SetupRaft: create raft dir: %w", err)
	}

	// Raft configuration.
	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(cfg.NodeID)
	raftConfig.HeartbeatTimeout = cfg.HeartbeatInterval.Duration()
	raftConfig.ElectionTimeout = cfg.LeaseDuration.Duration()
	raftConfig.LeaderLeaseTimeout = cfg.LeaseDuration.Duration()
	raftConfig.SnapshotThreshold = 1024
	raftConfig.SnapshotInterval = 5 * time.Minute

	// Use slog-compatible logger adapter.
	raftConfig.Logger = newHCLogAdapter(logger)

	// TCP transport.
	bindAddr := fmt.Sprintf("0.0.0.0:%d", cfg.GRPCPort+1) // Raft uses GRPCPort+1 by convention.
	advertiseAddr, err := net.ResolveTCPAddr("tcp", bindAddr)
	if err != nil {
		return nil, fmt.Errorf("meta.SetupRaft: resolve bind addr: %w", err)
	}

	transport, err := raft.NewTCPTransport(bindAddr, advertiseAddr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("meta.SetupRaft: create transport: %w", err)
	}

	// BoltDB log and stable store.
	boltStore, err := raftboltdb.NewBoltStore(filepath.Join(raftDir, "raft.db"))
	if err != nil {
		return nil, fmt.Errorf("meta.SetupRaft: create bolt store: %w", err)
	}

	// File-based snapshot store.
	snapshotStore, err := raft.NewFileSnapshotStore(raftDir, 3, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("meta.SetupRaft: create snapshot store: %w", err)
	}

	r, err := raft.NewRaft(raftConfig, fsm, boltStore, boltStore, snapshotStore, transport)
	if err != nil {
		return nil, fmt.Errorf("meta.SetupRaft: create raft: %w", err)
	}

	// Bootstrap if no existing state.
	hasState, err := raft.HasExistingState(boltStore, boltStore, snapshotStore)
	if err != nil {
		return nil, fmt.Errorf("meta.SetupRaft: check existing state: %w", err)
	}

	if !hasState {
		logger.Info("no existing Raft state, bootstrapping cluster",
			"node_id", cfg.NodeID,
			"bind_addr", bindAddr)

		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      raft.ServerID(cfg.NodeID),
					Address: raft.ServerAddress(bindAddr),
				},
			},
		}

		if f := r.BootstrapCluster(configuration); f.Error() != nil {
			return nil, fmt.Errorf("meta.SetupRaft: bootstrap: %w", f.Error())
		}
	}

	return r, nil
}
