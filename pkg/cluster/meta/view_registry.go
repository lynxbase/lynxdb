package meta

import (
	"fmt"

	"github.com/lynxbase/lynxdb/pkg/cluster/sharding"
)

// ViewStatus represents the state of a materialized view in the cluster.
type ViewStatus string

const (
	// ViewStatusPending means the view is registered but backfill has not started.
	ViewStatusPending ViewStatus = "pending"
	// ViewStatusBackfill means the view is currently being backfilled.
	ViewStatusBackfill ViewStatus = "running"
	// ViewStatusActive means the view is fully operational.
	ViewStatusActive ViewStatus = "complete"
)

// GlobalViewInfo holds cluster-wide materialized view metadata.
type GlobalViewInfo struct {
	Name            string          `msgpack:"name"`
	Query           string          `msgpack:"query"`
	Status          ViewStatus      `msgpack:"status"`
	BackfillState   string          `msgpack:"backfill_state"` // "pending", "running", "complete"
	CoordinatorNode sharding.NodeID `msgpack:"coordinator_node"`
	Version         uint64          `msgpack:"version"`
}

// RegisterViewPayload is the payload for CmdRegisterView.
type RegisterViewPayload struct {
	ViewInfo GlobalViewInfo `msgpack:"view_info"`
}

// UnregisterViewPayload is the payload for CmdUnregisterView.
type UnregisterViewPayload struct {
	Name string `msgpack:"name"`
}

// applyRegisterView registers or updates a materialized view definition.
func (s *MetaState) applyRegisterView(payload []byte) error {
	var p RegisterViewPayload
	if err := UnmarshalPayload(payload, &p); err != nil {
		return fmt.Errorf("meta.applyRegisterView: %w", err)
	}

	existing, ok := s.Views[p.ViewInfo.Name]
	if ok {
		p.ViewInfo.Version = existing.Version + 1
	} else {
		p.ViewInfo.Version = 1
	}

	s.Views[p.ViewInfo.Name] = &p.ViewInfo
	s.Version++

	return nil
}

// applyUnregisterView removes a materialized view definition.
func (s *MetaState) applyUnregisterView(payload []byte) error {
	var p UnregisterViewPayload
	if err := UnmarshalPayload(payload, &p); err != nil {
		return fmt.Errorf("meta.applyUnregisterView: %w", err)
	}

	delete(s.Views, p.Name)
	s.Version++

	return nil
}
