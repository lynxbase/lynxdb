package meta

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/hashicorp/raft"
	"github.com/vmihailenco/msgpack/v5"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"

	"github.com/lynxbase/lynxdb/pkg/cluster/rpc"
	clusterpb "github.com/lynxbase/lynxdb/pkg/cluster/rpc/proto"
	"github.com/lynxbase/lynxdb/pkg/cluster/sharding"
	"github.com/lynxbase/lynxdb/pkg/cluster/tracing"
	"github.com/lynxbase/lynxdb/pkg/config"
)

// Service implements the MetaServiceServer gRPC interface.
// It mediates between gRPC requests and the Raft consensus layer.
type Service struct {
	clusterpb.UnimplementedMetaServiceServer

	raft   *raft.Raft
	fsm    *MetaFSM
	logger *slog.Logger
	cfg    config.ClusterConfig
}

// NewService creates a new meta gRPC service.
func NewService(r *raft.Raft, fsm *MetaFSM, cfg config.ClusterConfig, logger *slog.Logger) *Service {
	return &Service{
		raft:   r,
		fsm:    fsm,
		logger: logger,
		cfg:    cfg,
	}
}

// Handshake handles a node joining the cluster.
func (s *Service) Handshake(ctx context.Context, req *clusterpb.HandshakeRequest) (*clusterpb.HandshakeResponse, error) {
	ctx, span := tracing.Tracer().Start(ctx, "lynxdb.meta.Handshake",
		tracing.WithNodeID(req.NodeId),
		tracing.WithIsLeader(s.raft.State() == raft.Leader),
	)
	defer span.End()

	if err := rpc.CheckVersion(req.ProtocolVersion); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "version check failed")

		return nil, err
	}

	// Validate roles.
	if err := validateRoles(req.Roles); err != nil {
		return nil, fmt.Errorf("meta.Service.Handshake: %w", err)
	}

	// Register node via Raft.
	payload, err := MarshalPayload(RegisterNodePayload{
		Info: NodeInfo{
			ID:        sharding.NodeID(req.NodeId),
			Addr:      req.Addr,
			GRPCAddr:  req.GrpcAddr,
			Roles:     req.Roles,
			StartedAt: time.Now(),
			Version:   req.Version,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("meta.Service.Handshake: marshal: %w", err)
	}

	cmdData, err := EncodeCommand(&Command{Type: CmdRegisterNode, Data: payload})
	if err != nil {
		return nil, fmt.Errorf("meta.Service.Handshake: encode: %w", err)
	}

	future := s.raft.Apply(cmdData, 5*time.Second)
	if err := future.Error(); err != nil {
		// If not leader, return leader address for redirect.
		if leaderAddr, leaderID := s.raft.LeaderWithID(); leaderAddr != "" {
			return &clusterpb.HandshakeResponse{
				ProtocolVersion: rpc.ProtocolVersion,
				LeaderId:        string(leaderID),
				LeaderAddr:      string(leaderAddr),
			}, fmt.Errorf("not leader, redirect to %s", leaderAddr)
		}

		return nil, fmt.Errorf("meta.Service.Handshake: raft apply: %w", err)
	}

	// Return current shard map.
	state := s.fsm.State()
	smData, err := msgpack.Marshal(state.ShardMap)
	if err != nil {
		return nil, fmt.Errorf("meta.Service.Handshake: marshal shard map: %w", err)
	}

	return &clusterpb.HandshakeResponse{
		ProtocolVersion: rpc.ProtocolVersion,
		LeaderId:        string(s.cfg.NodeID),
		ShardMap:        smData,
		ShardMapEpoch:   state.ShardMap.Epoch,
	}, nil
}

// Heartbeat processes a node health report.
func (s *Service) Heartbeat(_ context.Context, req *clusterpb.HeartbeatRequest) (*clusterpb.HeartbeatResponse, error) {
	var resources NodeResourceReport
	if req.Resources != nil {
		resources = NodeResourceReport{
			CPUPercent:    req.Resources.CpuPercent,
			MemoryUsed:   req.Resources.MemoryUsedBytes,
			MemoryTotal:  req.Resources.MemoryTotalBytes,
			DiskUsed:     req.Resources.DiskUsedBytes,
			DiskTotal:    req.Resources.DiskTotalBytes,
			ActiveQueries: req.Resources.ActiveQueries,
			IngestRateEPS: req.Resources.IngestRateEps,
		}
	}

	state := s.fsm.State()
	state.ProcessHeartbeat(sharding.NodeID(req.NodeId), resources)

	_, leaderID := s.raft.LeaderWithID()

	return &clusterpb.HeartbeatResponse{
		Ok:       true,
		LeaderId: string(leaderID),
	}, nil
}

// WatchShardMap streams shard map updates to the caller.
// When state version changes, field catalog and source registry are piggybacked
// on the update so query nodes receive merged global state without extra RPCs.
func (s *Service) WatchShardMap(req *clusterpb.WatchShardMapRequest, stream clusterpb.MetaService_WatchShardMapServer) error {
	_, span := tracing.Tracer().Start(stream.Context(), "lynxdb.meta.WatchShardMap",
		tracing.WithIsLeader(s.raft.State() == raft.Leader),
	)
	defer span.End()

	span.SetAttributes(attribute.Int64("lynxdb.shard_map.current_epoch", int64(req.CurrentEpoch)))

	currentEpoch := req.CurrentEpoch
	var lastVersion uint64
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case <-ticker.C:
			state := s.fsm.State()
			if state.ShardMap.Epoch <= currentEpoch && state.Version <= lastVersion {
				continue
			}

			smData, err := msgpack.Marshal(state.ShardMap)
			if err != nil {
				s.logger.Error("WatchShardMap: marshal shard map failed", "error", err)

				continue
			}

			update := &clusterpb.ShardMapUpdate{
				ShardMap: smData,
				Epoch:    state.ShardMap.Epoch,
			}

			// Piggyback field catalog and source registry when state version changes.
			if state.Version > lastVersion {
				if fcData, err := msgpack.Marshal(globalFieldInfoSlice(state.FieldCatalog)); err == nil {
					update.FieldCatalog = fcData
				}
				if srcData, err := msgpack.Marshal(globalSourceInfoSlice(state.Sources)); err == nil {
					update.SourceRegistry = srcData
				}
			}

			if err := stream.Send(update); err != nil {
				return err
			}
			currentEpoch = state.ShardMap.Epoch
			lastVersion = state.Version
		}
	}
}

// globalFieldInfoSlice converts a map of GlobalFieldInfo to a slice for serialization.
func globalFieldInfoSlice(m map[string]*GlobalFieldInfo) []*GlobalFieldInfo {
	if len(m) == 0 {
		return nil
	}
	result := make([]*GlobalFieldInfo, 0, len(m))
	for _, v := range m {
		result = append(result, v)
	}

	return result
}

// globalSourceInfoSlice converts a map of GlobalSourceInfo to a slice for serialization.
func globalSourceInfoSlice(m map[string]*GlobalSourceInfo) []*GlobalSourceInfo {
	if len(m) == 0 {
		return nil
	}
	result := make([]*GlobalSourceInfo, 0, len(m))
	for _, v := range m {
		result = append(result, v)
	}

	return result
}

// RenewLease extends a shard lease.
func (s *Service) RenewLease(ctx context.Context, req *clusterpb.RenewLeaseRequest) (*clusterpb.RenewLeaseResponse, error) {
	_, span := tracing.Tracer().Start(ctx, "lynxdb.meta.RenewLease",
		tracing.WithNodeID(req.NodeId),
		tracing.WithIsLeader(s.raft.State() == raft.Leader),
	)
	defer span.End()

	payload, err := MarshalPayload(RenewLeasePayload{
		ShardID:       req.ShardId,
		HolderID:      sharding.NodeID(req.NodeId),
		Epoch:         req.Epoch,
		LeaseDuration: s.cfg.LeaseDuration.Duration(),
	})
	if err != nil {
		return nil, fmt.Errorf("meta.Service.RenewLease: marshal: %w", err)
	}

	cmdData, err := EncodeCommand(&Command{Type: CmdRenewLease, Data: payload})
	if err != nil {
		return nil, fmt.Errorf("meta.Service.RenewLease: encode: %w", err)
	}

	future := s.raft.Apply(cmdData, 5*time.Second)
	if err := future.Error(); err != nil {
		if leaderAddr, _ := s.raft.LeaderWithID(); leaderAddr != "" {
			return &clusterpb.RenewLeaseResponse{
				Granted:    false,
				LeaderAddr: string(leaderAddr),
			}, nil
		}

		return nil, fmt.Errorf("meta.Service.RenewLease: raft apply: %w", err)
	}

	// Check if the apply returned an error (e.g., epoch mismatch).
	if resp := future.Response(); resp != nil {
		if applyErr, ok := resp.(error); ok {
			return &clusterpb.RenewLeaseResponse{Granted: false}, applyErr
		}
	}

	state := s.fsm.State()
	lease := state.Leases[req.ShardId]

	var expiresNs int64
	if lease != nil {
		expiresNs = lease.ExpiresAt.UnixNano()
	}

	return &clusterpb.RenewLeaseResponse{
		Granted:          true,
		ExpiresAtUnixNs: expiresNs,
	}, nil
}

// RequestCompaction grants a compaction task.
func (s *Service) RequestCompaction(_ context.Context, req *clusterpb.RequestCompactionRequest) (*clusterpb.RequestCompactionResponse, error) {
	payload, err := MarshalPayload(GrantCompactionPayload{
		ShardID:        req.ShardId,
		AssignedNode:   sharding.NodeID(req.NodeId),
		CatalogVersion: req.CatalogVersion,
		InputParts:     req.InputParts,
		TargetLevel:    int(req.TargetLevel),
		TTL:            10 * time.Minute,
	})
	if err != nil {
		return nil, fmt.Errorf("meta.Service.RequestCompaction: marshal: %w", err)
	}

	cmdData, err := EncodeCommand(&Command{Type: CmdGrantCompaction, Data: payload})
	if err != nil {
		return nil, fmt.Errorf("meta.Service.RequestCompaction: encode: %w", err)
	}

	future := s.raft.Apply(cmdData, 5*time.Second)
	if err := future.Error(); err != nil {
		if leaderAddr, _ := s.raft.LeaderWithID(); leaderAddr != "" {
			return &clusterpb.RequestCompactionResponse{
				Granted:    false,
				LeaderAddr: string(leaderAddr),
			}, nil
		}

		return nil, fmt.Errorf("meta.Service.RequestCompaction: raft apply: %w", err)
	}

	if resp := future.Response(); resp != nil {
		if applyErr, ok := resp.(error); ok {
			return &clusterpb.RequestCompactionResponse{Granted: false}, applyErr
		}
	}

	return &clusterpb.RequestCompactionResponse{
		Granted: true,
		TaskId:  req.ShardId,
	}, nil
}

// ReleaseCompaction releases a compaction lock.
func (s *Service) ReleaseCompaction(_ context.Context, req *clusterpb.ReleaseCompactionRequest) (*clusterpb.ReleaseCompactionResponse, error) {
	payload, err := MarshalPayload(ReleaseCompactionPayload{
		ShardID:     req.TaskId,
		Success:     req.Success,
		OutputParts: req.OutputParts,
	})
	if err != nil {
		return nil, fmt.Errorf("meta.Service.ReleaseCompaction: marshal: %w", err)
	}

	cmdData, err := EncodeCommand(&Command{Type: CmdReleaseCompaction, Data: payload})
	if err != nil {
		return nil, fmt.Errorf("meta.Service.ReleaseCompaction: encode: %w", err)
	}

	future := s.raft.Apply(cmdData, 5*time.Second)
	if err := future.Error(); err != nil {
		return nil, fmt.Errorf("meta.Service.ReleaseCompaction: raft apply: %w", err)
	}

	return &clusterpb.ReleaseCompactionResponse{Ok: true}, nil
}

// ProposeDrain initiates draining a shard from a node.
func (s *Service) ProposeDrain(_ context.Context, req *clusterpb.ProposeDrainRequest) (*clusterpb.ProposeDrainResponse, error) {
	payload, err := MarshalPayload(ProposeDrainPayload{
		ShardID: req.ShardId,
		NodeID:  sharding.NodeID(req.NodeId),
	})
	if err != nil {
		return nil, fmt.Errorf("meta.Service.ProposeDrain: marshal: %w", err)
	}

	cmdData, err := EncodeCommand(&Command{Type: CmdProposeDrain, Data: payload})
	if err != nil {
		return nil, fmt.Errorf("meta.Service.ProposeDrain: encode: %w", err)
	}

	future := s.raft.Apply(cmdData, 5*time.Second)
	if err := future.Error(); err != nil {
		if leaderAddr, _ := s.raft.LeaderWithID(); leaderAddr != "" {
			return &clusterpb.ProposeDrainResponse{
				Ok:         false,
				LeaderAddr: string(leaderAddr),
			}, nil
		}

		return nil, fmt.Errorf("meta.Service.ProposeDrain: raft apply: %w", err)
	}

	if resp := future.Response(); resp != nil {
		if applyErr, ok := resp.(error); ok {
			return &clusterpb.ProposeDrainResponse{Ok: false}, applyErr
		}
	}

	return &clusterpb.ProposeDrainResponse{Ok: true}, nil
}

// CompleteDrain marks a shard drain as complete and assigns a new primary.
func (s *Service) CompleteDrain(_ context.Context, req *clusterpb.CompleteDrainRequest) (*clusterpb.CompleteDrainResponse, error) {
	payload, err := MarshalPayload(CompleteDrainPayload{
		ShardID:      req.ShardId,
		NodeID:       sharding.NodeID(req.NodeId),
		NewPrimaryID: sharding.NodeID(req.NewPrimaryId),
	})
	if err != nil {
		return nil, fmt.Errorf("meta.Service.CompleteDrain: marshal: %w", err)
	}

	cmdData, err := EncodeCommand(&Command{Type: CmdCompleteDrain, Data: payload})
	if err != nil {
		return nil, fmt.Errorf("meta.Service.CompleteDrain: encode: %w", err)
	}

	future := s.raft.Apply(cmdData, 5*time.Second)
	if err := future.Error(); err != nil {
		if leaderAddr, _ := s.raft.LeaderWithID(); leaderAddr != "" {
			return &clusterpb.CompleteDrainResponse{
				Ok:         false,
				LeaderAddr: string(leaderAddr),
			}, nil
		}

		return nil, fmt.Errorf("meta.Service.CompleteDrain: raft apply: %w", err)
	}

	if resp := future.Response(); resp != nil {
		if applyErr, ok := resp.(error); ok {
			return &clusterpb.CompleteDrainResponse{Ok: false}, applyErr
		}
	}

	return &clusterpb.CompleteDrainResponse{Ok: true}, nil
}

// ReportFieldCatalog receives field catalog deltas from ingest nodes and
// applies them to the FSM via Raft consensus.
func (s *Service) ReportFieldCatalog(_ context.Context, req *clusterpb.ReportFieldCatalogRequest) (*clusterpb.ReportFieldCatalogResponse, error) {
	fields := make([]FieldDelta, len(req.Fields))
	for i, f := range req.Fields {
		topValues := make([]FieldValueEntry, len(f.TopValues))
		for j, tv := range f.TopValues {
			topValues[j] = FieldValueEntry{Value: tv.Value, Count: tv.Count}
		}
		fields[i] = FieldDelta{
			Name:      f.Name,
			Type:      f.Type,
			Count:     f.Count,
			TopValues: topValues,
		}
	}

	payload, err := MarshalPayload(UpdateFieldCatalogPayload{
		NodeID: sharding.NodeID(req.NodeId),
		Fields: fields,
	})
	if err != nil {
		return nil, fmt.Errorf("meta.Service.ReportFieldCatalog: marshal: %w", err)
	}

	cmdData, err := EncodeCommand(&Command{Type: CmdUpdateFieldCatalog, Data: payload})
	if err != nil {
		return nil, fmt.Errorf("meta.Service.ReportFieldCatalog: encode: %w", err)
	}

	future := s.raft.Apply(cmdData, 5*time.Second)
	if err := future.Error(); err != nil {
		return nil, fmt.Errorf("meta.Service.ReportFieldCatalog: raft apply: %w", err)
	}

	return &clusterpb.ReportFieldCatalogResponse{Ok: true}, nil
}

// ReportSources receives source registry deltas from ingest nodes and
// applies them to the FSM via Raft consensus.
func (s *Service) ReportSources(_ context.Context, req *clusterpb.ReportSourcesRequest) (*clusterpb.ReportSourcesResponse, error) {
	sources := make([]SourceDelta, len(req.Sources))
	for i, src := range req.Sources {
		sources[i] = SourceDelta{
			Name:       src.Name,
			EventCount: src.EventCount,
			LastSeen:   time.Unix(0, src.LastSeenUnixNs),
		}
	}

	payload, err := MarshalPayload(UpdateSourceRegistryPayload{
		NodeID:  sharding.NodeID(req.NodeId),
		Sources: sources,
	})
	if err != nil {
		return nil, fmt.Errorf("meta.Service.ReportSources: marshal: %w", err)
	}

	cmdData, err := EncodeCommand(&Command{Type: CmdUpdateSourceRegistry, Data: payload})
	if err != nil {
		return nil, fmt.Errorf("meta.Service.ReportSources: encode: %w", err)
	}

	future := s.raft.Apply(cmdData, 5*time.Second)
	if err := future.Error(); err != nil {
		return nil, fmt.Errorf("meta.Service.ReportSources: raft apply: %w", err)
	}

	return &clusterpb.ReportSourcesResponse{Ok: true}, nil
}

// AssignAlert assigns an alert to a query node via rendezvous hashing.
func (s *Service) AssignAlert(_ context.Context, req *clusterpb.AssignAlertRequest) (*clusterpb.AssignAlertResponse, error) {
	queryNodes := make([]sharding.NodeID, len(req.QueryNodeIds))
	for i, id := range req.QueryNodeIds {
		queryNodes[i] = sharding.NodeID(id)
	}

	payload, err := MarshalPayload(AssignAlertPayload{
		AlertID:    req.AlertId,
		QueryNodes: queryNodes,
	})
	if err != nil {
		return nil, fmt.Errorf("meta.Service.AssignAlert: marshal: %w", err)
	}

	cmdData, err := EncodeCommand(&Command{Type: CmdAssignAlert, Data: payload})
	if err != nil {
		return nil, fmt.Errorf("meta.Service.AssignAlert: encode: %w", err)
	}

	future := s.raft.Apply(cmdData, 5*time.Second)
	if err := future.Error(); err != nil {
		return nil, fmt.Errorf("meta.Service.AssignAlert: raft apply: %w", err)
	}

	// Read back the assignment.
	state := s.fsm.State()
	assignedNode := ""
	if aa, ok := state.AlertAssign[req.AlertId]; ok {
		assignedNode = string(aa.AssignedNode)
	}

	return &clusterpb.AssignAlertResponse{
		Ok:             true,
		AssignedNodeId: assignedNode,
	}, nil
}

// ReportAlertFired records the timestamp of an alert trigger.
func (s *Service) ReportAlertFired(_ context.Context, req *clusterpb.ReportAlertFiredRequest) (*clusterpb.ReportAlertFiredResponse, error) {
	payload, err := MarshalPayload(UpdateAlertFiredPayload{
		AlertID: req.AlertId,
		FiredAt: time.Unix(0, req.FiredAtUnixNs),
	})
	if err != nil {
		return nil, fmt.Errorf("meta.Service.ReportAlertFired: marshal: %w", err)
	}

	cmdData, err := EncodeCommand(&Command{Type: CmdUpdateAlertFired, Data: payload})
	if err != nil {
		return nil, fmt.Errorf("meta.Service.ReportAlertFired: encode: %w", err)
	}

	future := s.raft.Apply(cmdData, 5*time.Second)
	if err := future.Error(); err != nil {
		return nil, fmt.Errorf("meta.Service.ReportAlertFired: raft apply: %w", err)
	}

	return &clusterpb.ReportAlertFiredResponse{Ok: true}, nil
}

// RegisterView registers or updates a materialized view definition in the cluster FSM.
func (s *Service) RegisterView(_ context.Context, req *clusterpb.RegisterViewRequest) (*clusterpb.RegisterViewResponse, error) {
	payload, err := MarshalPayload(RegisterViewPayload{
		ViewInfo: GlobalViewInfo{
			Name:            req.Name,
			Query:           req.Query,
			Status:          ViewStatus(req.Status),
			BackfillState:   req.Status,
			CoordinatorNode: sharding.NodeID(req.CoordinatorNodeId),
		},
	})
	if err != nil {
		return nil, fmt.Errorf("meta.Service.RegisterView: marshal: %w", err)
	}

	cmdData, err := EncodeCommand(&Command{Type: CmdRegisterView, Data: payload})
	if err != nil {
		return nil, fmt.Errorf("meta.Service.RegisterView: encode: %w", err)
	}

	future := s.raft.Apply(cmdData, 5*time.Second)
	if err := future.Error(); err != nil {
		return nil, fmt.Errorf("meta.Service.RegisterView: raft apply: %w", err)
	}

	return &clusterpb.RegisterViewResponse{Ok: true}, nil
}

// validRoles is the set of recognized cluster role names.
var validRoles = map[string]bool{
	"meta":   true,
	"ingest": true,
	"query":  true,
}

// validateRoles checks that all role strings are recognized.
func validateRoles(roles []string) error {
	if len(roles) == 0 {
		return fmt.Errorf("no roles specified")
	}

	for _, r := range roles {
		if !validRoles[strings.ToLower(strings.TrimSpace(r))] {
			return fmt.Errorf("unknown role %q (valid: meta, ingest, query)", r)
		}
	}

	return nil
}
