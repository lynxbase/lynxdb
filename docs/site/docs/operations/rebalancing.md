---
title: Rebalancing and Partition Splitting
description: How LynxDB automatically rebalances shards across nodes and splits hot partitions for even load distribution.
---

# Rebalancing and Partition Splitting

LynxDB automatically rebalances shard assignments when the cluster topology changes and splits hot partitions to distribute write load. Both operations run as background processes on the meta leader.

## Automatic Rebalancing

### When Rebalancing Occurs

The rebalancer detects topology changes by monitoring the `MetaState.Version` counter. A rebalance is triggered when:

- A new node joins the cluster
- A node is declared dead
- A node is manually deregistered

### How It Works

1. **Detection**: The rebalancer polls the FSM version every 2 seconds.
2. **Planning**: When a change is detected, it computes an **incremental rebalance plan** by comparing the current shard map against the hash ring's desired assignments. Only partitions whose primary or replicas differ are included in the plan.
3. **Cooldown**: A minimum of 30 seconds must elapse between consecutive rebalances to prevent thrashing during rapid topology changes.
4. **Application**: The plan is committed via Raft, ensuring all meta nodes agree.

### Minimal-Move Guarantee

The incremental rebalancer ensures that:

- Partitions already in their desired state are untouched (their epoch, state, and ISR remain intact).
- Partitions in transient states (`draining`, `migrating`, `splitting`) are skipped.
- Only partitions that actually need to move are included in the plan.
- The consistent hash ring (128 vnodes per node) minimizes partition movement — when a node joins or leaves, only ~1/N partitions move.

### Rebalance Lifecycle

When a primary must change, the shard goes through a drain cycle:

```
ShardActive (old primary)
  → ShardMigrating (PendingPrimary set)
  → old primary drains (flushes memtable, uploads segments)
  → ShardDraining
  → drain completes
  → ShardActive (new primary)
```

During migration:
- The **old primary** continues serving reads for in-flight queries.
- The **new primary** is recorded in `PendingPrimary` / `PendingReplicas`.
- Once the drain completes, the meta leader transitions the shard back to `ShardActive` with the new primary.

### Replica-Only Changes

When only the replica set changes (primary stays the same), the update is applied immediately without a drain cycle. The shard stays `ShardActive` throughout.

## Partition Splitting

### When Splitting Occurs

The splitter monitors per-partition ingest rates from node heartbeats. A partition is eligible for splitting when:

- Its ingest rate exceeds `HotPartitionThresholdEPS` (default: 50,000 events/sec)
- It has not already reached the maximum split depth (default: 10 levels)
- The cooldown period has elapsed (default: 2 minutes between splits)

### Hash-Bit Subdivision

LynxDB uses **hash-bit subdivision** to split partitions without rehashing existing data:

1. The splitter selects the hottest partition above threshold.
2. It examines one additional bit of the original `xxhash64` value (the "split bit").
3. Events where the split bit is 0 go to **child A**; events where it is 1 go to **child B**.
4. The `SplitRegistry` is updated so the event router consults it during assignment.

```
Before split:
  Partition 42 → all events with xxhash64(...) % 1024 == 42

After split (split_bit = 10):
  Child A (partition 1048618) → bit 10 of hash is 0
  Child B (partition 1048619) → bit 10 of hash is 1
```

**Key property**: No existing data needs to be rehashed or moved. Old segments remain readable under the parent partition prefix. Only new events are routed to the children.

### Split Lifecycle

```
ShardActive (parent partition)
  → ShardSplitting (split info recorded, children created as ShardMigrating)
  → children catch up with new writes
  → CmdCompleteSplit
  → children → ShardActive
  → parent removed from shard map
```

During the split:
- The parent continues serving reads.
- The `SplitRegistry` routes new writes to the correct child partition.
- Once children are stable, the split is completed and the parent is removed.

### Split Depth and Limits

Splits are chained — a child partition can be split again if it becomes hot. The `MaxSplitDepth` (default: 10) prevents unbounded splitting. At depth 10, a single original partition could be split into up to 1,024 leaf partitions.

## Monitoring

### Key Metrics

| Metric | Description |
|--------|-------------|
| `rebalance_total` | Total rebalances applied |
| `rebalance_move_total` | Total shard moves across all rebalances |
| `rebalance_duration_ns` | Duration of last rebalance |
| `split_total` | Total partition splits proposed |
| `split_duration_ns` | Duration of last split |
| `shard_draining` | Shards currently draining |
| `shard_migrating` | Shards currently migrating |
| `shard_splitting` | Shards currently splitting |

### Checking Shard State

```bash
lynxdb status
```

In cluster mode, the status output includes shard state counts:

```
Shards:  active=1024 draining=0 migrating=2 splitting=0
Epoch:   47
```

## Troubleshooting

### Rebalance Not Completing

If shards are stuck in `migrating` or `draining`:

1. Check the meta leader logs for errors.
2. Verify the target node is alive and reachable.
3. The drain timeout (default 5 minutes) will eventually force-complete the drain.

### Excessive Splitting

If splits are happening too frequently:

1. Check for uneven data distribution (one source/host dominating).
2. Increase `HotPartitionThresholdEPS` if the current threshold is too low for your workload.
3. Consider increasing `virtual_partition_count` to spread load more evenly across initial partitions.

### Partition Count Growing

Splits increase the total partition count. Monitor `shard_active` to ensure it stays within reasonable bounds. The `MaxSplitDepth` limit prevents unbounded growth.

## Next Steps

- [Distributed Architecture](/docs/architecture/distributed) -- how sharding works
- [Cluster Configuration](/docs/configuration/cluster) -- all cluster settings
- [Monitoring](/docs/operations/monitoring) -- cluster metrics reference
- [Large Cluster](/docs/deployment/large-cluster) -- production deployment guide
