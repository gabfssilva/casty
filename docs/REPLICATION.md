# Replication in Casty

Casty provides **transparent replication** for high-availability actors across a distributed cluster. This document explains how to use the replication API.

## Overview

Replication in Casty provides:

- **High Availability**: Actors survive individual node failures
- **Configurable Consistency**: Choose your consistency vs. performance trade-off
- **Automatic Failover**: Deterministic hash ring rebalancing (no election needed)
- **Hinted Handoff**: Automatic catch-up when replicas recover
- **Actor-Agnostic**: No changes to actor implementation

## Quick Start

```python
from casty.cluster import ClusteredActorSystem, Replication, Quorum

async with ClusteredActorSystem.create(...) as system:
    # Replicate with 3 copies, wait for quorum
    accounts = await system.spawn(
        Account,
        name="accounts",
        sharded=True,
        replication=Replication(factor=3, consistency=Quorum()),
    )

    # Usage is identical to non-replicated actors
    await accounts["acc-1"].send(UpdateBalance(100))
    balance = await accounts["acc-1"].ask(GetBalance())
```

## The `Replication` Configuration

```python
from casty.cluster import Replication, Quorum, Strong, Eventual

# Replication dataclass
replication = Replication(
    factor=3,              # Number of replicas (1=no replication)
    consistency=Quorum(),  # Consistency level
)
```

### Replication Factor

- **`factor=1`** (default): No replication. Actor exists only on primary node.
- **`factor=2`**: Primary + 1 replica (2 total copies)
- **`factor=3`**: Primary + 2 replicas (3 total copies)
- **`factor=N`**: Primary + N-1 replicas

### Consistency Levels

Determine how many replicas must acknowledge writes before returning:

#### `Eventual()` (Fire-and-Forget)
- Returns immediately without waiting for replicas
- Replicas updated asynchronously
- **Best for**: High throughput, eventual consistency acceptable
- **Latency**: ~1x single-node
- **Default when**: `factor > 1`

```python
replication=Replication(factor=3, consistency=Eventual())
```

#### `Quorum()`
- Waits for majority of replicas (⌈n/2⌉ + 1)
- For 3 replicas: waits for 2 acks
- **Best for**: Balance of consistency and performance
- **Latency**: ~1.5-2x single-node
- **Default when**: `factor > 1` and not specified

```python
replication=Replication(factor=3, consistency=Quorum())
```

#### `Strong()`
- Waits for all replicas to acknowledge
- For 3 replicas: waits for all 3 acks
- **Best for**: Strongest consistency guarantees
- **Latency**: ~2-3x single-node

```python
replication=Replication(factor=3, consistency=Strong())
```

#### `AtLeast(n)`
- Waits for at least n replicas
- **Best for**: Custom requirements

```python
replication=Replication(factor=5, consistency=AtLeast(3))
```

## Actor Types with Replication

### Sharded Actors

Distribute entities across cluster with replication:

```python
accounts = await system.spawn(
    Account,
    name="accounts",
    sharded=True,
    replication=Replication(factor=3, consistency=Quorum()),
)

# Entity automatically replicated to 3 nodes
await accounts["acc-1"].send(UpdateBalance(100))

# Survive primary node failure
# Automatic failover to replica via hash ring rebalancing
```

### Singletons

Cluster-wide unique actors with replication:

```python
leader = await system.spawn(
    Leader,
    singleton=True,
    replication=Replication(factor=2, consistency=Strong()),
)

# Leader exists on exactly 1 node, replicated to 1 backup
# If primary fails, backup automatically becomes primary
await leader.send(BecomeLeader())
```

### Named Actors

Regular actors registered in cluster with replication:

```python
worker = await system.spawn(
    Worker,
    name="worker-1",
    replication=Replication(factor=3, consistency=Quorum()),
)

# Named actor replicated to 3 nodes
await worker.send(DoWork(100))
```

## How It Works

### Write Path (Coordinator-based Replication)

```
1. User sends message to entity
   await accounts["acc-1"].send(UpdateBalance(100))

2. Message routed to Cluster

3. Cluster calculates replica placement via hash ring
   preference_list = ["node-1", "node-2", "node-3"]

4. ReplicationCoordinator sends writes in PARALLEL to all replicas

5. Each replica processes and returns ack

6. Coordinator waits for quorum/all/eventual based on consistency

7. Returns to user (or fire-and-forget for Eventual)
```

### Automatic Failover

When a node fails:

1. **SWIM detects failure** → notifies Cluster
2. **Hash ring rebalances** → new preference list calculated
3. **Future messages route to new primary** → deterministic, no election
4. **Hinted handoff replays** → when failed node recovers

### Hinted Handoff

When a replica is offline:

1. **Write received but replica down** → write stored as "hint"
2. **Max 1000 hints per node** → FIFO eviction if exceeded
3. **Replica recovers** → hints replayed automatically
4. **Catch-up complete** → state converges

## Performance Characteristics

| Metric | Eventual | Quorum | Strong |
|--------|----------|--------|--------|
| **Latency** | ~1x | ~1.5-2x | ~2-3x |
| **Throughput** | Highest | Medium | Lowest |
| **Consistency** | Eventual | Strong | Strongest |
| **Use Case** | Metrics, logs | General purpose | Critical state |

## Examples

### Example 1: Highly Available Account Service

```python
accounts = await system.spawn(
    Account,
    name="accounts",
    sharded=True,
    replication=Replication(factor=3, consistency=Quorum()),
)

# Each account replicated to 3 nodes
# Survives any single node failure
# Waits for 2/3 replicas before returning

await accounts["acc-123"].send(Deposit(amount=100))
balance = await accounts["acc-123"].ask(GetBalance())
```

### Example 2: Critical Leader Election

```python
leader = await system.spawn(
    Leader,
    singleton=True,
    replication=Replication(factor=2, consistency=Strong()),
)

# Leader + 1 hot backup
# Waits for both before committing
# Strongest possible consistency

await leader.send(UpdateClusterState(...))
```

### Example 3: Log Service (Fire-and-Forget)

```python
logs = await system.spawn(
    LogService,
    name="logs",
    sharded=True,
    replication=Replication(factor=3, consistency=Eventual()),
)

# Replicas updated asynchronously
# Returns immediately
# Best for high-volume logging

await logs["service-a"].send(Log(message="..."))
```

### Example 4: No Replication (Default)

```python
temp = await system.spawn(
    TempProcessor,
    name="temp",
    # replication defaults to Replication(factor=1, consistency=Eventual())
)

# No replication
# Single node only
# Fails if node dies
```

## Default Behavior

If `replication` parameter is not provided:

```python
# Equivalent to:
replication=Replication(factor=1, consistency=Eventual())
# = No replication
```

## Error Handling

### Write Timeout

If not enough replicas acknowledge in time:

```python
try:
    # Might timeout if cluster degraded
    await accounts["acc-1"].send(UpdateBalance(100))
except asyncio.TimeoutError:
    # Handle timeout (write may have been stored as hint)
    pass
```

### Quorum Unavailable

If cluster size < quorum requirement:

```python
# Replication factor 5, Quorum requires 3 nodes
# If only 2 nodes alive → writes fail
# Decrease factor or use Eventual() for degraded mode
```

## Best Practices

1. **Choose consistency wisely**
   - `Eventual()` for logs, metrics, events
   - `Quorum()` for general business logic
   - `Strong()` for critical state only

2. **Size replication factor for your SLA**
   - `factor=2`: Survive 1 node failure
   - `factor=3`: Survive 1 node failure + maintenance
   - `factor=5`: Survive 2 concurrent failures (Quorum)

3. **Monitor hinted handoff**
   - Max 1000 hints per node by default
   - High hint count = slow recovery
   - Consider faster node recovery strategies

4. **Use sharding for scale**
   - Distribute load with `sharded=True`
   - Each entity replicated independently
   - No hot spots

5. **Test failover scenarios**
   - Kill nodes during operation
   - Verify automatic recovery
   - Monitor performance impact

## Architecture

```
ClusteredActorSystem
  └─ Cluster Actor
      ├─ SwimCoordinator (failure detection)
      ├─ GossipManager (state sync)
      ├─ TransportMux (networking)
      └─ ReplicationCoordinator (replication)
          ├─ Tracks metadata per type
          ├─ Coordinates parallel writes
          ├─ Manages quorum tracking
          └─ Handles hinted handoff
```

## See Also

- [Main Documentation](../README.md)
- [Example 6: Replication](../examples/06-replication.py)
- [API Reference](../casty/cluster/__init__.py)
