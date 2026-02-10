# Shard Replication

Sharding solves the capacity problem but introduces a new failure mode: if a node goes down, all entities hosted on that node become unavailable. **Replication** addresses this by maintaining passive copies of each entity on other nodes.

The primary processes commands and pushes persisted events to its replicas after each command. Replicas maintain their own copy of the event journal and can be promoted to primary if the original node fails. This requires the entity to use event sourcing — replication operates on the event stream, not on raw state.

The `min_acks` parameter controls the consistency/latency trade-off:

| `min_acks` | Behavior |
|------------|----------|
| `0` | Fire-and-forget replication. Lowest latency, but recent events may be lost if the primary fails before replication completes. |
| `1+` | The primary waits for N replica acknowledgments before confirming the command. Stronger durability at the cost of increased latency. |

```python
accounts = node.spawn(
    Behaviors.sharded(
        account_entity,
        num_shards=100,
        replication=ReplicationConfig(
            replicas=2,        # 2 passive replicas per entity
            min_acks=1,        # wait for 1 replica ack before confirming
            ack_timeout=5.0,   # seconds to wait for acks
        ),
    ),
    "accounts",
)
```

The coordinator allocates primary and replicas on different nodes. When a node fails, the failure detector triggers a `NodeDown` event, and the coordinator promotes the replica with the highest event sequence number to primary. With three nodes and `replicas=2`, every entity has a primary on one node and replicas on the other two — the cluster tolerates the loss of any single node without data loss.

---

**Next:** [Distributed Data Structures](distributed-data-structures.md)
