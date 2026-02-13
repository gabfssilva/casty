# Cluster Sharding

A single process has limits — memory, CPU, network connections. When a system manages millions of entities (bank accounts, user sessions, IoT sensors), no single machine can host them all. **Cluster sharding** solves this by partitioning entities across multiple nodes and routing messages transparently.

Consider a network of 100,000 temperature sensors reporting readings every second. Each sensor is modeled as an actor that aggregates its readings. No single machine can host all 100,000 actors — but if they are partitioned into 256 shards distributed across 8 nodes, each node manages roughly 12,500 sensors. When a sensor reports a reading, the cluster routes the message to whichever node owns that sensor's shard. If nodes are added or removed, shards are rebalanced automatically.

Cluster sharding has three components:

- **Shards.** A logical partition of the entity space. Entity IDs are hashed deterministically to a shard number (Casty uses MD5). The number of shards is fixed at creation time and should be significantly larger than the expected number of nodes — this ensures rebalancing is granular (individual shards move between nodes, not entire ranges).
- **Coordinator.** Decides which node owns which shard. Uses a least-shard-first allocation strategy: when a shard is accessed for the first time, it is assigned to the node currently hosting the fewest shards. The coordinator is replicated across the cluster with a leader/follower topology — the leader makes allocation decisions, followers cache allocations and forward unknown shards to the leader.
- **Region.** Each node runs a region that manages local entities. When a message arrives for a shard owned by this node, the region spawns the entity actor (if it doesn't exist yet) and delivers the message. Messages for shards owned by other nodes are forwarded over the network.

Underneath the sharding layer, a single **topology actor** owns all cluster membership — it runs the gossip protocol for state propagation, a **phi accrual failure detector** (Hayashibara et al.) for identifying unresponsive nodes, and **vector clocks** for resolving conflicting state during network partitions. Cluster consumers (coordinator, receptionist, singleton managers) self-subscribe via `SubscribeTopology` and receive `TopologySnapshot` pushes whenever the cluster state changes.

```python
@dataclass(frozen=True)
class Deposit:
    amount: int

@dataclass(frozen=True)
class GetBalance:
    reply_to: ActorRef[int]

type AccountMsg = Deposit | GetBalance

def account_entity(entity_id: str) -> Behavior[AccountMsg]:
    def active(balance: int) -> Behavior[AccountMsg]:
        async def receive(ctx: ActorContext[AccountMsg], msg: AccountMsg) -> Behavior[AccountMsg]:
            match msg:
                case Deposit(amount):
                    return active(balance + amount)
                case GetBalance(reply_to):
                    reply_to.tell(balance)
                    return Behaviors.same()

        return Behaviors.receive(receive)

    return active(0)

async def main() -> None:
    async with (
        ClusteredActorSystem(
            name="bank", host="127.0.0.1", port=25520,
            node_id="node-1", seed_nodes=[("127.0.0.1", 25521)],
        ) as node1,
        ClusteredActorSystem(
            name="bank", host="127.0.0.1", port=25521,
            node_id="node-2", seed_nodes=[("127.0.0.1", 25520)],
        ) as node2,
    ):
        accounts1 = node1.spawn(Behaviors.sharded(account_entity, num_shards=100), "accounts")
        accounts2 = node2.spawn(Behaviors.sharded(account_entity, num_shards=100), "accounts")
        await asyncio.sleep(0.3)

        # Deposit via node 1
        accounts1.tell(ShardEnvelope("alice", Deposit(100)))
        accounts1.tell(ShardEnvelope("bob", Deposit(200)))
        await asyncio.sleep(0.3)

        # Deposit via node 2 — routes to the correct node transparently
        accounts2.tell(ShardEnvelope("alice", Deposit(50)))
        await asyncio.sleep(0.3)

        # Query from either node
        balance = await node1.ask(
            accounts1,
            lambda r: ShardEnvelope("alice", GetBalance(reply_to=r)),
            timeout=2.0,
        )
        print(f"Alice's balance: {balance}")  # Alice's balance: 150

asyncio.run(main())
```

Every node in the cluster runs the same code — only `host`, `port`, and `node_id` differ. Each node has a human-readable `node_id` (e.g. `"node-1"`, `"worker-east"`) that is propagated through the topology actor and can be used with `system.lookup("/actor", node="node-1")` to address actors on specific nodes. `ShardEnvelope(entity_id, message)` wraps a message with the entity ID for routing. The proxy actor on each node caches shard-to-node mappings and forwards messages to the correct region, whether local or remote.

`ClusteredActorSystem` extends `ActorSystem`. Spawning a `ShardedBehavior` (produced by `Behaviors.sharded()`) returns an `ActorRef[ShardEnvelope[M]]`. All other spawns work identically to the local `ActorSystem`. This means existing actors that don't need distribution require no changes when moving to a clustered deployment.

---

**Next:** [Cluster Broadcast](cluster-broadcast.md)
