# Clustered

In this guide you'll form a **two-node cluster** with sharded entities distributed across nodes. Along the way you'll learn how nodes discover each other via seed nodes, how `ShardEnvelope` routes messages to the right node, and how to query entities from anywhere in the cluster.

## Messages and Entity Factory

A simple counter entity. Each `entity_id` gets its own actor instance, placed on whichever node the shard coordinator assigns:

```python
--8<-- "examples/guides/06_clustered.py:24:53"
```

The factory receives the `entity_id` and returns a `Behavior`. Casty calls it lazily — the actor is only created when the first message for that entity arrives.

## Forming a Cluster

Two `ClusteredActorSystem` instances. Node 1 starts independently; node 2 joins via `seed_nodes`:

```python
--8<-- "examples/guides/06_clustered.py:59:73"
```

`wait_for(2)` blocks until both nodes reach `MemberStatus.up`. In production, each node runs in its own process — here we run both in one process for simplicity.

## Spawning Sharded Entities

Both nodes spawn the same shard type. The coordinator distributes shards across the cluster:

```python
--8<-- "examples/guides/06_clustered.py:76:85"
```

`num_shards=10` means entity IDs are hashed into 10 buckets. Each bucket is assigned to one node. You send messages through the local proxy — routing to the correct node is transparent.

## Routing and Querying

Messages are wrapped in `ShardEnvelope(entity_id, message)`. The proxy routes based on entity ID:

```python
--8<-- "examples/guides/06_clustered.py:88:107"
```

Notice: all queries go through `proxy1` on node 1, but entities may live on node 2. The proxy handles the cross-node routing transparently — you never need to know which node owns which shard.

## Running It

```python
--8<-- "examples/guides/06_clustered.py:113:127"
```

Output:

```
── Cluster formed (2 nodes) ──

── Sending increments ──
  [alice] 0 + 10 = 10
  [alice] 10 + 5 = 15
  [bob] 0 + 100 = 100
  [carol] 0 + 42 = 42

── Querying balances ──
  alice: 15
  bob: 100
  carol: 42
```

## Run the Full Example

```bash
git clone https://github.com/gabfssilva/casty.git
cd casty
uv run python examples/guides/06_clustered.py
```

---

**What you learned:**

- **`ClusteredActorSystem`** extends `ActorSystem` with cluster membership and shard routing.
- **Seed nodes** bootstrap cluster formation — node 2 connects to node 1's address to join.
- **`Behaviors.sharded(factory, num_shards=N)`** distributes entities across nodes by hashing the entity ID.
- **`ShardEnvelope(entity_id, msg)`** routes messages to the correct node transparently.
- **`wait_for(n)`** blocks until the cluster has enough members — useful for quorum requirements.
