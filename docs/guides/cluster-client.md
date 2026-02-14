# Client + Cluster

In this guide you'll connect to a running cluster **from outside** using `ClusterClient`. Along the way you'll learn how the client discovers topology, how `entity_ref` creates a local proxy for routing, and how `ask()` enables request-reply from external code.

## When to Use ClusterClient

`ClusteredActorSystem` joins the cluster as a full member — it runs actors, owns shards, participates in gossip. `ClusterClient` is for code that *uses* the cluster without joining it: API servers, CLI tools, monitoring dashboards. It connects via TCP, subscribes to topology updates, and routes messages directly to the node owning each shard.

## Messages and Entity Factory

A loyalty points system. Each user is a sharded entity:

```python
--8<-- "examples/guides/07_cluster_client.py:25:54"
```

## Starting the Cluster

A single-node cluster with a sharded "loyalty" entity type:

```python
--8<-- "examples/guides/07_cluster_client.py:63:78"
```

## Connecting from Outside

`ClusterClient` takes a list of contact points and the cluster's system name:

```python
--8<-- "examples/guides/07_cluster_client.py:81:88"
```

The client connects to the contact point, subscribes to `TopologySnapshot` updates, and caches shard allocations locally. `entity_ref("loyalty", num_shards=NUM_SHARDS)` creates a local proxy actor that routes `ShardEnvelope` messages to the cluster.

## Fire and Forget

`tell()` works the same as inside the cluster — wrap the message in `ShardEnvelope`:

```python
--8<-- "examples/guides/07_cluster_client.py:90:95"
```

## Request-Reply

`client.ask()` creates a temporary ref, sends the message, and waits for the response:

```python
--8<-- "examples/guides/07_cluster_client.py:97:107"
```

The reply travels back through the existing TCP connection — no extra server socket or reverse tunnel needed.

## Running It

```python
--8<-- "examples/guides/07_cluster_client.py:63:110"
```

Output:

```
── Cluster running ──

── Sending points ──
  [user-1] +100 points (total: 100)
  [user-1] +50 points (total: 150)
  [user-2] +200 points (total: 200)

── Querying points ──
  user-1: 150 points
  user-2: 200 points
```

## Run the Full Example

```bash
git clone https://github.com/gabfssilva/casty.git
cd casty
uv run python examples/guides/07_cluster_client.py
```

---

**What you learned:**

- **`ClusterClient`** connects to a cluster without joining it — topology-aware routing with zero membership.
- **`entity_ref(shard_type, num_shards=N)`** returns a cached proxy that routes `ShardEnvelope` to the correct node.
- **`client.ask()`** enables request-reply from outside the cluster using temporary refs.
- **Fault tolerance** is built in — the client rotates contact points on timeout and caches topology locally.
