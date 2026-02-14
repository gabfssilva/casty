# Cluster Client

Not every process needs to join the cluster. A web server, an API gateway, or a CLI tool might need to talk to cluster actors without hosting any itself. **ClusterClient** connects to a Casty cluster as an external observer — it receives topology updates and gives you three capabilities:

- **Route messages to sharded entities** — zero hops, no proxy overhead.
- **Discover services** — `lookup()` reads from the locally cached topology, no network round-trip.
- **Request-reply** — `ask()` creates a temporary ref so the cluster can respond directly.

No cluster membership, no gossip participation, no failure detection overhead.

```python
async with ClusterClient(
    contact_points=[("10.0.1.10", 25520), ("10.0.1.11", 25520)],
    system_name="my-cluster",
) as client:
    # Sharded entities
    counter = client.entity_ref("counter", num_shards=100)
    counter.tell(ShardEnvelope("user-42", Increment(1)))

    # Request-reply
    count = await client.ask(
        counter,
        lambda r: ShardEnvelope("user-42", GetCount(reply_to=r)),
    )

    # Service discovery
    listing = client.lookup(PAYMENT_KEY)
    for instance in listing.instances:
        instance.ref.tell(ProcessPayment(amount=100))
```

## How It Works

On startup, `ClusterClient` opens a TCP connection (with `client_only=True` — no server socket) and spawns a topology subscriber actor that sends `SubscribeTopology` to the first reachable contact point. From that point on, the cluster pushes `TopologySnapshot` updates whenever membership, shard allocations, or the service registry changes.

Replies travel back through the same bidirectional TCP connection — the client doesn't need to be reachable from the cluster.

## Sharded Entities

Each call to `entity_ref(shard_type, num_shards=N)` creates a local proxy actor for that shard type. The proxy:

1. Receives `TopologySnapshot` updates from the subscriber and caches shard-to-node mappings.
2. When a `ShardEnvelope` arrives, hashes the entity ID to a shard and routes directly to the owning node's region actor over TCP.
3. On cache miss, queries the coordinator on the leader node via `GetShardLocation` and buffers messages until the response arrives.

Subsequent calls with the same `shard_type` return the cached proxy — no duplicate actors.

```python
counter = client.entity_ref("counter", num_shards=100)
counter.tell(ShardEnvelope("user-42", Increment(1)))
counter.tell(ShardEnvelope("user-99", Increment(5)))
```

## Service Discovery

`ClusterClient` can discover services registered with `Behaviors.discoverable()`. The `TopologySnapshot` carries the full service registry, so `lookup()` is a synchronous local read:

```python
PAYMENT_KEY: ServiceKey[PaymentMsg] = ServiceKey("payment")

listing = client.lookup(PAYMENT_KEY)
for instance in listing.instances:
    instance.ref.tell(ProcessPayment(amount=100))
```

If no topology has been received yet (e.g. called right after startup), `lookup()` returns an empty `Listing`. Once the first snapshot arrives, all registered services become visible.

You can also use `ask()` through discovered services:

```python
result = await client.ask(
    next(iter(listing.instances)).ref,
    lambda r: GetStatus(reply_to=r),
)
```

See [Service Discovery](service-discovery.md) for how `Behaviors.discoverable()` and the receptionist work.

## Request-Reply

`client.ask()` creates a temporary remotely-addressable ref so the cluster can respond directly via the existing TCP connection:

```python
balance = await client.ask(
    accounts,
    lambda r: ShardEnvelope("alice", GetBalance(reply_to=r)),
    timeout=5.0,
)
```

The temporary ref is cleaned up after the response or timeout. Works with both sharded entities and discovered services.

## Fault Tolerance

The client includes a TCP circuit breaker. When a send to a node fails:

1. The proxy evicts all shard allocations routed to that node.
2. Failed shards are retried after a short delay — the proxy re-queries the coordinator for updated locations.
3. If no topology update arrives within the liveness window (10 seconds by default), the subscriber reconnects to the next contact point.

The subscriber also enriches its contact list from `TopologySnapshot` — if a seed node goes down, it can still reconnect through any other cluster member it has seen.

## SSH Tunnels

When the cluster lives in a private network (e.g. AWS VPC), the client can reach it through SSH forward tunnels. **Reverse tunnels are not needed** — replies travel back through the same TCP connection the client opened.

### Setup

Set up forward tunnels to each cluster node:

```bash
ssh -N \
  -L 25520:10.0.1.10:25520 \
  -L 25521:10.0.1.11:25520 \
  -L 25522:10.0.1.12:25520 \
  -i ~/.ssh/key ec2-user@bastion.example.com
```

Then configure `ClusterClient` with an `address_map` that translates cluster-internal addresses to local tunnel endpoints:

```python
async with ClusterClient(
    contact_points=[("10.0.1.10", 25520), ("10.0.1.11", 25520)],
    system_name="my-cluster",
    client_host="127.0.0.1",
    client_port=0,
    address_map={
        ("10.0.1.10", 25520): ("127.0.0.1", 25520),
        ("10.0.1.11", 25520): ("127.0.0.1", 25521),
        ("10.0.1.12", 25520): ("127.0.0.1", 25522),
    },
) as client:
    counter = client.entity_ref("counter", num_shards=100)
    count = await client.ask(
        counter,
        lambda r: ShardEnvelope("user-42", GetCount(reply_to=r)),
    )
```

### How it works

- **`address_map`** translates logical cluster addresses to local tunnel endpoints. When the client sends to `10.0.1.10:25520`, the TCP layer connects to `127.0.0.1:25520` instead — the SSH forward tunnel delivers it to the actual node.
- **`client_only=True`** (internal default) means the client opens outbound connections only. No server socket, no inbound port needed.
- Replies arrive on the same TCP connection, so no reverse tunnel or `advertised_host`/`advertised_port` is required.
- If `address_map` doesn't contain an address, the client connects directly (identity fallback).

## Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `contact_points` | *(required)* | List of `(host, port)` for cluster nodes |
| `system_name` | *(required)* | Must match the cluster's actor system name |
| `client_host` | `"127.0.0.1"` | Local bind address |
| `client_port` | `0` | Local bind port (`0` for auto-assignment) |
| `address_map` | `None` | `dict[tuple[str, int], tuple[str, int]]` — logical-to-tunnel address mapping |
| `advertised_host` | `None` | Override host in reply addresses (for advanced network setups) |
| `advertised_port` | `None` | Override port in reply addresses (for advanced network setups) |

## When to Use

| Scenario | Use |
|----------|-----|
| Process hosts sharded entities or needs leader election | `ClusteredActorSystem` |
| Process sends work to sharded entities from outside | `ClusterClient` |
| Process needs to discover services without joining the cluster | `ClusterClient` |
| Process needs `Subscribe` for live service updates | `ClusteredActorSystem` |
| Process needs request-reply with cluster actors | `ClusterClient` |

---

**Next:** [Cluster Backend](cluster-backend.md)
