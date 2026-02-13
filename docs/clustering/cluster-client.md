# Cluster Client

Not every process needs to join the cluster. A web server, an API gateway, or a CLI tool might need to send work to sharded entities without hosting any itself. **ClusterClient** connects to a Casty cluster as an external observer — it receives topology updates, caches shard allocations, and routes `ShardEnvelope` messages directly to the owning node. Zero hops, no proxy overhead, no cluster membership.

```python
async with ClusterClient(
    contact_points=[("10.0.1.10", 25520), ("10.0.1.11", 25520)],
    system_name="my-cluster",
) as client:
    counter = client.entity_ref("counter", num_shards=100)
    counter.tell(ShardEnvelope("user-42", Increment(1)))

    count = await client.ask(
        counter,
        lambda r: ShardEnvelope("user-42", GetCount(reply_to=r)),
    )
    print(count)
```

## How It Works

On startup, `ClusterClient` opens a TCP listener (for receiving responses) and spawns a topology subscriber actor that sends `SubscribeTopology` to the first reachable contact point's `/_topology` actor. From that point on, the cluster pushes `TopologySnapshot` updates whenever membership, shard allocations, or leadership changes.

Each call to `entity_ref(shard_type, num_shards=N)` creates a local proxy actor for that shard type. The proxy:

1. Receives `TopologySnapshot` updates from the subscriber and caches shard-to-node mappings.
2. When a `ShardEnvelope` arrives, hashes the entity ID to a shard and routes directly to the owning node's region actor over TCP.
3. On cache miss (new shard not yet in the allocation table), queries the coordinator on the leader node via `GetShardLocation` and buffers messages until the response arrives.

Subsequent calls with the same `shard_type` return the cached proxy — no duplicate actors.

## Request-Reply

`client.ask()` creates a temporary remotely-addressable ref so the cluster can respond directly to the client via TCP:

```python
balance = await client.ask(
    accounts,
    lambda r: ShardEnvelope("alice", GetBalance(reply_to=r)),
    timeout=5.0,
)
```

The temporary ref is cleaned up after the response or timeout.

## Service Discovery

`ClusterClient` can discover services registered with `Behaviors.discoverable()` — no cluster membership required. The `TopologySnapshot` already carries the service registry, so `lookup()` reads from the locally cached snapshot with no network round-trip:

```python
PAYMENT_KEY: ServiceKey[PaymentMsg] = ServiceKey("payment")

async with ClusterClient(
    contact_points=[("10.0.1.10", 25520)],
    system_name="my-cluster",
) as client:
    listing = client.lookup(PAYMENT_KEY)
    for instance in listing.instances:
        instance.ref.tell(ProcessPayment(amount=100))

    # Request-reply through a discovered service
    result = await client.ask(
        next(iter(listing.instances)).ref,
        lambda r: GetStatus(reply_to=r),
    )
```

`lookup()` is synchronous — it returns a `Listing[M]` immediately from the cached topology. If no topology has been received yet (e.g. called right after startup), it returns an empty `Listing`.

## Fault Tolerance

The client includes a TCP circuit breaker. When a send to a node fails:

1. The proxy evicts all shard allocations routed to that node.
2. Failed shards are retried after a short delay — the proxy re-queries the coordinator for updated locations.
3. If no topology update arrives within the liveness window (10 seconds by default), the subscriber reconnects to the next contact point.

The subscriber also enriches its contact list from `TopologySnapshot` — if a seed node goes down, it can still reconnect through any other cluster member it has seen.

## SSH Tunnels

When the cluster lives in a private network (e.g. AWS VPC), the client can reach it through SSH tunnels. `ClusterClient` supports this with two parameters: `address_map` for forward tunnels and `advertised_host`/`advertised_port` for reverse tunnels.

### Setup

Set up SSH tunnels to the bastion host:

```bash
# Forward tunnels — reach each cluster node
ssh -L 12345:10.0.1.10:25520 \
    -L 12346:10.0.1.11:25520 \
    -R 9999:localhost:5000 \    # Reverse tunnel — cluster reaches the client
    user@bastion.example.com
```

Then configure `ClusterClient` with the mapping:

```python
async with ClusterClient(
    contact_points=[("10.0.1.10", 25520), ("10.0.1.11", 25520)],
    system_name="my-cluster",
    client_host="127.0.0.1",
    client_port=5000,
    advertised_host="bastion.example.com",
    advertised_port=9999,
    address_map={
        ("10.0.1.10", 25520): ("localhost", 12345),
        ("10.0.1.11", 25520): ("localhost", 12346),
    },
) as client:
    counter = client.entity_ref("counter", num_shards=100)
    count = await client.ask(
        counter,
        lambda r: ShardEnvelope("user-42", GetCount(reply_to=r)),
    )
```

### How it works

- **`address_map`** translates logical cluster addresses to local tunnel endpoints. When the client sends to `10.0.1.10:25520`, the TCP layer connects to `localhost:12345` instead — the SSH forward tunnel delivers it to the actual node.
- **`advertised_host`/`advertised_port`** go into reply `ActorRef` addresses. The cluster sends responses to `bastion.example.com:9999`, which the SSH reverse tunnel delivers back to `localhost:5000`.
- **`client_host`/`client_port`** remain the local bind address for the TCP listener.
- If `address_map` doesn't contain an address, the client connects directly (identity fallback). This means topology-reported addresses that aren't in the map are used as-is.

## Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `contact_points` | *(required)* | List of `(host, port)` for cluster nodes |
| `system_name` | *(required)* | Must match the cluster's actor system name |
| `client_host` | `"127.0.0.1"` | Bind address for the local TCP listener |
| `client_port` | `0` | Bind port (`0` for auto-assignment) |
| `advertised_host` | `None` | Host in reply addresses (defaults to `client_host`) |
| `advertised_port` | `None` | Port in reply addresses (defaults to `client_port`) |
| `address_map` | `None` | `dict[tuple[str, int], tuple[str, int]]` — logical-to-actual address mapping |

## When to Use

| Scenario | Use |
|----------|-----|
| Process hosts sharded entities | `ClusteredActorSystem` |
| Process sends work but doesn't host entities | `ClusterClient` |
| Process needs leader election, singleton, or `Subscribe` for live updates | `ClusteredActorSystem` |
| Process needs to route messages or discover services | `ClusterClient` |

---

**Next:** [Cluster Backend](cluster-backend.md)
