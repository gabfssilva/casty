# casty

A typed, distributed actor framework for Python.

Casty allows you to create actors as plain classes: annotated fields are their state, async methods are their interface. It activates each actor on demand across a leaderless cluster, routes calls to it by key from any node, replicates its state, and survives node failure — while your code stays ordinary typed Python.

```python
import asyncio
import casty


@casty.actor
class Greeter:
    greetings: int = 0

    async def greet(self, who: str) -> str:
        self.greetings += 1
        return f"hello, {who}! (greeting #{self.greetings})"


async def main() -> None:
    node = await casty.start("127.0.0.1:7101")

    greeter = node.actor(Greeter, "front-door")
    print(await greeter.greet("world"))

    # same key -> same activation, from any node in the cluster
    print(await node.actor(Greeter, "front-door").greet("again"))

    await node.close()


asyncio.run(main())
```

The proxy is statically typed: `greeter.greet` has the signature of `Greeter.greet`, and the whole API type-checks under mypy strict.

## Installation

```sh
pip install casty
```

Requires Python 3.12+. Runtime dependency: `msgpack`. Optional compression codecs beyond the built-in zlib:

```sh
pip install "casty[lz4]"    # or casty[zstd]
```

## Three ways to run

```python
system = casty.local()                                 # in-process: no networking, no cluster
node = await casty.start("0.0.0.0:7001", seeds=[...])  # full cluster member: hosts actors
client = await casty.connect(["10.0.0.1:7001"])        # lite member: routes calls, hosts nothing
```

All three share the same API (`actor()`, `map()`, `close()`) and work as async context managers. `casty.local()` is a complete in-process actor system — mailboxes, lifecycle, supervision — for single-process applications that want the actor model without the cluster. A lite member joins the membership and knows the ring, so it routes calls in one hop, but never owns keys.

## Actors and state

Annotated class fields are the actor's state — the source of truth for snapshots and replication. Fields that shouldn't be persisted (connections, caches) are marked `transient` and rebuilt on activation:

```python
@casty.actor(idle_timeout=300.0)
class Session:
    visits: int = 0                                        # replicated state
    cache: dict[str, str] = casty.transient(factory=dict)  # rebuilt each activation

    @casty.activate
    async def _open(self) -> None:
        self.cache = await load_cache(casty.context().key)

    @casty.deactivate
    async def _close(self) -> None:
        ...  # runs on idle timeout, ctx.deactivate(), or node shutdown

    async def visit(self) -> int:
        self.visits += 1
        return self.visits
```

`@casty.activate` runs on every activation, including reactivation after the actor migrates to another node (state already restored). Inside a handler, `casty.context()` exposes the actor's own key, and `ctx.deactivate()` schedules deactivation after the current message.

Concurrency is the classic actor guarantee: one handler at a time per actor, FIFO mailbox. Reentrancy (an ask cycle A→B→A) is detected and raises `ReentrancyError` instead of deadlocking.

## Messages

Arguments and return values must be serializable. Built-ins (numbers, strings, lists, dicts, ...) work as-is; your own types are declared with `@casty.message`:

```python
@casty.message
class Reservation:
    order_id: str
    qty: int
```

`@casty.message` turns the class into a slotted dataclass and registers it under a stable wire name (default `module.QualName`, overridable with `name=...`). Serializability is validated recursively at import time, not at send time. Schema evolution is tolerant in both directions: new fields with defaults are accepted by old receivers, unknown fields are ignored.

## Clustering

Nodes are homogeneous — there is no leader. Start one node, point the others at it:

```python
node_a = await casty.start("10.0.0.1:7001")
node_b = await casty.start("10.0.0.2:7001", seeds=["10.0.0.1:7001"])
node_c = await casty.start("10.0.0.3:7001", seeds=["10.0.0.1:7001"])
```

Every key is owned by exactly one node, determined by a token ring that every member computes locally — placement needs no coordinator and no extra hops. Calls route directly to the owner regardless of which node dispatches them.

When a node joins or leaves, only the affected key ranges move. `node.close()` is graceful: it drains in-flight handlers, hands its ranges off to the new owners, and announces a clean leave — a rolling restart doesn't stampede the cluster.

TLS (including mTLS) is one argument away:

```python
node = await casty.start(
    "0.0.0.0:7001",
    seeds=[...],
    tls=casty.TLS(cert="node.pem", key="node.key", ca="ca.pem"),
)
```

## Replication and consistency

By default an actor lives in a single copy: fast, and its state dies with its node. Classes that need durability declare replication:

```python
@casty.actor(replicas=3, write=casty.MAJORITY)
class StockItem:
    on_hand: int = 0

    async def restock(self, qty: int) -> int:
        self.on_hand += qty
        return self.on_hand
```

After every handler that mutates state, the owner snapshots the annotated fields, stamps them with a hybrid logical clock, and pushes to the backup replicas; the caller gets its answer once `write` replicas have acknowledged. `write` and `read` take `casty.ONE`, `casty.MAJORITY`, `casty.ALL`, or an integer. `read=casty.MAJORITY` reads from a quorum and repairs stale replicas along the way.

If the owning node dies, the next call activates the actor elsewhere from the newest committed snapshot. Split-brain is handled by fencing: an owner that cannot reach a write quorum rejects writes with `QuorumUnavailableError` rather than diverging.

## Failure handling

Two failure domains, kept apart:

**Infrastructure failures** — timeout, unreachable owner, no quorum, a range mid-migration — surface as typed exceptions on the *caller*: `CastyTimeoutError`, `ActorUnavailableError`, `QuorumUnavailableError`, `RangeMovingError`, `UnknownActorTypeError`. Retries happen automatically only where provably safe (re-routing after a view change); everything else propagates.

**Actor failures** — an exception inside a handler — are decided by the *supervisor*, a plain function:

```python
def three_strikes(
    cls: type, key: str, exc: Exception, ctx: casty.FailureContext
) -> casty.Directive:
    return casty.KEEP if ctx.failures < 3 else casty.RESET


node = await casty.start("0.0.0.0:7001", supervisor=three_strikes)
```

- `KEEP` (default): state survives; the caller gets `ActorFailedError` wrapping the original exception.
- `RESET`: the activation is discarded; the next call starts from the last replicated snapshot (or fresh).
- `STOP`: graceful deactivation; the next call reactivates.

The global supervisor can be overridden per class: `@casty.actor(supervisor=...)`.

## Distributed map

`casty.Map` is a distributed key-value map built as sugar over the same machinery — partitioned into shard actors, each placed and replicated like any other actor:

```python
prices: casty.Map[str, float] = node.map("prices", replicas=3, write=casty.MAJORITY)

await prices.put("sku-1234", 49.90)
print(await prices.get("sku-1234"))
print(await prices.size())
```

## How it works

Full protocol specs live in [`docs/specs/`](docs/specs/); the short version:

**Membership** ([spec](docs/specs/02-membership.md)) — HyParView keeps a small active view of TCP connections per node plus a larger passive view as standby, which keeps the overlay connected even under massive failure. Plumtree broadcasts cluster events (join, leave, death) over that overlay, and each node assembles the full member list locally, repaired by periodic anti-entropy. Failure detection is neighbor-based with SWIM-style suspicion and refutation.

**Placement** ([spec](docs/specs/03-placement.md)) — a token ring with vnodes (Cassandra/Riak style): each node takes many positions on a hash ring, a key belongs to the first token at or after its hash, and replicas are the next distinct physical nodes along the ring. The ring is a pure function of the member list, so every node resolves any key to its owner locally, in zero hops.

**Replication** ([spec](docs/specs/05-replication.md)) — primary-based, single writer per key. The owner coordinates writes and stamps each key with a hybrid logical clock; activation handshakes with a replica quorum, so a node failure or a divergent view during churn resolves to the newest committed state. Exactly-one activation is eventual, not absolute — the tradeoff for having no consensus protocol.

**Transport** ([spec](docs/specs/01-transport.md)) — a multiplexed binary protocol over TCP (yamux-style): independent streams with credit-based flow control on one connection, so a multi-GB state handoff never blocks actor calls. Compression is negotiated per connection and only applied above a size threshold. The cluster overlay is control-plane only; actor calls use direct on-demand connections between caller and owner.

## Examples

Runnable, numbered walkthroughs in [`examples/`](examples/):

1. [Hello world](examples/01_hello_world.py) — one node, one actor, one call
2. [Lifecycle](examples/02_lifecycle.py) — transient state, hooks, idle deactivation
3. [Cluster](examples/03_cluster.py) — three nodes, ring placement, lite member
4. [Supervision](examples/04_supervision.py) — KEEP / RESET / STOP with a custom policy
5. [Replicated inventory](examples/05_replicated_inventory.py) — quorum writes surviving node loss, distributed map

```sh
uv run python examples/01_hello_world.py
```
