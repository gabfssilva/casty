# casty

Typed, replicated virtual actors for Python.

casty is an actor library for Python 3.13+, with a core written in Rust. An actor is an `async` function that owns the
state of one key and handles its messages one at a time. The same code runs in one process or across a cluster, where
casty places each key on a node, routes messages to it, replicates its state, and moves the key to another node when
its node fails.

## Contents

- [Installation](#installation)
- [Quick start](#quick-start)
- [Guide](#guide)
- [Clusters](#clusters)
- [Collections](#collections)
- [Reference](#reference)
- [How it works](#how-it-works)
- [Guarantees and limits](#guarantees-and-limits)
- [Development](#development)

## Installation

```sh
uv add casty        # or: pip install casty
```

casty needs Python 3.13 or later (3.14 and free-threaded 3.14t included). The wheels use the stable ABI, so one wheel
per platform covers every supported Python version, and there are no Python dependencies.

On a platform without a wheel, the install builds from source and needs a Rust toolchain, 1.90 or later. The same
applies to installing from a checkout:

```sh
uv add ./casty      # or: pip install ./casty
uv build --wheel    # dist/casty-<version>-cp313-abi3-<platform>.whl
```

## Quick start

```python
import asyncio
from dataclasses import dataclass

from casty import ActorSystem, Context, Ref, actor


@dataclass(frozen=True)
class Greet:
    reply_to: Ref[str]
    name: str


@actor(initial=0)
async def greeter(ctx: Context[int, Greet]) -> None:
    async for msg in ctx.inbox:
        count = await ctx.state.update(lambda count: count + 1)
        msg.reply_to.tell(f"hello, {msg.name}! greeting #{count} from {ctx.key}")


async def main() -> None:
    async with ActorSystem() as system:
        ana = system.ref(greeter, "ana")
        print(await ana.ask(Greet, "world"))  # hello, world! greeting #1 from ana
        print(await ana.ask(Greet, "again"))  # hello, again! greeting #2 from ana
        print(await system.ref(greeter, "bia").ask(Greet, "world"))  # greeting #1 from bia


asyncio.run(main())
```

- `greeter` is an actor type. `Context[int, Greet]` declares its state type and its message type.
- `"ana"` and `"bia"` are keys. Each key is an entity with its own state and mailbox.
- `system.ref(greeter, "ana")` is the address of the entity. Obtaining it creates the key if it does not exist.
- `ana.ask(Greet, "world")` sends `Greet(reply_to, "world")` and waits for what the actor tells `reply_to`.

## Guide

### Actors, keys and refs

An actor is an entity identified by `(actor type, key)`. There is no `spawn`, no hierarchy and no actor object: you
name the entity and send it a message.

A key is activated when its ref is obtained or a message arrives: casty loads its state and runs the body, which is
the decorated function. `ctx.inbox` ends after `idle_after` without messages (one minute by default); when the body
returns, the key is deactivated and its state stays stored. The next ref or message activates it again from the saved
state.

- Only active keys use a task and memory for a body.
- Local variables do not survive deactivation, a restart or a move to another node. What must survive goes in the
  state.
- A key handles one message at a time, so a body needs no locks.

### tell and ask

```python
ref.tell(Deposit(50))
ok = await ref.ask(Withdraw, 30)
```

`tell` does not block and does not raise because of the receiver. Delivery is at most once.

`ask` takes the message class and its remaining arguments, builds the message with a reply ref as the first argument,
and waits for the value told to that ref. The first field must be annotated `Ref[R]`: the checker infers the return
type of `ask` from it, and casty decodes the answer with it.

```python
@dataclass(frozen=True)
class Withdraw:
    reply_to: Ref[bool]
    amount: int
```

`ask` raises `TimeoutError` after the system's `ask_timeout` (10 seconds by default). Keyword arguments go to the
message, so a shorter deadline is set with `asyncio.timeout`. A message may still be processed after its `ask` timed
out.

### State

`ctx.state` is a `State[S]`:

- `ctx.state.value` is the last saved state.
- `await ctx.state.set(new)` replaces it, and returns once the write level of the type has confirmed it (see
  [Replication](#replication-and-write-levels)). `value` changes only then.
- `await ctx.state.update(change)` saves what `change` makes of the current state and returns it. `change` is a
  function, sync or `async`, from the state to the new state.

```python
@actor(initial=0)
async def account(ctx: Context[int, AccountMsg]) -> None:
    async for msg in ctx.inbox:
        match msg:
            case Deposit(reply_to, amount):
                reply_to.tell(await ctx.state.update(lambda balance: balance + amount))
            case Withdraw(reply_to, amount) if amount <= ctx.state.value:
                await ctx.state.set(ctx.state.value - amount)
                reply_to.tell(True)
            case Withdraw(reply_to, _):
                reply_to.tell(False)
```

State is kept in memory on the replicas. There is no disk persistence.

### Creating keys

`system.ref(actor, key)` is the only way to reach an entity, and it does not wait for anything. Obtaining a ref asks
the owner to create the key, if it does not exist, and to activate it. What goes wrong with that shows in the first
`ask`.

A key that does not exist starts from:

- `initial=` passed to `ref`, when there is one;
- otherwise the default of the type, `@actor(initial=...)`;
- otherwise `None`, if the state type allows it (`Context[Draft | None, M]`).

A type with none of the three cannot be created, and `ref` raises `TypeError`. Both checkers report that call.

```python
@actor
async def document(ctx: Context[str, Read]) -> None: ...


readme = system.ref(document, "readme", initial="# casty")
system.ref(document, "readme", initial="other")  # the key exists: its state is unchanged
system.ref(document, "readme")  # TypeError, and an error in pyright and mypy
```

`initial` only matters for a key that does not exist. When two callers pass different values, the first to reach the
owner decides.

### become

`ctx.become(behavior, state)` hands the key to another actor type that takes the same messages, so each state of a
state machine is a function of its own.

```python
@actor
async def paid(ctx: Context[Paid, OrderMsg]) -> None:
    async for msg in ctx.inbox:
        match msg:
            case Pay(reply_to, _):
                reply_to.tell("refused: already paid")
            case Ship(reply_to, tracking):
                reply_to.tell(f"shipped as {tracking}")


@actor
async def pending(ctx: Context[Pending, OrderMsg]) -> None:
    async for msg in ctx.inbox:
        match msg:
            case Pay(reply_to, amount):
                await ctx.become(paid, Paid(ctx.state.value.items, amount))
                reply_to.tell(f"paid {amount}")
            case Ship(reply_to, _):
                reply_to.tell("refused: not paid yet")
```

- The code after `become` still runs; the next message is read by the new behavior.
- The entity keeps its identity, `(pending, key)`, and every ref to it stays valid.
- The change is saved with the state, so it survives restarts and moves.
- The checker verifies that the state matches the new behavior. `become(other)` without a state requires the same
  state type and continues from the last saved state.
- `state.set` and `state.update` after `become` raise.

### Refs inside bodies

A `Ref` is a value: it can be a field of a message or of the state. `ctx.system` reaches any entity and `ctx.self` is
the ref of the entity itself.

```python
@actor(initial=0)
async def teller(ctx: Context[int, Transfer]) -> None:
    async for msg in ctx.inbox:
        source = ctx.system.ref(account, msg.source)
        target = ctx.system.ref(account, msg.target)
        if await source.ask(Withdraw, msg.amount):
            await target.ask(Deposit, msg.amount)
            await ctx.state.set(ctx.state.value + 1)
            msg.reply_to.tell(True)
        else:
            msg.reply_to.tell(False)
```

`ask` cycles are not detected; they end in `TimeoutError`.

### Streams and proactive actors

A body is ordinary asyncio code: `asyncio.sleep`, `asyncio.timeout`, `TaskGroup` and async iteration all work.

`ctx.merge(source)` yields mailbox messages and items of `source` in arrival order, until `source` ends. Idleness does
not end it, and an exception from `source` propagates to the body.

```python
@actor(initial=Window())
async def meter(ctx: Context[Window, Reading | Averages]) -> None:
    async for event in ctx.merge(every(0.5)):  # an async generator of Tick
        match event:
            case Reading(value):
                await ctx.state.set(Window((*ctx.state.value.readings, value), ctx.state.value.averages))
            case Tick():
                ...
            case Averages(reply_to):
                reply_to.tell(ctx.state.value.averages)
```

`Context[S]` without a message type declares an actor that takes no messages:

```python
@actor(initial=Cursor())
async def consumer(ctx: Context[Cursor]) -> None:
    async for record in broker.stream(ctx.key, ctx.state.value.offset + 1):
        await process(record)
        await ctx.state.set(Cursor(record.offset))


system.ref(consumer, "orders-0")  # creates the key and starts the body
```

An active key carries a mark in its replicated state. If its node dies, the node that takes the key over runs the body
again from the saved state, with no message sent. Work done before the state is saved is repeated after a crash: at
least once.

### Failures

| Situation | `ask` raises | Entity |
|---|---|---|
| The body raised while handling the message | `ActorFailed` (actor, key, exception class and text) | Drops the message, waits the backoff, restarts from the last saved state |
| A ref received in a message points at a key that was never created | `NotStarted` | |
| Bounded mailbox is full | `MailboxFull` | |
| Owner unreachable, too few replicas, or owner changing | `Unavailable` | The message may or may not have been processed |
| The owner's code does not have the actor type | `UnknownActor` | |
| No answer within `ask_timeout` | `TimeoutError` | The message may still be processed |

For `tell`, the same situations drop the message and log it.

The restart delay follows `Backoff`: `first`, multiplied by `factor` on each consecutive failure, up to `limit`. The
mailbox is kept across restarts.

Mailboxes are unbounded by default, so an actor slower than its senders accumulates messages in memory.
`@actor(mailbox=n)` bounds it; a full mailbox refuses the new message.

### Types

State, messages and replies are always serialized, even within one process. Types are checked when `@actor` runs, and
an unsupported one raises `SchemaError` naming the field:
`state: Cart.items: list[int] is not supported; use tuple[int, ...]`.

| Annotation | Notes |
|---|---|
| `None`, `bool`, `int`, `float`, `str`, `bytes` | Integers are limited to 64 bits |
| `Literal[...]` of `str`, `int` or `bool` | |
| `datetime` | Must be timezone-aware |
| `UUID` | |
| `tuple[T, ...]`, `tuple[A, B]`, `frozenset[T]` | |
| `Mapping[K, V]` | Read back as a `dict` |
| `@dataclass(frozen=True)`, including generic ones | |
| Unions, `type` aliases, recursive aliases | Dataclasses in a union need distinct names |
| `Ref[T]` | Arrives bound to the receiving system |

`list`, `dict`, `set`, non-frozen dataclasses, plain classes, `Any`, `object` and `Callable` are refused. Mutable
containers are refused because mutating the state in place would change the local state without replicating it.

Types in a body's annotation must be defined before the decorated function.

**Schema evolution.** Dataclasses are encoded by field name, so two versions of the code can share a cluster:

- a missing field with a default gets the default;
- an unknown field is ignored;
- a missing field without a default, an unknown union member or a mismatched value raises `SchemaError`.

An old node that saves a state written by a new node drops the fields it does not know. Renaming a dataclass that is
in a union, or replacing a dataclass field type by a union containing it, is incompatible.

## Clusters

### Starting a node

```python
cluster = Cluster(
    bind="0.0.0.0:7400",
    advertise="10.0.0.5:7400",  # what other nodes dial; defaults to `bind`
    seeds=("10.0.0.4:7400",),
)

async with ActorSystem(cluster=cluster) as system:
    await stop.wait()
```

- Without seeds other than itself, a node starts a cluster alone. Otherwise `async with` returns once it sees another
  member; bound the wait with `asyncio.timeout`.
- Nodes do not list actor types. A type is named `module:qualname`, and a node imports it when the name arrives from
  the cluster. **All nodes of a cluster must run the same code.**
- `system.ref(actor, key)` reaches the key from any node. `system.members` is the member table as this node sees it,
  with status `alive`, `leaving`, `suspect` or `dead`.
- Nodes with a different cluster `name` are refused with `Refused`.
- Several `ActorSystem`s can run in one process, each on its own port.

### Replication and write levels

Consistency is chosen per actor type:

```python
@actor(initial=Ledger(), replicas=3, write="majority")  # the defaults
async def ledger(ctx: Context[Ledger, LedgerMsg]) -> None: ...
```

`replicas` is the number of nodes that keep a copy of each key, capped by the size of the cluster. `write` is how many
must confirm a write of the state:

| `write` | Confirmations | Behaviour |
|---|---|---|
| `"majority"` | more than half | Confirmed writes survive the loss of a minority of the copies. The minority side of a partition cannot write. |
| `"all"` | all | Confirmed writes survive the loss of all copies but one. Any replica away refuses writes. |
| `"one"` | one | Writes while any replica is reachable. A change of owner can lose confirmed writes, and two owners can write during one. |

When too few replicas answer within `write_timeout`, `state.set` raises `Unavailable`, the body restarts and the message
being handled fails with `Unavailable`.

### Clients

A `Client` sends messages to a cluster without joining it. It hosts no actors, holds no state and takes no part in
membership, so the number of clients does not affect the membership protocol.

```python
async with Client(seeds=("10.0.0.4:7400",)) as client:
    tally = await client.ref(poll, "vim").ask(Count)
```

`Client` and `ActorSystem` both implement the `System` protocol (`ref`, `node`). The client imports the actor
types it uses, routes each message directly to the owner, and refreshes its member table every `sync_every`.

### Leaving, crashing and deploying

- **Leaving `async with` normally is an orderly exit.** The node stops being chosen as owner, finishes the messages in
  progress, hands its stored state to the next replicas and leaves, within `leave_timeout`. Pending asks to it do not
  fail.
- **Leaving by exception or cancellation is a crash.** The others detect it by heartbeat, and its keys are activated
  elsewhere from their replicas.
- **A rolling deploy** is an orderly exit and a join, node by node, relying on schema evolution and on keys moving with
  their state. While a deploy introduces an actor type, keys of that type placed on old nodes raise `UnknownActor`.

A restarted process is a new node, with a new incarnation and no state.

### TLS and compression

```python
Cluster(
    bind="0.0.0.0:7400",
    tls=TLS(cert="node.pem", key="node.key", ca="ca.pem"),
    compression=Compression(codecs=("zstd",), min_bytes=4096),
)
```

With `ca`, both sides verify the peer certificate against it, and `require_client_cert=True` (the default) makes it
mutual TLS. Host names are not verified: nodes are authenticated by the CA. Compression is negotiated per connection
among `zstd`, `lz4` and `zlib`, and skipped for payloads under `min_bytes`. `address_map` maps an advertised address to
the one to dial, for NAT and tunnels. `Client` takes the same three parameters.

## Collections

Named, replicated data structures built on actors, over an `ActorSystem` or a `Client`.

```python
from casty import Collections
from casty.collections import MISSING

collections = Collections(system)

visits = collections.counter("visits")
await visits.add(5)

prices = collections.dict("prices", key=str, value=int)
await prices.put("book", 30)
await prices.get("pen") is MISSING  # True

async with collections.lock("report", ttl=60) as lease:
    lease.token  # fencing token, increasing with each acquisition
```

| Collection | Factory | Operations |
|---|---|---|
| `Counter` | `counter(name, stripes=1)` | `add`, `get`, `reset` |
| `Register[T]` | `register(name, value=T)` | `get`, `set`, `compare_and_set`, `get_and_set` |
| `Dict[K, V]` | `dict(name, key=K, value=V, index_shards=16)` | `put`, `get`, `contains`, `remove`, `items`, `size`, `clear` |
| `Set[T]` | `set(name, value=T, shards=16)` | `add`, `remove`, `contains`, `items`, `size`, `clear`, `union`, `intersection`, `difference` |
| `MultiMap[K, V]` | `multimap(name, key=K, value=V, shards=16)` | `put`, `get`, `contains`, `remove`, `remove_key`, `size`, `clear` |
| `Queue[T]` | `queue(name, value=T)` | `offer`, `poll`, `peek`, `drain`, `size`, `clear` |
| `Semaphore` | `semaphore(name, capacity=n)` | `try_acquire`, `acquire`, `available` |
| `Lock` | `lock(name, ttl=30.0, timeout=None)` | `try_lock`, `acquire`, `locked`, `async with` |
| `Barrier` | `barrier(name, parties=n)` | `wait`, `waiting` |

All take `replicas` (default 3). Data collections also take `write` (default `"majority"`); semaphores, locks and
barriers always use `"majority"`.

- An absent entry is `MISSING`, distinct from a stored `None`.
- The first operation fixes the configuration of a name. Other settings or types raise `ConfigurationError`.
- Mutating calls return after the state is saved. A failed or timed-out call may have committed and is not retried.
- `Dict` stores each entry under its own key. `items`, `size` and `clear` walk an index and are not atomic, as are
  reads of a striped `Counter` and the set algebra of `Set`.
- `Queue` has a single owner. A lost `poll` or `drain` answer loses the removed items.
- `Semaphore` and `Lock` grant a `Lease` with a TTL in seconds, renewed only by `lease.renew(ttl)`. Expiry uses wall
  clocks, which must be synchronized. Waiters are served in order. A lease can expire under a slow holder, so the
  protected resource must reject tokens older than the newest it has seen.
- `acquire` and `Barrier.wait` wait indefinitely; bound them with `asyncio.timeout`. `async with lock` is not
  reentrant.

## Reference

### `@actor`

| Parameter | Default | Meaning |
|---|---|---|
| `initial` | none | State a key starts from. Without it, `ref` takes `initial=`, or the state type allows `None`. |
| `replicas` | `3` | Nodes that keep a copy of each key |
| `write` | `"majority"` | `"one"`, `"majority"` or `"all"` |
| `mailbox` | `None` | Mailbox capacity; `None` is unbounded |

### `Context[S, M]`

| Member | Meaning |
|---|---|
| `key` | Key of this entity |
| `state` | `State[S]`: `value`, `await set(state)`, `await update(change)` |
| `inbox` | Messages in arrival order; ends after `idle_after` without messages |
| `self` | Ref to this entity |
| `system` | System of the node running this activation |
| `await become(behavior[, state])` | Hand the key to another actor type with the same messages |
| `merge(source)` | Messages and items of `source` in arrival order, until `source` ends |

### `ActorSystem`

| Parameter | Default | Meaning |
|---|---|---|
| `cluster` | `None` | Without it, the system runs in this process only |
| `idle_after` | 1 min | Time without messages after which `inbox` ends |
| `backoff` | `Backoff(first=100ms, limit=10s, factor=2.0)` | Delay before restarting a body that raised |
| `ask_timeout` | 10 s | Deadline of `ask` |
| `write_timeout` | 5 s | How long a write of the state or an activation waits for replicas |
| `leave_timeout` | 30 s | Budget of an orderly exit |

Members: `ref(actor, key, initial=...)`, `node`, `members`.

### `Cluster`

| Parameter | Default | Meaning |
|---|---|---|
| `bind` | required | `host:port` to listen on; port 0 picks a free one |
| `seeds` | `()` | Nodes to join through |
| `advertise` | `bind` | Address other nodes dial |
| `name` | `"casty"` | Cluster name |
| `tls` | `None` | `TLS(cert, key, ca=None, require_client_cert=True)` |
| `compression` | `Compression()` | `codecs=None` (all), `min_bytes=4096` |
| `address_map` | `None` | Advertised address to dialled address |
| `heartbeat` | 1 s | Ping period |
| `suspect_after` | 5 s | Silence before `suspect`; must exceed `heartbeat` |
| `dead_after` | 5 s | Time as `suspect` before `dead` |
| `remove_after` | 1 min | Time as `dead` before removal; `None` disables it |
| `anti_entropy` | 30 s | Period of the full table exchange |
| `overlay` | `Overlay()` | `active=5`, `passive=30`, `join_walk=6`, `passive_walk=3`, `shuffle_every=10s`, `graft_after=500ms` |

With the defaults a dead node is detected in about ten seconds. Above roughly fifteen nodes, raise `suspect_after`.

### `Client`

`seeds` (required), `name`, `tls`, `compression`, `address_map`, `ask_timeout` (10 s), `sync_every` (5 s). Members:
`ref`, `node`, `members`.

### Errors

`SchemaError`, `NotStarted`, `ActorFailed`, `MailboxFull`, `Unavailable`, `UnknownActor`, `Refused`, and the built-in
`TimeoutError`. `casty.collections` adds `ConfigurationError`. After a system exits, `ref`, `tell` and `ask`
raise `RuntimeError`.

## How it works

### Layers

```text
Python          actor bodies, and the typed surface the checkers read
casty-py        extension module: bridge between the asyncio loop and the core
casty-node      services of a node: membership, placement, routing, replication, handoff
casty-core      rules without I/O: schema, ring, member table, gossip, replica and owner logic
casty-net       TCP transport: framing, multiplexing, flow control, TLS, compression
```

The rules of the actor model are in Rust: mailboxes, ordering, idleness, activation, restarts, `become`, replication
and routing. The core calls Python only to create and cancel the task of a body, to build and read message and state
objects, to import an actor type by name, and to pull the next item of a `merge` source. Bodies run on the asyncio
loop.

The protocol rules take messages in and return messages to send, with no I/O, so they are tested by simulation over
random loss, delay and reordering.

A system without a `Cluster` is the same runtime with one node and one replica.

### Serialization

`@actor` compiles the annotations of the body into a schema, a tree of converters. Encoding walks the value and the
schema together and writes msgpack directly.

- A dataclass is a map keyed by field name, which is what allows schema evolution.
- A dataclass at the top of a sent value, or in a union, is written as `[name, map]`. The top is tagged because the
  holder of a `Ref[Deposit]` does not know the full message union of the receiver.
- A `Ref` is written as its target, an entity `(type, key)` or a pending `ask`, and decoded bound to the receiving
  system.
- The state is split into pages, one per top-level field. A write sends only the pages that changed. A state that is
  not a dataclass is one page.

### Life of a key

1. A message, or the request a `ref` sends, arrives for a key without an activation. The node creates the mailbox,
   queues what arrives and asks the replicas for the state.
2. If no replica has one, the key starts from the `initial` the ref offered, or from the default of the type.
3. The node writes the state back with an `@active` page, and creates the body task once that write is confirmed.
4. `ctx.inbox` yields messages in arrival order and ends after `idle_after` without any.
5. When the body returns, it runs again if messages arrived and it had read at least one. Otherwise the node clears
   `@active` and drops the activation. The read condition keeps a body that never reads its inbox from spinning.

`become` writes the new state with an `@behavior` page naming the new type. At the next inbox read the core cancels
the old body and starts the new one on the same mailbox. Later activations read `@behavior` to pick the function.

### Transport

One TCP connection per pair of nodes, opened by whoever sends first. On simultaneous dials, the handshake keeps the
connection of the node with the lower incarnation, so nothing queued is lost.

- The connection is multiplexed into streams. Frames have a 12-byte header: version, type, flags, stream, length.
- Replication has its own stream, so state transfers do not delay actor messages.
- Flow control is per stream, with credit returned as the receiver consumes bytes. `send` never waits; envelopes
  without credit queue on the connection.
- The handshake carries protocol versions, cluster name, node identity, role and offered compressors. A mismatch
  surfaces as `Refused`.
- A lost connection drops what was not written, and the next send redials with exponential backoff. Nothing is resent,
  which is the origin of at-most-once delivery.
- Limits: 256 KiB per frame, 4 MiB per message, 256 KiB initial window, keepalive after 15 s of silence. A page above
  the message limit makes `state.set` raise `ValueError`.

A node identity is its advertised address plus an incarnation, a UUID new on every start. Envelopes for an incarnation
that is no longer listening are dropped.

### Membership

- **HyParView** gives each node an active view (5 neighbours) and a passive view (30 spares). Nodes talk regularly only
  to their active view, so per-node cost does not grow with the cluster.
- **Plumtree** broadcasts table changes over a tree on the active views: full events along tree edges, announcements
  along the others. A node that sees an announcement without the event requests it and repairs the tree.
- **SWIM-style records** order observations by incarnation number and status,
  `alive < leaving < suspect < dead < left`. A wrongly suspected node refutes by raising its number.

Failure detection is by heartbeat to the active view: silence for `suspect_after` makes a neighbour `suspect`, and
`dead_after` without refutation makes it `dead`. Each period a node also pings the member outside its active view it
has heard from least recently; otherwise the minority side of a partition would keep unreachable members as alive.

A `dead` member is removed after `remove_after`, and only by a node that sees more than half of the members as
`alive`. Two sides of a partition cannot both hold a majority, so the smaller side never removes the larger. A node
that learns it was removed hands over its state and rejoins with a new identity.

Each node also exchanges its full table with a random member every `anti_entropy`. Member records carry the actor type
names the node knows, which is how type names spread.

### Placement

A consistent hash ring with 128 virtual nodes per node, positioned by an 8-byte BLAKE2b of the node identity and an
index. A key hashes to `blake2b("type/key")`. Its replicas are the owner of the next point and the following distinct
nodes, up to `replicas`. Its owner, the node that runs the body, is the first replica that is `alive` or `suspect`.
Every node computes this from its own member table; there is no coordinator.

- **The ring does not shrink when a node is `dead`.** Otherwise each side of a partition would recompute replicas
  among the nodes it sees and both would accept writes.
- **A replica set changes one node at a time.** When several members change together, the ring passes through the
  intermediate configurations, one member per step, in an order every node derives identically. With one replica
  swapped, the old owner's write quorum always overlaps the new set; with two it might not.

### Routing

A message goes to the owner the sender computes. The receiver checks that it is the owner in its own view, and
otherwise answers `WrongOwner`; the sender retries once with its current view, and a second refusal ends in
`Unavailable`.

Messages from one node to one key use one connection and one mailbox, which preserves their order while the owner does
not change. A pending `ask` fails with `Unavailable` as soon as the sender sees the target as `dead`.

### Replication

The owner is the only writer of a key.

- **Activation.** The owner picks an epoch, a round above any it knows paired with its identity, and sends `Prepare`.
  Each replica promises to reject older epochs and answers with what it holds. The owner takes the newest state by
  `(epoch, version)`, writes it back under its epoch, and starts the body once the write level confirms. That write
  also repairs lagging replicas.
- **Save.** The owner sends the changed pages and the version they are based on. A replica holding exactly that base
  applies the delta; any other requests the full state. `state.set` returns at the W-th confirmation.
- **Fencing.** A replica that promised a newer epoch rejects the write, which means another node took the key. The core
  cancels the body, so `state.set` never returns, fails the message in hand with `Unavailable`, and returns the mailbox
  to routing. The write is not retried, because it may have reached a replica and would be applied twice.

With `"majority"` and `"all"`, the replicas read at activation overlap those of every confirmed write. An activation
also waits for every replica the member table does not consider dead rather than stopping at the minimum, because the
overlap holds only within one replica set, and the set may be changing.

### Handoff

After each change of the ring or of an owner, every node sweeps what it stores and, from its own view:

- **reattaches** keys stored with `@active`, owned by this node and not running;
- **ends** activations of keys it no longer owns, returning their mailboxes to routing;
- **hands over** keys whose replica set no longer includes it, deleting the local copy once all current replicas
  confirm.

A node that gains a token range pulls it from the nodes of the previous ring. While the range is arriving, the node
accepts writes but does not count towards quorums; otherwise empty new replicas could form a quorum alone and start a
key from `initial`. The ring step is held until the transfer ends, and keys in that range can be unavailable meanwhile.

An orderly exit uses the same parts: broadcast `leaving` (no longer owner, still replica), drain activations, hand over
every stored key to the ring without the node, broadcast `left`.

### Clients

A client is in neither the ring nor the member table. It requests the table every `sync_every`, builds the same ring
and sends each message to the owner in one hop. It has no listener, so answers return on the connection it opened. Its
view can lag by `sync_every`, which costs one retry for a key that moved.

## Guarantees and limits

- Delivery is at most once. Messages from one node to one key arrive in order while the owner does not change.
- Two nodes may briefly run the same key, but with `"majority"` or `"all"` only one of them can save.
- A confirmed write of the state is not lost while fewer replicas fail than the write level tolerates.
- A failed write may reappear: if it reached fewer than W replicas, a later owner may adopt it. `Unavailable` means
  the operation may or may not have happened.
- Reads are not linearizable. An isolated owner answers from a possibly stale state until it tries to write.
- Side effects in a body can repeat after a restart or a change of owner.
- State lives in memory only. Losing every replica of a key loses the key.
- Bodies may run in parallel on free-threaded Python. Mutable state shared between actors is not supported.

## Development

```sh
uv sync                     # builds the extension and installs the dev tools
uv run pytest -q
cargo test --workspace
uv run ruff check . && uv run mypy && uv run pyright
cargo fmt --all --check && cargo clippy --workspace --all-targets
```

The Python suite runs real systems over TCP on loopback, with real crashes and partitions made by TCP proxies. There
are no mocks and no simulated clocks.
