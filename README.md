# casty

Typed, replicated virtual actors for Python.

You can think of an actor as a stateful entity identified by its type and key, defined as an async function.
It processes messages one at a time, in a single process or replicated across a cluster.

## Contents

- [Installation](#installation)
- [Quick start](#quick-start)
- [Guide](#guide)
- [Clusters](#clusters)
- [Collections](#collections)
- [Observing a node](#observing-a-node)
- [Reference](#reference)
- [API reference](https://gabfssilva.github.io/casty/)
- [How it works](#how-it-works)
- [Guarantees and limits](#guarantees-and-limits)
- [Development](#development)

## Installation

```sh
uv add casty        # or: pip install casty
```

casty needs Python 3.12 or later, free-threaded 3.14t included, and has no Python dependencies. Each platform has two
wheels: one on the stable ABI (`cp312-abi3`), for every version with the GIL, and one for 3.14t (`cp314-cp314t`),
which the stable ABI does not cover.

On a platform or interpreter without a wheel, the install builds from source and needs a Rust toolchain, 1.90 or
later. The same applies to installing from a checkout:

```sh
uv add ./casty      # or: pip install ./casty
uv build --wheel    # dist/casty-<version>-cp312-abi3-<platform>.whl, or cp314-cp314t on 3.14t
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
the decorated function. `ctx.inbox` ends after `idle_after` without messages (the type's, or the system's, one minute
by default); when the body returns, the key is deactivated and its state stays stored. The next ref or message
activates it again from the saved state.

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

`ask` raises `TimeoutError` after the `ask_timeout` of the actor type, or the system's (10 seconds by default). Keyword
arguments go to the message, so a shorter deadline is set with `asyncio.timeout`. An `ask` that times out or is
cancelled tells the key to stop working on its message, but what the body did before that stays (see
[Cancellation](#cancellation)).

### State

`ctx.state` is a `State[S]`:

- `ctx.state.value` is the last saved state.
- `await ctx.state.set(new)` replaces it, and returns once the write level of the type has confirmed it (see
  [Replication](#replication-and-write-levels)). `value` changes only then.
- `await ctx.state.update(change)` saves what `change` makes of the current state and returns it. `change` is a
  function, sync or `async`, from the state to the new state.
- `await ctx.state.delete()` deletes the state of the key on its replicas, at the write level of the type. The key is
  then one nothing wrote: activated again, it starts from `initial`. The body goes on from the default of its type; for
  a type without one, `value` raises until the next `set`. A body that ends without writing again leaves nothing of
  the key on any replica.

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

State is kept in memory on the replicas, unless the type is durable.

### Durable state

A type declared `durable=` is kept by the store of the system too: `ActorSystem(store=...)`, any object with async
`load(actor, key)`, `save(actor, key, version, state)` and `drop(actor, key, version)`, shared by every node of the
cluster.

```python
from casty.sqlite import SQLiteStore


@actor(initial=0, durable="write")
async def account(ctx: Context[int, AccountMsg]) -> None: ...


async with ActorSystem(cluster=cluster, store=SQLiteStore("accounts.db")) as system: ...
```

| `durable` | Saved to the store |
|---|---|
| `None` | Never: the state is in memory only (the default) |
| `"write"` | Every confirmed write, before `state.set` returns |
| a `timedelta` | The latest confirmed write, at most that long after it; the last write of an activation and deletions at once. Writes return without waiting for the store. |

Every activation of a durable key reads the store and goes on from the later of the replicas and the store, so a key
comes back after every replica, or the whole cluster, was lost. The store keeps the record with the greatest `version`
(bytes compared in order), which is how a late save of a node that lost the key never undoes a later one.

- Every node of a cluster is given a store reaching the same records. A durable type does not activate on a node
  without one: `ask` raises `Unavailable`.
- Store calls run as tasks on the event loop, bounded by the `write_timeout` of the type. A load that fails fails the
  activation, and with `"write"` a save that fails fails the write with `Unavailable`.
- The collections are not durable.

`casty.sqlite.SQLiteStore(path)` is a store in one SQLite file, for a system alone or for the nodes of one machine,
which may all open the same file. Nodes on several machines need a store over a database they all reach;
`src/casty/sqlite.py` is the shape of one, one statement per method. `examples/11-durable-state` stops a whole cluster
and reads its durable keys back on new nodes.

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
system.ref(document, "readme")  # TypeError, and an error in pyright
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

An `ask` made by a body carries the keys whose bodies wait for its answer: those waiting on the message the body is on,
and the body itself until it reads again. One that comes back to such a key raises `ReentrancyError` at once, naming
the cycle, instead of waiting for the deadline: a body asking its own key, or two keys asking each other. The chain
names the 16 most recent keys and only the task that read the message adds to it, so a longer cycle, one through a task
the body started, and two requests that each hold a key the other asks still end in `TimeoutError`.

### What holds up a key

A body takes its next message when it reads `inbox` again, so everything it awaits in between holds up the messages of
its key: a `state.set` until the write level confirms it, an `ask` until its answer or deadline, a sleep, any I/O.
Messages keep queuing meanwhile, up to `mailbox`. Nothing else blocks it: a task the body starts runs beside it.

### Cancellation

An `ask` that is cancelled, or passes its deadline, raises `CancelledError` or `TimeoutError` in the caller, as any
`await` does, and the key hears of it:

- a message still queued is dropped unread;
- a caller waiting for room in a full mailbox is let go;
- a body on the message is cancelled at the `await` it is on, and runs again, from the last confirmed state, for the
  next message.

A cancelled `ask` asks the body to stop; it does not undo what the body did before the `await` it was on, and a write
confirmed before it stays. A collection finishes the message it is on. An answer that arrived first is kept, and a
cancellation that crosses the answer the body already told changes nothing.

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
| Bounded mailbox is full, `on_full="refuse"` | `MailboxFull` | |
| The `ask` closes a cycle: the key it goes to waits, down the chain of asks, for its answer | `ReentrancyError`, at once, naming the cycle | The message is not queued |
| In a cluster, the message, an initial state or the answer is larger than `Limits.message` | `MessageTooLarge` | Nothing was sent |
| Owner unreachable, too few replicas, owner changing, or the store of a durable type not answering | `Unavailable` | The message may or may not have been processed |
| The owner's code does not have the actor type | `UnknownActor` | |
| The `ask` is cancelled, or gets no answer within `ask_timeout` | `CancelledError` / `TimeoutError` | The key drops the message if it is still queued, or cancels the body working on it (see [Cancellation](#cancellation)) |

For `tell`, the same situations drop the message and report a `MessageDropped` (see
[Observing a node](#observing-a-node)), which the default observer logs. In a cluster, a `tell` larger than
`Limits.message` raises `MessageTooLarge` where it is sent.

The restart delay follows the `backoff` of the type, or the system's: `first`, multiplied by `factor` on each
consecutive failure, up to `limit`. The mailbox is kept across restarts.

Mailboxes are unbounded by default, so an actor slower than its senders accumulates messages in memory.
`@actor(mailbox=n)` bounds it. A full mailbox refuses the new message (`on_full="refuse"`, the default), or, with
`on_full="wait"`, holds the `ask` until there is room, within its `ask_timeout`: the message stays with its sender,
which sends it again when the owner calls it back, one caller for each message taken, in the order they arrived. A
`tell` to a full mailbox is dropped either way. Actors whose full mailboxes wait on each other down one chain of asks
fail with `ReentrancyError`; two requests that each hold a key the other asks still end at the deadline.

### Types

State, messages and replies are always serialized, even within one process. Types are checked when `@actor` runs, and
an unsupported one raises `SchemaError` naming the field:
`state: Cart.items: list[int] is not supported; use tuple[int, ...]`.

| Annotation | Notes |
|---|---|
| `None`, `bool`, `int`, `float`, `str`, `bytes` | Integers are limited to 64 bits |
| `Literal[...]` of `str`, `int` or `bool` | |
| `datetime` | Must be timezone-aware |
| `date` | Days since 1970-01-01 |
| `time` | Must be timezone-aware with a fixed offset (`timezone(...)`); a `time` with a `ZoneInfo` has none |
| `timedelta` | Limited to 64 bits of microseconds |
| `Decimal` | Written as its string, so it stays exact |
| `UUID` | |
| `Enum`, `IntEnum`, `StrEnum` | Written as the member name; a renamed member still reads old payloads if the old name stays as an alias. `Flag` is refused: use a `frozenset` of an `Enum` |
| `PurePosixPath`, `PureWindowsPath`, `Path` | Written as its string, read as the annotated class. A `Path` is read in the receiver's flavour, so `PurePosixPath` is the portable annotation |
| `tuple[T, ...]`, `tuple[A, B]`, `frozenset[T]` | |
| `Mapping[K, V]` | Read back as a `dict` |
| `@dataclass(frozen=True)`, including generic ones | |
| Unions, `type` aliases, recursive aliases | Dataclasses in a union need distinct names. Alternatives written alike, such as `str` and an `Enum`, `int` and `date`, or `bytes` and an `Opaque` value, are refused as ambiguous |
| `Ref[T]` | Arrives bound to the receiving system |
| `Annotated[T, Opaque(encode, decode)]` | Any `T`: travels as the bytes `encode` returns and is read back with `decode`. casty does not read them: no schema evolution inside, no other language, and every node must decode what any other encodes |

`list`, `dict`, `set`, non-frozen dataclasses, plain classes, `Any`, `object` and `Callable` are refused. `list`,
`dict` and `set` are refused because mutating the state in place would change the local state without replicating it;
use `tuple`, `Mapping` and `frozenset`.

A type the schema refuses travels through `Opaque`:

```python
type Frame = Annotated[np.ndarray, Opaque(encode=to_bytes, decode=from_bytes)]
```

The checkers see `np.ndarray`; the wire carries bytes. Two values are equal when stored only if `encode` gives them the
same bytes, which collection keys, `compare_and_set` and the replication of changed pages rely on. An `Annotated`
without `Opaque` is read as its type.

Types in a body's annotation must be defined before the decorated function.

**Schema evolution.** Dataclasses are encoded by field name, so two versions of the code can share a cluster:

- a missing field with a default gets the default;
- an unknown field is ignored;
- a missing field without a default, an unknown union member, an enum member the reader does not have or a mismatched
  value raises `SchemaError`;
- an `Opaque` value is read by its `decode`, so its evolution is the caller's.

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
  member; bound the wait with `asyncio.timeout`. A join that is cancelled, or refused, lets go of the address, the
  connections and the threads it took, and the system can be entered again.
- Nodes do not list actor types. A type is named `module:qualname`, and a node imports it when the name arrives from
  the cluster. **All nodes of a cluster must run the same code.**
- `system.ref(actor, key)` reaches the key from any node. `system.members` is the member table as this node sees it,
  with status `alive`, `leaving`, `suspect` or `dead`.
- Nodes with a different cluster `name` or different [`Limits`](#limits) are refused with `Refused`.
- Several `ActorSystem`s can run in one process, each on its own port.
- The transport of each `ActorSystem` in a cluster and of each `Client` runs on a thread per core of its own. Systems
  given the same `Runtime` share its threads instead, which is what a process holding many clients wants:

  ```python
  runtime = Runtime(threads=4)
  async with Client(seeds=seeds, runtime=runtime) as one, Client(seeds=others, runtime=runtime) as two:
      ...
  ```

  A system that ends, orderly or by a crash, stops its node, its listener and its connections and leaves the runtime
  running for the others; the threads go when nothing holds the runtime.

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

When too few replicas answer within the `write_timeout` of the type, or the system's, `state.set` raises `Unavailable`,
the body restarts and the message being handled fails with `Unavailable`.

### Pinned actors

A type declared with `@actor(pinned=True)` runs each key on one node, which the ref names: `at=member`, `at=node_id` or
`at="10.0.0.5:7400"`, the address the node advertises.

```python
@actor(initial=Load(), pinned=True)
async def agent(ctx: Context[Load, AgentMsg]) -> None: ...


for member in system.members:
    system.ref(agent, "agent", at=member).tell(Refresh())
```

- The key carries that address (`ctx.key` reads `@10.0.0.5:7400/agent`), so the ref reaches the node after it
  restarts.
- The key is never moved and has one copy: `replicas` defaults to 1, and more raises `ValueError`. `ask` raises
  `Unavailable` while no member up advertises the address.
- An orderly leave ends the key's activation and no other node starts it. A process that comes back on the address
  starts it from `initial`, or from the store for a durable type.
- A pinned type without `at=` raises `TypeError`, and so does any other type with it.

`examples/10-agent-per-node` keeps one agent per node, reached by `NodeId`, by `Member` and by address, while a node
joins, one leaves and a new process starts on its address.

### Clients

A `Client` sends messages to a cluster without joining it. It hosts no actors, holds no state and takes no part in
membership, so the number of clients does not affect the membership protocol.

```python
async with Client(seeds=("10.0.0.4:7400",)) as client:
    tally = await client.ref(poll, "vim").ask(Count)
```

`Client` and `ActorSystem` both implement the `System` protocol (`ref`, `node`). The client imports the actor
types it uses, routes each message directly to the owner, and refreshes its member table every `sync_every`.

A client has no listener: it can be answered, but not sent to.

- It cannot hold an entity. Every key `client.ref` reaches lives on a node.
- It cannot receive a `tell`. The only ref that leads to a client is the `reply_to` of its own `ask`, which takes one
  answer, over a connection the client opened. A second `tell` to it, one after the `ask` has ended, and one from a
  node the client has no connection to are dropped.
- It cannot be subscribed to anything. An entity that keeps a client's `reply_to` to report later changes reaches it
  once.

A process that only calls into the cluster, such as a web handler, a script or a batch job, fits a `Client`. One that
must be pushed to has two options:

- Poll: `ask` on an interval, or keep one `ask` pending that the entity answers only when something changes, and ask
  again after each answer and each `TimeoutError`. The entity keeps the `reply_to` and reads on; a body that awaited
  the change while on the message would hold up its key, and be cancelled when the `ask` times out.
- Run an `ActorSystem` joined to the cluster and receive in an entity, which can be told, subscribed and passed around
  as a `Ref`. The entity runs on whichever node owns its key, or, for a [pinned](#pinned-actors) type, on the process's
  own node (`at=system.node`). Either way the process becomes a member: it listens, takes part in membership, hosts its
  share of keys and replicas, and must run the same code as the other nodes.

### Leaving, crashing and deploying

- **Leaving `async with` normally is an orderly exit.** The node stops being chosen as owner, finishes the messages in
  progress, hands its stored state to the next replicas and leaves, within `leave_timeout`. Pending asks to it do not
  fail. Keys the replicas had not taken by then are reported in `HandoffEnded.abandoned`.
- **Leaving by exception or cancellation is a crash.** The others detect it by heartbeat, and its keys are activated
  elsewhere from their replicas.
- **A rolling deploy** is an orderly exit and a join, node by node, relying on schema evolution and on keys moving with
  their state. While a deploy introduces an actor type, keys of that type placed on old nodes raise `UnknownActor`.

A restarted process is a new node, with a new incarnation and nothing in memory. A ref to a pinned key reaches the new
process on the same address, where the key starts again from `initial`, or from the store for a durable type.

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
among `zstd`, `lz4` and `zlib`, and a frame shorter than `min_bytes` (4096) goes uncompressed. `address_map` maps an
advertised address to the one to dial, for NAT and tunnels. It is called on the event loop at every dial, which is when
a connection opens or opens again, so a tunnel that comes back on another port is followed. `Client` takes the same
three parameters.

### Limits

```python
Cluster(bind="0.0.0.0:7400", limits=Limits(message=32 * 1024 * 1024))
```

`Limits` sets the sizes of a connection in bytes: `message` (4 MiB) bounds one envelope between two nodes, which is a
message to a key, its answer, or a message of replication; `frame` (256 KiB) bounds each piece an envelope is cut into;
and `window` (256 KiB) is how much of a stream may be in flight. Each is at most 2 GiB; `message` must be at least
128 KiB, `frame` at most `message`, and `window` at least `frame`.

- Every node and client must have the same limits: the handshake refuses a peer whose limits differ, and that surfaces
  as `Refused`. `Client` takes `limits` too.
- A message or an initial state over `message` raises `MessageTooLarge` where it is sent, and an answer over it reaches
  the caller as `MessageTooLarge`, whether or not the key is on the node that sends it. A system without a cluster
  takes any size.
- The state is not bound by it: a page larger than a message is cut across as many as it takes. A write raises
  `MessageTooLarge` only when a field has a name that leaves no room for its data in a message.

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
| `Dict[K, V]` | `dict(name, key=K, value=V, index_shards=16)` | `put`, `get`, `contains`, `remove`, `scan`, `items`, `size`, `clear` |
| `Set[T]` | `set(name, value=T, shards=16)` | `add`, `remove`, `contains`, `scan`, `items`, `size`, `clear`, `union`, `intersection`, `difference` |
| `MultiMap[K, V]` | `multimap(name, key=K, value=V, shards=16)` | `put`, `get`, `contains`, `remove`, `remove_key`, `scan`, `size`, `clear` |
| `Queue[T]` | `queue(name, value=T)` | `offer`, `poll`, `peek`, `drain`, `size`, `clear` |
| `Semaphore` | `semaphore(name, capacity=n)` | `try_acquire`, `acquire`, `available` |
| `Lock` | `lock(name, ttl=30.0, timeout=None)` | `try_lock`, `acquire`, `locked`, `async with` |
| `Barrier` | `barrier(name, parties=n)` | `wait`, `waiting` |

All take `replicas` (default 3). Data collections also take `write` (default `"majority"`); semaphores, locks and
barriers always use `"majority"`.

- An absent entry is `MISSING`, distinct from a stored `None`.
- The first operation fixes the configuration of a name. Other settings or types raise `ConfigurationError`.
- A `Collections` answers the same object for the same name and arguments while that object is held, and checks the
  configuration with the cluster once for it, so keep one per system and hold what it gives you. What nobody holds is
  dropped, so a collection per entity name does not pile up; asked for again, it is built anew and checks again. While
  a name's configuration is held, asking for it with other settings raises at once.
- Mutating calls return after the state is saved. A failed or timed-out call may have committed and is not retried.
- The index of a dict, a set or a multimap is kept per shard in segments of about 512 keys, split as it grows, so
  adding a key rewrites one segment whatever the size; values listed under one multimap key stay together.
- `scan()` on `Dict`, `Set` and `MultiMap` is an async iterator that holds one segment of the index at a time, so a
  collection of any size can be read. It is not a snapshot: an entry written while it runs may be seen or not, and
  none is seen twice. `items()` collects it into a list.
- `Dict` stores each entry under its own key and lists the key in the index. Replacing the value of a key needs only
  its entry; adding or removing a key also needs the index segment of its key. `scan`, `items` and `clear` read the
  index a segment at a time and ask the entries of its keys at once. `size` counts the keys listed, one ask per
  segment: a put or removal that stopped halfway leaves a key listed without a value, which counts until the next
  scan drops it or the key is put or removed again.
- A removed `Dict` key leaves nothing on the replicas. Its generations come from the wall clock, so the clocks of the
  nodes must agree within the time between a removal and the next put of the key.
- `size` and `clear`, in every collection that has them, and reads of a striped `Counter` are not atomic: an entry
  written while they run may be counted, or survive, or not. `Set.union` reads both sets; `intersection` and
  `difference` read this set and ask the other about each member.
- `Queue` keeps its items in segments of up to 1024 items or 64 KiB under keys of their own, so an operation writes
  one segment and the queue has no size ceiling. A segment drained for good is deleted once it idles. A lost `poll`
  or `drain` answer loses the removed items.
- `Semaphore` and `Lock` grant a `Lease` with a TTL in seconds, renewed only by `lease.renew(ttl)` and given back by
  `lease.release()`, which nothing answers: a lost release leaves the permits held until the TTL runs out. Expiry uses
  wall clocks, which must be synchronized. Waiters are served in order. A lease can expire under a slow holder, so the
  protected resource must reject tokens older than the newest it has seen.
- `acquire` and `Barrier.wait` wait indefinitely; bound them with `asyncio.timeout`. `async with lock` is not
  reentrant.

`Semaphore` and `Lock` are the actor `casty.collections.semaphore` behind `ask`, and a body can use that actor without
waiting on it. A key takes its capacity from the `initial` its first ref is obtained with, and answers `Acquire` with
`Acquired` or `Denied`, which are messages: told to `ctx.self`, the answer arrives in the inbox, and the body goes on
reading meanwhile.

```python
from casty.collections import Acquired, Denied, SemaphoreState, semaphore


@actor(initial=0)
async def crawler(ctx: Context[int, Crawl | Acquired | Denied]) -> None:
    fetches = ctx.system.ref(semaphore.actor, "fetches", initial=SemaphoreState(capacity=8))
    async for msg in ctx.inbox:
        match msg:
            case Crawl(url):
                fetches.tell(semaphore.Acquire(ctx.self, ttl=60, lease_id=url))
            case Acquired(url, _):
                await fetch(url)  # at most 8 at once, across every key of every crawler
                fetches.tell(semaphore.Release(url))
            case Denied():
                pass
```

- `Acquire(reply_to, n=1, ttl=30.0, wait=None, lease_id=None)`. `wait` is how long the request stays in line: `None`
  for as long as it takes, `0` for now or never. The semaphore names the lease when `lease_id` is `None`. Sent again
  under the same `lease_id`, a request keeps its place in line, and a granted one hears its grant again.
- `Release(lease_id)` gives the permits back, or withdraws a request still waiting, and answers nothing.
  `Renew(reply_to, lease_id, ttl)` answers whether the lease was still held, and `Get(reply_to)` answers a `Status`.
- The capacity is the one of the ref that created the key; the `initial` of a later ref is ignored. A ref to a key
  nobody created, received in a message, meets `NotStarted`, and so does one to a key lost with every replica, since
  the collections are not durable, until a ref brings its capacity again. `Semaphore` and `Lock` do that themselves.

## Observing a node

### Events

`ActorSystem(observer=...)` and `Client(observer=...)` take any callable of one `casty.Event`. It is called on the
event loop, one event at a time, after the step that produced the event, so it may call back into the system and must
not block. An observer that raises is reported to the loop's exception handler and the node carries on.

| Event | Reported when |
|---|---|
| `MemberChanged(node, status, previous)` | A member of this node's table changed status |
| `ActivationStarted(actor, key)` | A key became active on this node |
| `ActivationEnded(actor, key)` | A key stopped being active on this node |
| `ActivationFailed(actor, key, error)` | A body raised and restarts, or the key cannot run here |
| `WriteFailed(actor, key, operation, reason, message)` | Taking a key over (`activate`) or a write (`write`) failed: `unavailable`, `fenced` or `too_large` |
| `HandoffStarted(actor, direction)` | Keys of a type started moving `in` to or `out` of this node after a change of the ring |
| `HandoffEnded(actor, direction, abandoned)` | Nothing of the type moves in that direction any more |
| `ConnectionLost(node)` | A connection to a node ended |
| `MessageDropped(actor, key, reason)` | A `tell` ended on this node without reaching its key |

An observer may say which kinds it takes with a method `wants(kind: type[Event]) -> bool`: the system asks it once per
kind when it enters and never builds an event of a kind it declined.

Without an observer, a system reports to `casty.LoggingObserver`, which writes every event to the `casty` logger:
dropped messages, failed bodies and writes, keys a leave went without, and members turning suspect or dead are
warnings; other membership changes, lost connections and handoffs are info; activations starting and ending are debug,
and the default observer takes them only when the `casty` logger takes debug records as the system enters, so a node
that does not log at debug builds no event per activation. Passing an observer replaces it; to keep the logging too,
call a `LoggingObserver()` from your observer.

`HandoffEnded.abandoned` names the keys an orderly leave went without: the nodes that replicate them now had not taken
them within `leave_timeout`. Whatever of them only this node held left with it.

`MemberChanged(node, status, previous)` reports each change of status in this node's member table once, in the order
the table made it: a crash is `suspect`, then `dead`, then `left` once `remove_after` passes on a side that sees a
majority alive; an orderly shutdown is `leaving`, then `left`. The table can lag, and an event is not a consensus: it
is what this node heard, through gossip, when it heard it. Two nodes see a change at different moments, a node cut off
by a partition sees the changes of that time only after the heal, and a node that first hears of a later status skips
the ones before it (`previous` then says `alive` for a member now `dead`). A client asks for the table every
`sync_every` and reports what changed from one answer to the next. To react to membership in a loop, queue the events
from the observer:

```python
changes: asyncio.Queue[casty.MemberChanged] = asyncio.Queue()
logged = casty.LoggingObserver()


def observer(event: casty.Event, /) -> None:
    logged(event)
    if isinstance(event, casty.MemberChanged):
        changes.put_nowait(event)


async with casty.ActorSystem(cluster=cluster, observer=observer) as system:
    while True:
        change = await changes.get()
        ...
```

### Stats

`system.stats()` (and `client.stats()`) reads what the node counts at that moment, as a `Stats`:

| Field | Meaning |
|---|---|
| `actors` | Every actor type the node has met, mapped to `ActorStats(active, queued, deepest)`: its activations here, the messages waiting in their mailboxes, and the most waiting in one of them |
| `asks_in_flight` | Asks of this system, its bodies' included, waiting for an answer |
| `connections` | Connections open now |
| `writes_confirmed`, `writes_failed` | Writes of the keys this node owns, since the system entered |
| `bytes_sent`, `bytes_received` | Bytes on its connections since the system entered, compressed and before TLS |

Nothing is counted as messages go by: the activations are read when `stats()` is called, and the writes and bytes are
counters kept by the replication and the transport, so a node that nobody reads pays nothing for them. The counters
start again from zero when a node the cluster removed rejoins under a new identity. A client hosts no key and owns no
write, so those count zero.

### Activations, placement and release

`system.activations()` lists the keys active on the node, one `Activation(actor, key, since, queued)` each, by type and
key: what `stats()` counts, key by key.

`await system.placement(actor, key)` answers where the key is as that node routes, as `Placement(owner, replicas)`: the
replicas in ring order, and the owner, the first of them up, where a message goes and the key activates (`None` when
none is up). It takes the `key` and `at=` of `ref`, and a `Client` answers it too, from the table it last synced.

`await system.release(actor, key)` ends the key's activation on that node as if it had idled out: the body finishes the
message it is on and ends at its next read, its writes land, the key lets go with its last write, and what reached it
meanwhile goes, in order, to the activation that starts it again from its state. `key` is the key as `ctx.key` reads
it. It answers `False` when the key is not active there.

## Reference

The API reference, generated from the docstrings, is at <https://gabfssilva.github.io/casty/>: [casty](https://gabfssilva.github.io/casty/casty/), [casty.collections](https://gabfssilva.github.io/casty/collections/), [casty.observer](https://gabfssilva.github.io/casty/observer/) and [casty.sqlite](https://gabfssilva.github.io/casty/sqlite/).

### `@actor`

| Parameter | Default | Meaning |
|---|---|---|
| `initial` | none | State a key starts from. Without it, `ref` takes `initial=`, or the state type allows `None`. |
| `replicas` | `3`, or `1` when pinned | Nodes that keep a copy of each key |
| `write` | `"majority"` | `"one"`, `"majority"` or `"all"` |
| `pinned` | `False` | Each key runs on the node its ref names with `at=`; one copy |
| `mailbox` | `None` | Mailbox capacity; `None` is unbounded |
| `on_full` | `"refuse"` | What an `ask` meets at a full bounded mailbox: `"refuse"` raises `MailboxFull`, `"wait"` waits for room within `ask_timeout`; needs `mailbox` |
| `durable` | `None` | `"write"` saves every confirmed write to the system's store before `state.set` returns; a `timedelta` saves the latest confirmed write at most that long after it, and the last write of an activation and deletions at once. `None` keeps the state in memory only. |
| `idle_after` | the system's | Time without messages after which `inbox` ends |
| `ask_timeout` | the system's | Deadline of an `ask` to this type |
| `write_timeout` | the system's | How long a write of the state or an activation waits for replicas |
| `backoff` | the system's | Delay before restarting a body that raised |

### `Context[S, M]`

| Member | Meaning |
|---|---|
| `key` | Key of this entity |
| `state` | `State[S]`: `value`, `await set(state)`, `await update(change)`, `await delete()` |
| `inbox` | Messages in arrival order; ends after the type's `idle_after`, or the system's, without messages |
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
| `observer` | `None` | Called with every event of the node; `None` is a `LoggingObserver`. See [Observing a node](#observing-a-node). |
| `store` | `None` | Where durable types keep their state outside every process (see `casty.Store`). Every node of a cluster is given one reaching the same records. |
| `runtime` | `None` | Threads the transport runs on, shared with the systems and clients given the same `Runtime`; `None` starts a thread per core for this node alone. A system without `cluster` has no transport and uses none. See [Starting a node](#starting-a-node). |

`idle_after`, `backoff`, `ask_timeout` and `write_timeout` apply to the actor types that set none.

Members: `ref(actor, key, initial=..., at=...)`, `node`, `members`, `stats()`, `activations()`,
`await placement(actor, key, at=...)`, `await release(actor, key)`.

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
| `limits` | `Limits()` | `frame=256 KiB`, `message=4 MiB`, `window=256 KiB`; the same on every node and client |
| `heartbeat` | 1 s | Ping period |
| `suspect_after` | 5 s | Silence before `suspect`; must exceed `heartbeat` |
| `dead_after` | 5 s | Time as `suspect` before `dead` |
| `remove_after` | 1 min | Time as `dead` before removal; `None` disables it |
| `anti_entropy` | 30 s | Period of the full table exchange |
| `overlay` | `Overlay()` | `active=5`, `passive=30`, `join_walk=6`, `passive_walk=3`, `shuffle_every=10s`, `graft_after=500ms` |

With the defaults a dead node is detected in about ten seconds. Above roughly fifteen nodes, raise `suspect_after`.

Every timing of `ActorSystem`, `Cluster` and `Client` is a `timedelta`. A negative timing raises `ValueError`, and so
does a `heartbeat`, `anti_entropy`, `overlay.graft_after`, `overlay.shuffle_every` or `sync_every` of zero.

### `Client`

`seeds` (required), `name`, `tls`, `compression`, `address_map`, `limits`, `ask_timeout` (10 s), `sync_every` (5 s),
`observer`, `runtime`. Members: `ref`, `node`, `members`, `stats()`, `await placement(actor, key, at=...)`. A client
hosts nothing and can only be answered; see [Clients](#clients).

### Errors

`SchemaError`, `NotStarted`, `ActorFailed`, `MailboxFull`, `Unavailable`, `UnknownActor`, `Refused`,
`ReentrancyError`, `MessageTooLarge`, and the built-in `TimeoutError`. `MessageTooLarge` is a `ValueError`: a message,
an initial state or an answer is larger than `Limits.message`, and nothing was sent; a state write whose field name
leaves no room for its data in a message raises it too. `casty.collections` adds `ConfigurationError`. After a system
exits, `ref`, `tell` and `ask` raise `RuntimeError`; `stats`, `activations`, `placement` and `release` also raise it
before the system enters.

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
objects, to import an actor type by name, to pull the next item of a `merge` source, to call the store of durable
types, and to hand events to the observer. Bodies, store calls and the observer run on the asyncio loop.

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
2. If no replica has one, and for a durable type the store has none either, the key starts from the `initial` the ref
   offered, or from the default of the type.
3. The node writes the state back with an `@active` page, and creates the body task once that write is confirmed.
4. `ctx.inbox` yields messages in arrival order and ends after `idle_after` without any.
5. When the body returns, it runs again if messages arrived and it had read at least one. Otherwise the node clears
   `@active` and drops the activation. The read condition keeps a body that never reads its inbox from spinning. A key
   whose body deleted its state and did not write again leaves nothing, and a collection key back at the state it
   starts from is deleted instead.

`become` writes the new state with an `@behavior` page naming the new type. At the next inbox read the core cancels
the old body and starts the new one on the same mailbox. Later activations read `@behavior` to pick the function.

### Transport

One TCP connection per pair of nodes, opened by whoever sends first. On simultaneous dials, the handshake keeps the
connection of the node with the lower incarnation, so nothing queued is lost.

- The connection is multiplexed into streams. Frames have a 12-byte header: version, type, flags, stream, length.
- Replication has its own stream, so state transfers do not delay actor messages.
- Flow control is per stream, with credit returned as the receiver consumes bytes. `send` never waits; envelopes
  without credit queue on the connection.
- The handshake carries protocol versions, cluster name, node identity, role, offered compressors and the sizes of
  `Limits`. A mismatch surfaces as `Refused`.
- A lost connection drops what was not written, and the next send redials with exponential backoff. Nothing is resent,
  which is the origin of at-most-once delivery.
- Limits, by default: 256 KiB per frame, 4 MiB per message, 256 KiB initial window (`Cluster(limits=...)`,
  `Client(limits=...)`), keepalive after 15 s of silence. A page larger than a message is cut across as many as it
  takes and put back together before a replica applies it.

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
Every node computes this from its own member table; there is no coordinator. A node that does not see more than half
of the members `alive` owns none of its keys but the pinned ones: it may be on the small side of a partition.

- **The ring does not shrink when a node is `dead`.** Otherwise each side of a partition would recompute replicas
  among the nodes it sees and both would accept writes.
- **A replica set changes one node at a time.** When several members change together, the ring passes through the
  intermediate configurations, one member per step, in an order every node derives identically. With one replica
  swapped, the old owner's write quorum always overlaps the new set; with two it might not.

A pinned key, `@host:port/name`, is kept by the member advertising that address and by no other, whatever the ring;
its owner is that member while `alive` or `suspect`.

### Routing

A message goes to the owner the sender computes. The receiver checks that it is the owner in its own view, and
otherwise answers `WrongOwner`; the sender retries once with its current view, and a second refusal ends in
`Unavailable`.

Messages from one node to one key use one connection and one mailbox, which preserves their order while the owner does
not change. With `on_full="wait"`, a message that waited for room on another node can be overtaken by a later one from
the same node. A pending `ask` fails with `Unavailable` as soon as the sender sees the target as `dead`.

An `ask` made by a body carries the chain of keys waiting on it, at most 16, and the owner checks it before the mailbox.
A cancelled `ask` sends a cancellation for its request, routed like the request and through the transport even when the
key is on the same node, so it queues behind it. One that still arrives first, as after a `WrongOwner` retry, is kept
until the deadline of the type's `ask` and drops the request when it comes.

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
- **Deletion.** A deletion writes a tombstone, fenced like a save. Each replica keeps it for `leave_timeout`, then asks
  the other replicas of the key whether they keep anything older (replacing an older copy with the tombstone), and
  forgets it once all of them have answered. An activation that finds a tombstone as the latest write starts the key
  from `initial`.
- **Store.** A durable type's owner saves each write its replicas confirmed, versioned by its stamp; an activation
  reads the store once its replicas answered and promises again above the round the store's record was written in. A
  replica forgets a tombstone only once the store dropped the records it replaces.

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

Ranges and handovers travel as numbered streams of messages that each fit in `Limits.message`. An answer missing a
message is not counted and the source is asked again; each message of a handover is confirmed on its own, so the
confirmation of a large handover never exceeds the limit.

A pinned type has no ranges: a change of the ring pulls nothing of it and reports no `HandoffStarted`/`HandoffEnded`
for it, and its keys are never handed over. They are reattached and ended like any other key, their owner being the node
at the address they name while that node is `alive` or `suspect`, so an orderly leave ends them and a process that
comes back on the address starts them again.

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
- Reads are not linearizable. An isolated owner stops seeing the others `alive` about `dead_after` before they can
  declare it dead and take its keys over, and gives its keys up then, so it does not answer from a state they wrote
  past. That margin is timing, not consensus: a process paused through it, or two activations overlapping while a
  change of owner spreads, can still answer from a stale state.
- Side effects in a body can repeat after a restart or a change of owner.
- State lives in memory unless its type is `durable`: losing every replica of a key of any other type loses the key. A
  write of a `durable="write"` type that returned survives the loss of every replica and a restart of the whole
  cluster; with a period, losing every replica loses at most that period of writes.
- A durable type activates only while its store answers, and with `"write"` writes only while it answers:
  `Unavailable` also means the store did not answer, and a write that failed there may reappear, since its replicas
  had confirmed it.
- Every node of a cluster is given a store reaching the same records, and a store serves one cluster: two clusters, or
  a cluster and a system alone, on the same records mix their keys. The collections are not durable.
- A confirmed deletion does not come back from a replica that missed it: the others keep their tombstone until it has
  answered. A copy handed over by a node that no longer replicates the key, more than twice `leave_timeout` after the
  deletion, could bring it back.
- The store forgets a deleted key when the replicas forget its tombstone; a save of an older write still in flight
  more than twice `leave_timeout` after the deletion could put that state back in the store, where it is read only if
  every replica of the key is lost.
- A key of a pinned type has one copy and is never moved: it is unavailable while its node is down, and its state,
  unless the type is durable, goes with the node.
- Bodies may run in parallel on free-threaded Python. Mutable state shared between actors is not supported.

## Development

```sh
uv sync                     # builds the extension and installs the dev tools
uv run pytest -q
cargo test --workspace
uv run ruff check . && uv run pyright
cargo fmt --all --check && cargo clippy --workspace --all-targets
```

The Python suite runs real systems over TCP on loopback, with real crashes and partitions made by TCP proxies. There
are no mocks and no simulated clocks.

`CASTY_CHAOS=1 uv run pytest tests/chaos -s` runs the chaos suite: node processes on this machine crashed, restarted,
drained, joined, upgraded, partitioned, isolated, slowed and clock-skewed, and the whole cluster restarted at once, on
a seeded schedule, under traffic of a ledger, a ledger kept by a SQLite store, a pinned type, a counter, a dict, a set,
a queue, a barrier and a lock, with the invariants checked after each fault. `CHAOS_NODES`, `CHAOS_MINUTES` and
`CHAOS_SEED` shape a run; a failure prints its seed, its steps and the `CHAOS_REPLAY=…/run.json` that replays it. A
plain `pytest` never starts it.
