# casty

A minimalist, type-safe actor framework for Python 3.12+ with built-in distributed clustering.

Write an actor as a plain `async def`, send it typed messages, and run the same code in one process or on a cluster that places each actor on a node, replicates its state to a quorum and moves it as nodes come and go.

- **Typed end to end.** pyright checks every message, state and answer against the actor's annotation, and casty serializes them from it.
- **Virtual actors.** Nothing to spawn, supervise or stop: an actor is activated by its first message and deactivated when it goes idle.
- **Replicated state.** A write returns once a quorum of replicas confirms it, with the number of replicas and the write level chosen per actor type.
- **No coordinator.** Consistent hashing places keys, gossip keeps the membership, and keys move with their state when nodes join, leave or crash.
- **Durable when needed.** A type can also keep its state in a store, such as the SQLite one included.
- **Schedules and collections.** Timers that outlive the node running them, and replicated counters, registers, dicts, sets, multimaps, queues, semaphores, locks and barriers.
- **Rust runtime.** One compiled extension, no Python dependencies, free-threaded 3.14t included.

## Contents

- [Installation](#installation)
- [The actor model](#the-actor-model)
- [State](#state)
- [Clustered actors](#clustered-actors)
- [Ask](#ask)
- [Awaiting coroutines and blocking I/O](#awaiting-coroutines-and-blocking-io)
- [Schedulers](#schedulers)
- [Collections](#collections)
- [Failures](#failures)
- [Types](#types)
- [Guarantees and limits](#guarantees-and-limits)
- [Development](#development)
- [License](#license)

## Installation

```sh
uv add casty        # or: pip install casty
```

casty needs Python 3.12 or later and has no Python dependencies. Wheels cover every version with the GIL (`cp312-abi3`) and free-threaded 3.14t (`cp314-cp314t`); on a platform without one, the install builds from source and needs Rust 1.90 or later. [`examples`](examples) has one runnable program per topic.

## The actor model

An actor is a unit of computation that keeps its own state and talks to others only through messages. A few rules define it:

- **Actors.** An actor has an address, a mailbox and a behavior. In casty the behavior is an `async def` decorated with `@actor`, the actor type, and the address is that type plus a key, a string you choose, such as a user id. `(account, "acc-1")` and `(account, "acc-2")` are two actors running the same code, each with its own state and its own mailbox.
- **Messages.** Actors do not call each other or share memory. They send messages, immutable values queued in the mailbox of the receiver, and sending does not wait for the receiver.
- **One message at a time.** An actor handles its messages one by one, in the order they arrive. Nothing else runs on it meanwhile, so it needs no locks.
- **Private state.** Only the actor reads and writes its state; everything else reaches that state through messages to it.
- **Location transparency.** A reference to an actor names it, not where it runs. A message sent through it reaches the actor in this process or on any node of a cluster, and the code is the same.
- **Virtual actors.** casty never creates or destroys an actor. Every `(type, key)` can be addressed at any time: its first message activates it on the node that owns its key, and it is deactivated when it goes idle, with its state kept in between.
- **Failures restart from state.** When a body raises, casty restarts it from the last saved state, after a backoff. Local variables do not survive a restart, so what must survive goes in the state.

In code:

```python
import asyncio
from dataclasses import dataclass

from casty import ActorSystem, Context, actor


@dataclass(frozen=True)
class Greet:
    name: str


@actor
async def greeter(ctx: Context[None, Greet]) -> None:
    async for msg in ctx.inbox:
        print(f"hello, {msg.name}, from {ctx.key}")


async def main() -> None:
    async with ActorSystem() as system:
        ana = system.ref(greeter, "ana")
        ana.tell(Greet("world"))  # hello, world, from ana
        ana.tell(Greet("again"))  # hello, again, from ana
        system.ref(greeter, "bia").tell(Greet("world"))  # hello, world, from bia
        await asyncio.sleep(1)  # tell does not wait: leaving the block stops the system and drops what is queued


asyncio.run(main())
```

- `Context[None, Greet]` declares the state of `greeter`, none so far, and the messages it takes, each a frozen dataclass. pyright checks every `tell` against it, and casty serializes every message, even within one process.
- `system.ref(greeter, "ana")` is a `Ref`, the address of an actor. A ref is a value: a message or a state can carry one.
- `tell` queues the message and returns. Delivery is at most once, and the messages one node sends to one key arrive in the order they were sent.
- The body takes the next message when it reads `ctx.inbox` again. The inbox ends after `idle_after` without messages (one minute by default), the body returns, and the actor is deactivated.

## State

`ctx.state` holds the state of the actor, of the type `S` of `Context[S, M]`:

- `ctx.state.value` is the last saved state.
- `await ctx.state.set(new)` replaces it, and `await ctx.state.update(change)` saves what `change` makes of it and returns the result. Both return once the replicas confirm the write, and `value` changes only then.
- `await ctx.state.delete()` deletes it, and the key starts from its initial state again.

```python
@actor(initial=0)
async def greeter(ctx: Context[int, Greet]) -> None:
    async for msg in ctx.inbox:
        count = await ctx.state.update(lambda count: count + 1)
        print(f"hello, {msg.name}! greeting #{count} from {ctx.key}")
```

A key nothing has written starts from the `initial` given to `system.ref(actor, key, initial=...)`, otherwise from the `initial` of `@actor`, otherwise from `None` when the state type allows it. The state is serialized like the messages, so it is immutable: frozen dataclasses, tuples, `Mapping`s and `frozenset`s (see [Types](#types)).

`ctx.become(other, state)` hands the key to another actor type that takes the same messages, so each state of a state machine can be a function of its own; the key and its refs stay the same ([`examples/01-state-machine`](examples/01-state-machine)).

The state lives in memory on its replicas. A type declared `durable=` is also kept by the store of the system, which every node reaches, so its keys survive the loss of every replica and a restart of the whole cluster:

```python
from casty.sqlite import SQLiteStore


@actor(initial=0, durable="write")  # or a timedelta: saved at most that long after each write
async def account(ctx: Context[int, AccountMsg]) -> None: ...


async with SQLiteStore("accounts.db") as store, ActorSystem(cluster=cluster, store=store) as system: ...
```

A store is any object with async `load`, `save` and `drop` (`casty.Store`). `SQLiteStore` serves the nodes of one machine; `src/casty/sqlite.py` is the shape of one over a database that several machines reach. [`examples/11-durable-state`](examples/11-durable-state) stops a cluster and reads its keys back on new nodes.

## Clustered actors

To run on a cluster, give the system a `Cluster`: the address it listens on and the nodes it joins through. The actors do not change.

```python
from casty import Cluster

cluster = Cluster(bind="0.0.0.0:7400", advertise="10.0.0.5:7400", seeds=("10.0.0.4:7400",))
async with ActorSystem(cluster=cluster) as system:
    system.ref(greeter, "ana").tell(Greet("world"))  # printed on the node that runs ana
```

- Each key is placed on a node by consistent hashing, and every message for it is routed there, from whichever node sends it. There is no coordinator: every node computes the placement from its own view of the members, which gossip keeps up to date.
- A node that meets an actor type it does not know imports it by `module:qualname`. **All nodes of a cluster run the same code**, with actor types at module scope.
- Leaving `async with` normally is an orderly exit: the node finishes the messages in progress and hands its keys to the next replicas. A crash is detected by heartbeat, and its keys are activated elsewhere from their replicas.

Each key is kept by several nodes, as many as the actor type declares:

```python
@actor(initial=Ledger(), replicas=3, write="majority")  # the defaults
async def ledger(ctx: Context[Ledger, LedgerMsg]) -> None: ...
```

| `write` | A write returns when | Behaviour |
|---|---|---|
| `"majority"` | more than half of the replicas confirm | Survives the loss of a minority of the copies; the minority side of a partition cannot write |
| `"all"` | every replica confirms | Survives the loss of all copies but one; any replica away refuses writes |
| `"one"` | one replica confirms | Writes while any replica is reachable; a change of owner can lose confirmed writes |

A `Client` sends messages to a cluster without joining it. It hosts no actors and takes no part in membership, which fits web handlers, scripts and batch jobs; it can be answered, but not told.

```python
async with Client(seeds=("10.0.0.4:7400",)) as client:
    client.ref(greeter, "ana").tell(Greet("client"))
```

`@actor(pinned=True)` runs each key on the node its ref names, `system.ref(agent, "agent", at=member)`, with one copy, for an agent per node. `Cluster` also takes `tls` (mutual TLS against a CA), `compression` (zstd, lz4 or zlib) and `limits` (sizes of messages and frames, the same on every node). [`examples/06-distribution`](examples/06-distribution) runs a cluster on one machine and shows keys moving as nodes join and leave.

## Ask

`tell` does not wait for an answer. A message that has one is an `Askable[R]`, `R` being the type of its answer; it carries a `reply_to: Ref[R]`, and the actor answers by telling it:

```python
@dataclass(frozen=True)
class Count(Askable[int]):
    pass


@actor(initial=0)
async def greeter(ctx: Context[int, Greet | Count]) -> None:
    async for msg in ctx.inbox:
        match msg:
            case Greet(name):
                count = await ctx.state.update(lambda count: count + 1)
                print(f"hello, {name}! greeting #{count} from {ctx.key}")
            case Count():
                msg.reply_to.tell(ctx.state.value)
```

`ref.ask(msg)` sends the message with a `reply_to` of its own and waits for what is told to it. pyright reads the type of the answer from the message:

```python
async with ActorSystem() as system:
    ana = system.ref(greeter, "ana")
    ana.tell(Greet("world"))
    print(await ana.ask(Count()))  # 1
```

`reply_to` is keyword-only, so `case Count()` matches without it. `ask` raises `TimeoutError` after the `ask_timeout` of the type or of the system (10 seconds by default), and `asyncio.timeout` sets a shorter deadline.

`ref.ask` is how code outside the actors reads from them. A body that awaits it holds up its key until the answer arrives, and a cycle of such asks, a key asking itself or two keys asking each other, raises `ReentrancyError` at once. Between actors, `ctx.ask(target, msg, mapper)` gets the answer without holding up the key: it sends `msg` and returns, and the answer comes back to the actor as one more message, the one `mapper` makes of it.

```python
@actor(initial=0)
async def teller(ctx: Context[int, Transfer | Withdrawn]) -> None:
    async for msg in ctx.inbox:
        match msg:
            case Transfer(source, amount):
                source_account = ctx.system.ref(account, source)
                ctx.ask(source_account, Withdraw(amount), lambda ok, transfer=msg: Withdrawn(transfer, ok))
            case Withdrawn(transfer, ok):
                ...
```

The actor goes on with its mailbox meanwhile, so what it needs when the answer arrives goes in the message: a `lambda` reads the variables of the loop when it runs, and `transfer=msg` binds the message in hand. `failed=` turns what the `ask` raised, such as a `TimeoutError`, into a message too.

## Awaiting coroutines and blocking I/O

A body takes its next message when it reads `ctx.inbox` again, so what it waits for in between decides what waits with it. An `await` holds up the key: its messages queue until the coroutine returns, while the other keys of the node go on. A call that blocks, such as `requests.get`, `time.sleep` or a long computation, holds up the event loop, and with it every key of the node.

- **Await the writes of the key**: `state.set`, `update` and `delete`, `become` and `schedule`. The next message sees what they wrote.
- **Pipe every other coroutine** with `ctx.to_self(work, mapper)`, or with `ctx.ask` for an ask: a call to another service, a `gather` of several asks. The body goes on with its mailbox, and the result comes back as a message.
- **Run blocking calls in a thread** with `asyncio.to_thread`, piped like any other coroutine. A computation in Python holds the GIL on a thread as well, so it goes to a `ProcessPoolExecutor`, or to a thread on free-threaded 3.14t.

```python
@actor
async def profiles(ctx: Context[Profile | None, Refresh | Fetched | Resized]) -> None:
    async for msg in ctx.inbox:
        match msg:
            case Refresh():
                ctx.to_self(crm.fetch(ctx.key), Fetched)  # an async client: piped as it is
            case Fetched(profile):
                await ctx.state.set(profile)  # a write of the key: awaited
                ctx.to_self(asyncio.to_thread(resize, profile.avatar), Resized)  # a blocking call: piped from a thread
            case Resized(avatar):
                ...
```

A body does not have to wait for messages at all: it can read a stream or run a `TaskGroup`, and `ctx.merge(source)` interleaves its mailbox with any async iterable ([`examples/03-streams`](examples/03-streams), [`examples/08-consumers`](examples/08-consumers)).

## Schedulers

`ctx.schedule(name, delay, interval, message)` tells the actor `message` after `delay` and then every `interval`, or once when `interval` is `None`. The schedule is saved with the state and sent only by the node where the actor is active, so a key with a schedule is a singleton of the cluster that goes on when its node dies:

```python
@dataclass(frozen=True)
class Check:
    pass


@actor(initial=date(2026, 1, 1))  # the last day with a report
async def nightly(ctx: Context[date, Check]) -> None:
    await ctx.schedule("check", timedelta(0), timedelta(hours=1), Check())
    async for _ in ctx.inbox:
        today = datetime.now(UTC).date()
        if ctx.state.value < today:
            await ctx.state.set(today)
            await build_report(today)


system.ref(nightly, "report")  # creates the key and starts the body, once for the whole cluster
```

- Scheduling under a name in use replaces that schedule, so the body above schedules on every activation without adding a second one. `ctx.schedules` maps each name to its schedule, which `cancel()` stops.
- A key with schedules does not go idle. A schedule goes off only while its key is active: a key that is not wakes on a message or a `ref`, never on the clock.
- Times follow the wall clock, and a time missed by more than one interval is skipped. A change of owner can lose a time or send it twice, so the body checks its state, as `nightly` does with the last day built.
- Saving the day before building the report builds it at most once per day; building it first builds it at least once.

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

All take `replicas` (default 3). Data collections also take `write` (default `"majority"`); semaphores, locks and barriers always use `"majority"`.

- An absent entry is `MISSING`, distinct from a stored `None`.
- The first operation fixes the configuration of a name, and other settings or types raise `ConfigurationError`. Keep one `Collections` per system.
- Mutating calls return after the state is saved. A call that failed or timed out may have committed, and is not retried.
- `scan()` reads a `Dict`, `Set` or `MultiMap` of any size one segment at a time; it is not a snapshot. `size` and `clear` are not atomic either.
- `Semaphore` and `Lock` grant a `Lease` with a TTL and a fencing token. Expiry uses the wall clocks of the nodes, which must agree, and a lease can expire under a slow holder, so the protected resource must reject tokens older than the newest it has seen.
- `acquire` and `Barrier.wait` wait indefinitely; bound them with `asyncio.timeout`.
- The collections are not durable.

## Failures

| Situation | `ask` raises |
|---|---|
| The body raised while handling the message | `ActorFailed`; the key restarts from its last saved state after the `backoff` of its type |
| The `ask` closes a cycle of asks | `ReentrancyError` |
| A bounded mailbox (`@actor(mailbox=n)`) is full | `MailboxFull` |
| The message or its answer is larger than `Limits.message` | `MessageTooLarge` |
| Owner unreachable, too few replicas, or the store of a durable type not answering | `Unavailable`: the message may or may not have been processed |
| The owner does not have the actor type | `UnknownActor` |
| A ref received in a message points at a key never created | `NotStarted` |
| No answer within `ask_timeout`, or the `ask` is cancelled | `TimeoutError` / `CancelledError`; the key drops the message if still queued, or cancels the body working on it |

For `tell`, the same situations drop the message and report a `MessageDropped` to the observer of the system, which by default logs it. `ActorSystem(observer=...)` takes any callable of a `casty.Event`, and `system.stats()` reads what the node counts.

## Types

State, messages and answers are always serialized, even within one process. Types are checked when `@actor` runs, and an unsupported one raises `SchemaError` naming the field: `state: Cart.items: list[int] is not supported; use tuple[int, ...]`.

- Supported: `None`, `bool`, `int` (64 bits), `float`, `str`, `bytes`, `Literal`, timezone-aware `datetime` and `time`, `date`, `timedelta`, `Decimal`, `UUID`, enums, paths, `tuple`, `frozenset`, `Mapping`, frozen dataclasses (generic ones too), unions, `type` aliases and `Ref[T]`.
- Refused: `list`, `dict`, `set`, mutable dataclasses, plain classes, `Any`, `object` and `Callable`. Mutating the state in place would change it without replicating it; use `tuple`, `Mapping` and `frozenset`.
- Any other type travels as bytes through `Annotated[T, Opaque(encode, decode)]`, which casty does not read.

Dataclasses are encoded by field name, so two versions of the code can share a cluster: a missing field takes its default, an unknown field is ignored, and a missing field without a default raises `SchemaError`. Moving or renaming an actor body renames its type, and the keys saved under the old name are no longer reached.

## Guarantees and limits

- Delivery is at most once. Messages from one node to one key arrive in order while the owner of the key does not change.
- A confirmed write of the state is not lost while fewer replicas fail than the write level tolerates. A failed write may reappear, and `Unavailable` means the operation may or may not have happened.
- Two nodes may briefly run the same key during a change of owner, but with `"majority"` or `"all"` only one of them can save.
- Side effects in a body can repeat after a restart or a change of owner.
- Reads are not linearizable: the handover of a key relies on timing, not consensus, so a process paused long enough can answer from a stale state.
- State lives in memory unless its type is durable: losing every replica of a key of any other type loses the key. A write of a `durable="write"` type that returned survives a restart of the whole cluster.
- Leases and schedules follow the wall clocks of the nodes, which must agree.
- A key of a pinned type has one copy and is unavailable while its node is down.

## Development

```sh
uv sync                              # builds the extension and installs the dev tools
uv sync --reinstall-package casty    # rebuilds the extension after a change to the Rust code
make check                           # what CI checks: ruff, rustfmt, clippy, pyright, both suites and the docs
make help                            # the other targets: the suite on 3.14t, the chaos run, benchmarks, wheels
```

The Python suite runs real systems over TCP on loopback, with crashes and partitions made by TCP proxies. `CASTY_CHAOS=1 uv run pytest tests/chaos -s` runs the chaos suite.

## License

MIT, see [LICENSE](LICENSE).
