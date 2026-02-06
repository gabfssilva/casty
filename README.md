<p align="center">
  <img src="logo.png" alt="Casty" width="200">
</p>

<p align="center">
  <strong>Minimalist, type-safe actor framework for Python 3.13+</strong>
</p>

<p align="center">
  <a href="https://pypi.org/project/casty/"><img src="https://img.shields.io/pypi/v/casty.svg" alt="PyPI"></a>
  <a href="https://pypi.org/project/casty/"><img src="https://img.shields.io/pypi/pyversions/casty.svg" alt="Python"></a>
  <a href="https://github.com/gabfssilva/casty/actions"><img src="https://img.shields.io/github/actions/workflow/status/gabfssilva/casty/python-package.yml" alt="Tests"></a>
  <a href="https://github.com/gabfssilva/casty/blob/main/LICENSE"><img src="https://img.shields.io/github/license/gabfssilva/casty.svg" alt="License"></a>
</p>

<p align="center">
  <code>pip install casty</code>
</p>

## What is Casty?

Casty is an actor framework inspired by [Akka Typed](https://doc.akka.io/libraries/akka-core/current/typed/index.html) that brings the behavior-driven, functional actor model to Python. Instead of dealing with threads, locks, and shared state, you write small independent units called **actors** that communicate through **messages**.

**Why actors?**

- **No shared state** — Each actor owns its data. No locks, no race conditions.
- **Message-driven** — Actors communicate asynchronously through messages.
- **Fault-tolerant** — Actors can supervise each other and recover from failures.
- **Hierarchical** — Parent actors supervise children, forming a natural fault-tolerance tree.

**Why Casty?**

- **Functional** — Behaviors are values, not classes. State lives in closures, not mutable fields.
- **Type-safe** — Fully typed with `ActorRef[M]`, `Behavior[M]`, and Pyright strict mode.
- **Zero dependencies** — Pure Python, stdlib only. Nothing to install beyond Casty itself.
- **Built on asyncio** — Native async/await, no custom event loop needed.
- **Cluster-ready** — Distribute actors across nodes with `ClusteredActorSystem` and `Behaviors.sharded()`.

## Quick Start

```python
import asyncio
from dataclasses import dataclass
from casty import ActorContext, ActorSystem, Behavior, Behaviors

@dataclass(frozen=True)
class Greet:
    name: str

def greeter() -> Behavior[Greet]:
    async def receive(ctx: ActorContext[Greet], msg: Greet) -> Behavior[Greet]:
        print(f"Hello, {msg.name}!")
        return Behaviors.same()

    return Behaviors.receive(receive)

async def main() -> None:
    async with ActorSystem() as system:
        ref = system.spawn(greeter(), "greeter")
        ref.tell(Greet("Alice"))
        ref.tell(Greet("Bob"))
        await asyncio.sleep(0.1)

asyncio.run(main())
# Hello, Alice!
# Hello, Bob!
```

**What just happened?**

1. **Messages are frozen dataclasses** — `Greet` carries data immutably
2. **Behaviors are functions** — `greeter()` returns a `Behavior[Greet]` value
3. **State via closures** — returning `Behaviors.same()` keeps the current behavior; returning a new behavior transitions state
4. **`tell` is fire-and-forget** — messages are delivered asynchronously to the actor's mailbox

## Core Concepts

### Messages

Messages are frozen dataclasses. Group related messages with type aliases:

```python
@dataclass(frozen=True)
class Increment:
    amount: int

@dataclass(frozen=True)
class GetValue:
    reply_to: ActorRef[int]

type CounterMsg = Increment | GetValue
```

### Behaviors

Behaviors define how an actor handles messages. They are **values**, not classes — you compose them using the `Behaviors` factory:

| Factory | Purpose |
|---------|---------|
| `Behaviors.receive(handler)` | Handle messages with `async (ctx, msg) -> Behavior` |
| `Behaviors.setup(factory)` | Run initialization logic, then return the real behavior |
| `Behaviors.same()` | Keep the current behavior (return from handler) |
| `Behaviors.stopped()` | Stop the actor gracefully |
| `Behaviors.unhandled()` | Signal the message wasn't handled |
| `Behaviors.restart()` | Explicitly restart the actor |
| `Behaviors.supervise(behavior, strategy)` | Wrap with a supervision strategy |
| `Behaviors.with_lifecycle(behavior, ...)` | Attach lifecycle hooks |
| `Behaviors.sharded(entity_factory, num_shards=100)` | Create sharded entities distributed across cluster nodes |
| `Behaviors.event_sourced(...)` | Persist actor state as a sequence of events |
| `Behaviors.persisted(events)` | Return from command handler to persist events |

### Functional State

State is captured in closures. Returning a new behavior **is** the state transition:

```python
def counter(value: int = 0) -> Behavior[CounterMsg]:
    async def receive(ctx: ActorContext[CounterMsg], msg: CounterMsg) -> Behavior[CounterMsg]:
        match msg:
            case Increment(amount):
                return counter(value + amount)  # new state
            case GetValue(reply_to):
                reply_to.tell(value)
                return Behaviors.same()         # keep state

    return Behaviors.receive(receive)
```

No mutable fields. No `self.state = ...`. The function **is** the state.

### Actor References

When you spawn an actor, you get back a typed `ActorRef[M]`. It's the only way to communicate with an actor:

```python
ref = system.spawn(counter(), "my-counter")
ref.tell(Increment(10))  # fire-and-forget
```

### Request-Reply (Ask)

For request-response, include a `reply_to: ActorRef[R]` in the message. The system provides a convenience `ask`:

```python
result = await system.ask(
    ref,
    lambda reply_to: GetValue(reply_to=reply_to),
    timeout=5.0,
)
```

### Setup (Initialization)

Use `Behaviors.setup` when you need access to the `ActorContext` before handling messages — for example, to spawn children:

```python
def manager() -> Behavior[ManagerMsg]:
    async def setup(ctx: ActorContext[ManagerMsg]) -> Behavior[ManagerMsg]:
        workers: dict[str, ActorRef[Task]] = {}

        async def receive(ctx: ActorContext[ManagerMsg], msg: ManagerMsg) -> Behavior[ManagerMsg]:
            match msg:
                case Hire(name):
                    workers[name] = ctx.spawn(worker(name), name)
                    return Behaviors.same()
                case Assign(worker_name, task):
                    workers[worker_name].tell(Task(task))
                    return Behaviors.same()

        return Behaviors.receive(receive)

    return Behaviors.setup(setup)
```

### Supervision

Actors fail. Supervisors decide what happens next:

```python
from casty import Behaviors, OneForOneStrategy, Directive

strategy = OneForOneStrategy(
    max_restarts=3,
    within=60.0,
    decider=lambda exc: Directive.restart,
)

supervised = Behaviors.supervise(unreliable_worker(), strategy)
ref = system.spawn(supervised, "worker")
```

Directives:

| Directive | Behavior |
|-----------|----------|
| `Directive.restart` | Restart the actor, reset state |
| `Directive.stop` | Stop the actor permanently |
| `Directive.escalate` | Escalate to parent supervisor |

`OneForOneStrategy` tracks restart counts within a time window. If the limit is exceeded, the actor is stopped.

### Lifecycle Hooks

Attach hooks to observe actor lifecycle transitions:

```python
def my_actor() -> Behavior[str]:
    async def pre_start(ctx: ActorContext[str]) -> None:
        print("starting up")

    async def post_stop(ctx: ActorContext[str]) -> None:
        print("shutting down")

    return Behaviors.with_lifecycle(
        Behaviors.receive(receive),
        pre_start=pre_start,
        post_stop=post_stop,
    )
```

Available hooks: `pre_start`, `post_stop`, `pre_restart`, `post_restart`.

### Death Watch

Actors can watch other actors and receive a `Terminated` message when they stop:

```python
from casty import Terminated

def watcher_behavior() -> Behavior[Terminated]:
    async def setup(ctx: ActorContext[Terminated]) -> Behavior[Terminated]:
        child = ctx.spawn(some_actor(), "child")
        ctx.watch(child)

        async def receive(ctx: ActorContext[Terminated], msg: Terminated) -> Behavior[Terminated]:
            print(f"actor {msg.ref} has stopped")
            return Behaviors.same()

        return Behaviors.receive(receive)

    return Behaviors.setup(setup)
```

### Event Stream

The system-wide event bus for observability:

```python
from casty import ActorStarted, ActorStopped, DeadLetter

system.event_stream.subscribe(ActorStarted, lambda e: print(f"started: {e.ref}"))
system.event_stream.subscribe(ActorStopped, lambda e: print(f"stopped: {e.ref}"))
system.event_stream.subscribe(DeadLetter, lambda e: print(f"dead letter: {e.message}"))
```

Events: `ActorStarted`, `ActorStopped`, `ActorRestarted`, `DeadLetter`, `UnhandledMessage`.

### Mailbox Configuration

Control backpressure with bounded mailboxes:

```python
from casty import Mailbox, MailboxOverflowStrategy

ref = system.spawn(
    my_behavior(),
    "bounded-actor",
    mailbox=Mailbox(capacity=100, overflow=MailboxOverflowStrategy.drop_oldest),
)
```

Overflow strategies: `drop_new` (default), `drop_oldest`, `backpressure`.

### Cluster Sharding

Distribute actors across multiple nodes with `ClusteredActorSystem` and `Behaviors.sharded()`. Entities are automatically partitioned into shards and routed to the correct node — the API feels identical to local spawning:

```python
from casty import Behaviors, ShardEnvelope
from casty.sharding import ClusteredActorSystem

# Each node in the cluster runs the same code
async with ClusteredActorSystem(
    name="bank",
    host="10.0.0.1",
    port=25520,
    seed_nodes=[("10.0.0.2", 25520), ("10.0.0.3", 25520)],
) as system:
    # spawn returns ActorRef[ShardEnvelope[AccountMsg]]
    accounts = system.spawn(Behaviors.sharded(account_entity, num_shards=10), "accounts")

    # send messages — routing is transparent
    accounts.tell(ShardEnvelope("alice", Deposit(100)))

    # ask works the same way
    balance = await system.ask(
        accounts,
        lambda r: ShardEnvelope("alice", GetBalance(reply_to=r)),
        timeout=2.0,
    )
```

Each entity (e.g. `"alice"`) is hashed to a shard, and the coordinator assigns shards to nodes using a least-shard-first strategy. Messages to remote entities are forwarded automatically. Every node in the cluster runs the same code — just change `host` per machine.

### Event Sourcing

Persist actor state as a sequence of events instead of storing state directly. On recovery, events are replayed from the journal to reconstruct state — no data loss, full audit trail:

```python
from casty import Behaviors
from casty.journal import InMemoryJournal
from casty.actor import SnapshotEvery

@dataclass(frozen=True)
class AccountState:
    balance: int

# Events — what gets persisted
@dataclass(frozen=True)
class Deposited:
    amount: int

@dataclass(frozen=True)
class Withdrawn:
    amount: int

# Pure function: folds an event into state (used for replay too)
def on_event(state: AccountState, event: Deposited | Withdrawn) -> AccountState:
    match event:
        case Deposited(amount):
            return AccountState(balance=state.balance + amount)
        case Withdrawn(amount):
            return AccountState(balance=state.balance - amount)
    return state

# Async handler: decides what events to persist
async def on_command(ctx, state: AccountState, cmd) -> Behavior:
    match cmd:
        case Deposit(amount):
            return Behaviors.persisted(events=[Deposited(amount)])
        case Withdraw(amount) if state.balance >= amount:
            return Behaviors.persisted(events=[Withdrawn(amount)])
        case GetBalance(reply_to):
            reply_to.tell(state.balance)
            return Behaviors.same()  # no events, no persistence

journal = InMemoryJournal()

def account(entity_id: str) -> Behavior:
    return Behaviors.event_sourced(
        entity_id=entity_id,
        journal=journal,
        initial_state=AccountState(balance=0),
        on_event=on_event,
        on_command=on_command,
        snapshot_policy=SnapshotEvery(n_events=100),  # snapshot every 100 events
    )
```

The key difference from a regular behavior: the command handler receives an explicit `state` parameter managed by the framework. This is necessary because event sourcing must replay events to reconstruct state — closures can't be replayed.

The `EventJournal` protocol is storage-agnostic. `InMemoryJournal` is included for tests and development; implement the protocol for any database backend.

### Shard Replication

Add passive replicas to sharded entities for fault tolerance. The primary pushes persisted events to replicas after each command; replicas maintain their own journal copy and can be promoted on failover:

```python
from casty.replication import ReplicationConfig

accounts = node.spawn(
    Behaviors.sharded(
        account,
        num_shards=10,
        replication=ReplicationConfig(
            replicas=2,        # 2 passive replicas per entity
            min_acks=1,        # wait for 1 replica ack before confirming
            ack_timeout=5.0,   # seconds to wait for acks
        ),
    ),
    "accounts",
)
```

The coordinator allocates primary and replicas on different nodes and handles failover: when a primary's node goes down, the replica with the highest sequence number is promoted.

| `min_acks` | Behavior |
|------------|----------|
| `0` | Fire-and-forget — lowest latency, eventual consistency |
| `1+` | Wait for N acks — stronger durability guarantee |

### Distributed Data Structures

Higher-level data structures backed by cluster sharding. Call `system.distributed()` to get a facade, then create counters, maps, sets, and queues that are automatically distributed across nodes:

```python
from dataclasses import dataclass
from casty.sharding import ClusteredActorSystem
from casty.journal import InMemoryJournal

@dataclass(frozen=True)
class User:
    name: str
    email: str

async with ClusteredActorSystem(
    name="my-app", host="10.0.0.1", port=25520,
    seed_nodes=[("10.0.0.2", 25520)],
) as system:
    d = system.distributed()

    # Counter — increment, decrement, get
    views = d.counter("page-views", shards=50)
    await views.increment(100)      # -> 100
    await views.decrement(10)       # -> 90
    await views.get()               # -> 90

    # Dict — typed key-value map, one sharded entity per key
    users = d.map[str, User]("users", shards=50)
    await users.put("alice", User("Alice", "a@x.com"))   # -> None (previous)
    await users.get("alice")                              # -> User(...)
    await users.contains("alice")                         # -> True
    await users.delete("alice")                           # -> True (existed)

    # Set — add, remove, contains, size
    tags = d.set[str]("active-tags")
    await tags.add("python")        # -> True  (added)
    await tags.add("python")        # -> False (already present)
    await tags.size()               # -> 1

    # Queue — FIFO enqueue, dequeue, peek, size
    jobs = d.queue[str]("work-queue")
    await jobs.enqueue("task-1")
    await jobs.peek()               # -> "task-1" (doesn't remove)
    await jobs.dequeue()            # -> "task-1" (removes)
    await jobs.dequeue()            # -> None (empty)
```

All creation methods accept `shards` (default `100`) for distribution granularity and `timeout` (default `5.0s`) for operations. The same structure can be accessed from any node — just use the same `name` and `shards`.

To make structures persistent, pass an `EventJournal` to the facade. Every mutation is persisted as an event and replayed on recovery:

```python
d = system.distributed(journal=InMemoryJournal())
counter = d.counter("hits")
await counter.increment(10)
# actor restarts → replays Incremented(10) → recovers value=10
```

`InMemoryJournal` is included for development; implement the `EventJournal` protocol for any database backend.

## State Machines

Because behaviors are values, state machines are natural — each state is a function:

```python
def empty_cart() -> Behavior[CartMsg]:
    async def receive(ctx, msg):
        match msg:
            case AddItem(item):
                return active_cart({item.name: item})  # transition!
            case Checkout():
                print("cannot checkout an empty cart")
                return Behaviors.same()
    return Behaviors.receive(receive)

def active_cart(items: dict[str, Item]) -> Behavior[CartMsg]:
    async def receive(ctx, msg):
        match msg:
            case AddItem(item):
                return active_cart({**items, item.name: item})
            case Checkout(reply_to):
                reply_to.tell(Receipt(items))
                return checked_out()  # transition!
    return Behaviors.receive(receive)

def checked_out() -> Behavior[CartMsg]:
    # terminal state — rejects further modifications
    ...
```

No enums, no if/else chains. The behavior **is** the state.

## Contributing

```bash
git clone https://github.com/gabfssilva/casty
cd casty
uv sync
uv run pytest
uv run pyright src/casty/
uv run ruff check src/ tests/
```

## License

MIT — see [LICENSE](LICENSE) for details.
