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
  <a href="https://github.com/gabfssilva/casty/blob/main/LICENSE"><img src="https://img.shields.io/github/license/gabfssilva.svg" alt="License"></a>
</p>

<p align="center">
  <code>pip install casty</code>
</p>

---

## What is Casty?

Casty is an actor framework inspired by [Akka Typed](https://doc.akka.io/libraries/akka-core/current/typed/index.html) that brings the behavior-driven, functional actor model to Python. Instead of dealing with threads, locks, and shared mutable state, you model your system as independent units called **actors** that communicate exclusively through **messages**.

The actor model was introduced by Carl Hewitt in 1973 and later battle-tested in Erlang/OTP — the foundation of telecom systems achieving 99.9999% uptime — and in Akka, which powers JVM systems processing millions of messages per second. Casty brings these ideas to Python's `asyncio` ecosystem with a design that embraces Python's strengths: closures for state, frozen dataclasses for immutability, type aliases for exhaustive matching, and Protocols for extensibility.

Casty makes several deliberate design choices that distinguish it from other actor libraries:

- **Behaviors are values, not classes.** There is no `Actor` base class to subclass. A behavior is a frozen dataclass produced by a factory function, composed and transformed like any other value.
- **State lives in closures, not mutable fields.** An actor's state is captured in the closure of its message handler. State transitions happen by returning a new behavior that closes over the new state.
- **Immutability by default.** All messages, behaviors, events, and configurations are frozen dataclasses. If a value can't be mutated after creation, it's safe to pass between actors without copying.
- **Zero external dependencies.** Pure Python, stdlib only. Nothing to install beyond Casty itself.
- **Type-safe end-to-end.** `ActorRef[M]`, `Behavior[M]`, and PEP 695 type aliases ensure that the type checker catches message mismatches at development time, not at runtime.
- **Cluster-ready.** Distribute actors across nodes with `ClusteredActorSystem`, gossip-based membership, phi accrual failure detection, and automatic shard rebalancing.

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

Four things happened here:

1. **`Greet` is a frozen dataclass.** Messages are immutable values — safe to send between actors without defensive copying.
2. **`greeter()` returns a `Behavior[Greet]`.** The behavior is a value produced by `Behaviors.receive()`, not a class instance. The actor system interprets this value to wire up the message handler.
3. **`Behaviors.same()` means "keep the current behavior."** Returning a different behavior would transition the actor to a new state. This is the fundamental mechanism for state management.
4. **`tell()` is fire-and-forget.** Messages are enqueued in the actor's mailbox and processed asynchronously, one at a time.

The sections that follow build on these foundations progressively — from functional state management through fault tolerance, event sourcing, and distributed clustering. Each section introduces one concept, explains why it exists in the actor model, and shows how Casty implements it.

---

## Actors and Messages

In the actor model, an **actor** is the fundamental unit of computation. When an actor receives a message, it can do exactly three things:

1. **Send messages** to other actors it knows about.
2. **Create new actors** (its children).
3. **Designate the behavior** for the next message it receives.

That's the entire model. There is no shared memory between actors, no synchronization primitives, no mutex. Concurrency emerges naturally: each actor processes one message at a time from its mailbox, and multiple actors run concurrently on the asyncio event loop.

A **message** is any value sent to an actor. In Casty, messages are frozen dataclasses — immutable by construction. Related messages are grouped using PEP 695 type aliases, which enables exhaustive pattern matching via `match` statements:

```python
from dataclasses import dataclass
from casty import ActorRef

@dataclass(frozen=True)
class Deposit:
    amount: int

@dataclass(frozen=True)
class Withdraw:
    amount: int
    reply_to: ActorRef[str]

@dataclass(frozen=True)
class GetBalance:
    reply_to: ActorRef[int]

type AccountMsg = Deposit | Withdraw | GetBalance
```

The type alias `AccountMsg` is a union of all messages the actor accepts. When handling messages with `match`, the type checker verifies that all variants are covered. If a new message type is added to the union but not handled, Pyright reports an error at development time.

## Behaviors as Values

In most actor frameworks, an actor is defined by subclassing a base class and overriding a `receive` method. Casty takes a different approach, inspired by Akka Typed: **behaviors are values**, not classes.

A behavior is a frozen dataclass that describes how an actor processes messages. You compose behaviors using the `Behaviors` factory:

| Factory | Purpose |
|---------|---------|
| `Behaviors.receive(handler)` | Create a behavior from an async message handler `(ctx, msg) -> Behavior` |
| `Behaviors.setup(factory)` | Run initialization logic with access to `ActorContext`, then return the real behavior |
| `Behaviors.same()` | Keep the current behavior unchanged (returned from a message handler) |
| `Behaviors.stopped()` | Stop the actor gracefully |
| `Behaviors.unhandled()` | Signal that the message was not handled |
| `Behaviors.restart()` | Explicitly restart the actor |
| `Behaviors.supervise(behavior, strategy)` | Wrap a behavior with a supervision strategy |
| `Behaviors.with_lifecycle(behavior, ...)` | Attach lifecycle hooks (pre_start, post_stop, etc.) |
| `Behaviors.event_sourced(...)` | Persist actor state as a sequence of events |
| `Behaviors.persisted(events)` | Return from a command handler to persist events and update state |
| `Behaviors.sharded(entity_factory, ...)` | Distribute entities across cluster nodes via sharding |

Because behaviors are values, they compose naturally. A behavior can be wrapped with supervision, decorated with lifecycle hooks, and backed by event sourcing — all through function composition, not class inheritance.

## Functional State

The actor model requires each actor to designate the behavior for its *next* message. This is the mechanism for state transitions. In Casty, state is captured in closures: the message handler closes over the current state, and returning a new behavior with different closed-over values constitutes a state transition.

Consider a bank account actor that tracks a balance:

```python
import asyncio
from dataclasses import dataclass
from casty import ActorContext, ActorRef, ActorSystem, Behavior, Behaviors

@dataclass(frozen=True)
class Deposit:
    amount: int

@dataclass(frozen=True)
class GetBalance:
    reply_to: ActorRef[int]

type AccountMsg = Deposit | GetBalance

def bank_account(balance: int = 0) -> Behavior[AccountMsg]:
    async def receive(ctx: ActorContext[AccountMsg], msg: AccountMsg) -> Behavior[AccountMsg]:
        match msg:
            case Deposit(amount):
                return bank_account(balance + amount)
            case GetBalance(reply_to):
                reply_to.tell(balance)
                return Behaviors.same()

    return Behaviors.receive(receive)

async def main() -> None:
    async with ActorSystem() as system:
        account = system.spawn(bank_account(), "account")
        account.tell(Deposit(100))
        account.tell(Deposit(50))
        await asyncio.sleep(0.1)

asyncio.run(main())
```

The line `return bank_account(balance + amount)` is the state transition. It creates a new `ReceiveBehavior` whose handler closes over `balance + amount`. There is no mutable field, no `self.balance = ...`, no `nonlocal`. The function call **is** the state transition.

This approach — called **behavior recursion** — makes state transitions explicit, traceable, and impossible to corrupt through accidental sharing. Each invocation of `bank_account(n)` produces a completely independent behavior value.

## Request-Reply

Actors communicate via fire-and-forget `tell()` by default. When a response is needed, the actor model uses the **reply-to pattern**: the sender includes its own `ActorRef` in the message so the receiver can send a response back.

```python
import asyncio
from dataclasses import dataclass
from casty import ActorContext, ActorRef, ActorSystem, Behavior, Behaviors

@dataclass(frozen=True)
class Deposit:
    amount: int

@dataclass(frozen=True)
class Withdraw:
    amount: int
    reply_to: ActorRef[str]

@dataclass(frozen=True)
class GetBalance:
    reply_to: ActorRef[int]

type AccountMsg = Deposit | Withdraw | GetBalance

def bank_account(balance: int = 0) -> Behavior[AccountMsg]:
    async def receive(ctx: ActorContext[AccountMsg], msg: AccountMsg) -> Behavior[AccountMsg]:
        match msg:
            case Deposit(amount):
                return bank_account(balance + amount)
            case Withdraw(amount, reply_to) if balance >= amount:
                reply_to.tell("ok")
                return bank_account(balance - amount)
            case Withdraw(_, reply_to):
                reply_to.tell(f"insufficient funds (balance={balance})")
                return Behaviors.same()
            case GetBalance(reply_to):
                reply_to.tell(balance)
                return Behaviors.same()

    return Behaviors.receive(receive)

async def main() -> None:
    async with ActorSystem() as system:
        account = system.spawn(bank_account(), "account")
        account.tell(Deposit(100))

        balance = await system.ask(
            account,
            lambda reply_to: GetBalance(reply_to=reply_to),
            timeout=5.0,
        )
        print(f"Balance: {balance}")  # Balance: 100

        result = await system.ask(
            account,
            lambda reply_to: Withdraw(200, reply_to=reply_to),
            timeout=5.0,
        )
        print(f"Withdraw: {result}")  # Withdraw: insufficient funds (balance=100)

asyncio.run(main())
```

`system.ask()` is a convenience that creates a temporary actor behind the scenes, passes its `ActorRef` as the `reply_to` field, and awaits the response with a timeout. The underlying mechanism is still `tell` — `ask` simply wraps the reply-to pattern into a coroutine.

> **Warning:** `system.ask()` is meant for use **outside** actors — from `main()`, HTTP handlers, or other external code. If called inside an actor's receive handler, it blocks that actor's mailbox until the response arrives (actors process one message at a time), which can lead to deadlocks.
>
> Inside actors, pass `ctx.self` as `reply_to` and handle the response as a regular message:

```python
@dataclass(frozen=True)
class Deposit:
    amount: int

@dataclass(frozen=True)
class GetBalance:
    reply_to: ActorRef[int]

type AccountMsg = Deposit | GetBalance

def bank_account(balance: int = 0) -> Behavior[AccountMsg]:
    async def receive(ctx: ActorContext[AccountMsg], msg: AccountMsg) -> Behavior[AccountMsg]:
        match msg:
            case Deposit(amount):
                return bank_account(balance + amount)
            case GetBalance(reply_to):
                reply_to.tell(balance)
                return Behaviors.same()

    return Behaviors.receive(receive)

@dataclass(frozen=True)
class CheckBalance:
    account: ActorRef[AccountMsg]

type MonitorMsg = CheckBalance | int

def monitor() -> Behavior[MonitorMsg]:
    async def receive(ctx: ActorContext[MonitorMsg], msg: MonitorMsg) -> Behavior[MonitorMsg]:
        match msg:
            case CheckBalance(account):
                # Non-blocking: sends the request and keeps processing
                account.tell(GetBalance(reply_to=ctx.self))
                return Behaviors.same()
            case int() as balance:
                print(f"Balance: {balance}")
                return Behaviors.same()

    return Behaviors.receive(receive)

async def main() -> None:
    async with ActorSystem() as system:
        acc = system.spawn(bank_account(), "account")
        acc.tell(Deposit(100))

        mon = system.spawn(monitor(), "monitor")
        mon.tell(CheckBalance(account=acc))  # prints "Balance: 100"
```

## Actor Hierarchies

Actors form a tree. When an actor spawns a child via `ctx.spawn()`, it becomes the parent. This hierarchy is not merely organizational — it is the foundation of fault tolerance. A parent is responsible for the lifecycle of its children: it can stop them, watch them for termination, and define supervision strategies for their failures.

`Behaviors.setup()` provides access to the `ActorContext` at initialization time, which is where children are typically spawned:

```python
import asyncio
from dataclasses import dataclass
from casty import ActorContext, ActorRef, ActorSystem, Behavior, Behaviors, Terminated

@dataclass(frozen=True)
class Task:
    description: str

@dataclass(frozen=True)
class Hire:
    name: str

@dataclass(frozen=True)
class Assign:
    worker: str
    task: str

type ManagerMsg = Hire | Assign | Terminated

def worker(name: str) -> Behavior[Task]:
    async def receive(ctx: ActorContext[Task], msg: Task) -> Behavior[Task]:
        print(f"[{name}] working on: {msg.description}")
        return Behaviors.same()

    return Behaviors.receive(receive)

def manager() -> Behavior[ManagerMsg]:
    async def setup(ctx: ActorContext[ManagerMsg]) -> Behavior[ManagerMsg]:
        workers: dict[str, ActorRef[Task]] = {}

        async def receive(ctx: ActorContext[ManagerMsg], msg: ManagerMsg) -> Behavior[ManagerMsg]:
            match msg:
                case Hire(name):
                    ref = ctx.spawn(worker(name), name)
                    ctx.watch(ref)
                    workers[name] = ref
                    return Behaviors.same()
                case Assign(worker_name, task):
                    if worker_name in workers:
                        workers[worker_name].tell(Task(task))
                    return Behaviors.same()
                case Terminated(ref):
                    print(f"Worker stopped: {ref.address}")
                    return Behaviors.same()

        return Behaviors.receive(receive)

    return Behaviors.setup(setup)

async def main() -> None:
    async with ActorSystem() as system:
        mgr = system.spawn(manager(), "manager")
        mgr.tell(Hire("alice"))
        mgr.tell(Hire("bob"))
        await asyncio.sleep(0.1)

        mgr.tell(Assign("alice", "implement login"))
        mgr.tell(Assign("bob", "write tests"))
        await asyncio.sleep(0.1)

asyncio.run(main())
```

**Death watch** is the mechanism by which an actor observes the termination of another actor. Calling `ctx.watch(ref)` registers interest; when the watched actor stops — whether gracefully or due to failure — the watcher receives a `Terminated` message containing the stopped actor's ref.

## Supervision

In traditional programming, errors propagate upward through the call stack via exceptions. In the actor model, errors propagate **to the supervisor**. This is the "let it crash" philosophy pioneered by Erlang/OTP: instead of writing defensive code within the actor to handle every possible failure, let the actor fail fast and let its supervisor decide the recovery strategy.

A supervisor is any actor that has spawned children. The supervision strategy defines what happens when a child fails:

| Directive | Effect |
|-----------|--------|
| `Directive.restart` | Restart the actor with its initial behavior, resetting state |
| `Directive.stop` | Stop the actor permanently |
| `Directive.escalate` | Propagate the failure to the next supervisor up the hierarchy |

`OneForOneStrategy` supervises each child independently. It tracks restart counts within a configurable time window — if a child exceeds the limit, it is stopped instead of restarted:

```python
import asyncio
from casty import ActorContext, ActorSystem, Behavior, Behaviors, Directive, OneForOneStrategy

def unreliable_worker() -> Behavior[str]:
    async def receive(ctx: ActorContext[str], msg: str) -> Behavior[str]:
        if msg == "crash":
            raise RuntimeError("something went wrong")
        print(f"Processed: {msg}")
        return Behaviors.same()

    return Behaviors.receive(receive)

async def main() -> None:
    strategy = OneForOneStrategy(
        max_restarts=3,
        within=60.0,
        decider=lambda exc: Directive.restart,
    )

    async with ActorSystem() as system:
        ref = system.spawn(
            Behaviors.supervise(unreliable_worker(), strategy),
            "worker",
        )

        ref.tell("crash")          # fails, supervisor restarts
        await asyncio.sleep(0.2)

        ref.tell("hello")          # succeeds — actor recovered
        await asyncio.sleep(0.1)

asyncio.run(main())
```

The `decider` function receives the exception and returns a directive. This allows fine-grained control: restart on transient errors, stop on fatal ones, escalate on unknown failures.

An important interaction to note: when a supervised actor without event sourcing is restarted, its state is reset to the initial behavior. This means the bank account from previous sections would lose its balance on restart. Event sourcing (covered later) solves this by replaying persisted events to reconstruct state after a restart.

## Actor Runtime

The following concepts control the operational behavior of actors. They are grouped here because each is important but self-contained — none changes the fundamental mental model established in previous sections.

### Lifecycle Hooks

Actors transition through a defined lifecycle: start, stop, and (if supervised) restart. Lifecycle hooks allow executing side effects at each boundary — acquiring resources on start, releasing them on stop, logging on restart:

```python
from casty import ActorContext, Behavior, Behaviors

def my_actor() -> Behavior[str]:
    async def pre_start(ctx: ActorContext[str]) -> None:
        ctx.log.info("Actor starting")

    async def post_stop(ctx: ActorContext[str]) -> None:
        ctx.log.info("Actor stopped")

    async def receive(ctx: ActorContext[str], msg: str) -> Behavior[str]:
        return Behaviors.same()

    return Behaviors.with_lifecycle(
        Behaviors.receive(receive),
        pre_start=pre_start,
        post_stop=post_stop,
    )
```

Available hooks: `pre_start` (before first message), `post_stop` (after final message), `pre_restart` (before restart), `post_restart` (after restart, before first message of new incarnation).

### Event Stream

The `EventStream` is a system-wide publish/subscribe bus for observability. Every significant actor lifecycle event is published automatically:

```python
from casty import ActorStarted, ActorStopped, DeadLetter

system.event_stream.subscribe(ActorStarted, lambda e: print(f"Started: {e.ref}"))
system.event_stream.subscribe(ActorStopped, lambda e: print(f"Stopped: {e.ref}"))
system.event_stream.subscribe(DeadLetter, lambda e: print(f"Dead letter: {e.message}"))
```

A `DeadLetter` is published when a message is sent to an actor that has already stopped. This is valuable for debugging message routing issues.

Available events: `ActorStarted`, `ActorStopped`, `ActorRestarted`, `DeadLetter`, `UnhandledMessage`. In clustered mode, additional events are published: `MemberUp`, `MemberLeft`, `UnreachableMember`, `ReachableMember`.

### Mailbox Configuration

Each actor has a mailbox — a bounded queue that buffers incoming messages. When messages arrive faster than the actor can process them, the overflow strategy determines what happens:

```python
from casty import Mailbox, MailboxOverflowStrategy

ref = system.spawn(
    my_behavior(),
    "bounded-actor",
    mailbox=Mailbox(capacity=100, overflow=MailboxOverflowStrategy.drop_oldest),
)
```

| Strategy | Behavior |
|----------|----------|
| `drop_new` (default) | Discard the incoming message when the mailbox is full |
| `drop_oldest` | Discard the oldest message in the mailbox to make room for the new one |
| `backpressure` | Raise `asyncio.QueueFull`, propagating pressure to the sender |

### Scheduling

The scheduler is an actor (spawned lazily by the system) that manages timed message delivery. Two patterns are supported: periodic ticks and one-shot delays.

```python
# Send a BalanceReport to the account actor every 30 seconds
system.tick("report", account_ref, BalanceReport(), interval=30.0)

# Send a Timeout message after 5 seconds
system.schedule("timeout", account_ref, Timeout(), delay=5.0)

# Cancel a scheduled task
system.cancel_schedule("report")
```

Scheduled tasks are identified by a string key. Scheduling a new task with the same key cancels the previous one.

## State Machines

Because behaviors are values and state transitions are function calls, finite state machines emerge as a natural pattern. Each state is a behavior function, each transition is a return value. No enum, no conditional dispatch on a status field — the behavior **is** the state.

```python
import asyncio
from dataclasses import dataclass
from casty import ActorContext, ActorRef, ActorSystem, Behavior, Behaviors

@dataclass(frozen=True)
class Item:
    name: str
    price: float

@dataclass(frozen=True)
class Receipt:
    items: tuple[Item, ...]
    total: float

@dataclass(frozen=True)
class AddItem:
    item: Item

@dataclass(frozen=True)
class Checkout:
    reply_to: ActorRef[Receipt]

@dataclass(frozen=True)
class GetTotal:
    reply_to: ActorRef[float]

type CartMsg = AddItem | Checkout | GetTotal

def empty_cart() -> Behavior[CartMsg]:
    async def receive(ctx: ActorContext[CartMsg], msg: CartMsg) -> Behavior[CartMsg]:
        match msg:
            case AddItem(item):
                return active_cart({item.name: item})
            case Checkout():
                return Behaviors.same()  # cannot checkout empty cart
            case GetTotal(reply_to):
                reply_to.tell(0.0)
                return Behaviors.same()

    return Behaviors.receive(receive)

def active_cart(items: dict[str, Item]) -> Behavior[CartMsg]:
    async def receive(ctx: ActorContext[CartMsg], msg: CartMsg) -> Behavior[CartMsg]:
        match msg:
            case AddItem(item):
                return active_cart({**items, item.name: item})
            case Checkout(reply_to):
                all_items = tuple(items.values())
                total = sum(i.price for i in all_items)
                reply_to.tell(Receipt(items=all_items, total=total))
                return checked_out()
            case GetTotal(reply_to):
                reply_to.tell(sum(i.price for i in items.values()))
                return Behaviors.same()

    return Behaviors.receive(receive)

def checked_out() -> Behavior[CartMsg]:
    async def receive(ctx: ActorContext[CartMsg], msg: CartMsg) -> Behavior[CartMsg]:
        match msg:
            case GetTotal(reply_to):
                reply_to.tell(0.0)
                return Behaviors.same()
            case _:
                return Behaviors.same()  # terminal state, ignore modifications

    return Behaviors.receive(receive)

async def main() -> None:
    async with ActorSystem() as system:
        cart = system.spawn(empty_cart(), "cart")

        cart.tell(AddItem(Item("keyboard", 75.0)))
        cart.tell(AddItem(Item("mouse", 25.0)))
        await asyncio.sleep(0.1)

        total = await system.ask(cart, lambda r: GetTotal(reply_to=r), timeout=5.0)
        print(f"Total: ${total:.2f}")  # Total: $100.00

        receipt = await system.ask(cart, lambda r: Checkout(reply_to=r), timeout=5.0)
        print(f"Receipt: {len(receipt.items)} items, ${receipt.total:.2f}")
        # Receipt: 2 items, $100.00

        # Further modifications are ignored — checked_out is a terminal state
        cart.tell(AddItem(Item("monitor", 300.0)))

asyncio.run(main())
```

Three functions, three states: `empty_cart`, `active_cart`, `checked_out`. The transitions are explicit in the return values — `empty_cart` transitions to `active_cart` on the first `AddItem`, `active_cart` transitions to `checked_out` on `Checkout`. A `checked_out` actor ignores further modifications. The type checker ensures every state handles the full `CartMsg` union.

## Event Sourcing

Traditional persistence stores the **current state** — a row in a database with `balance = 500`. Event sourcing stores the **sequence of facts that produced the state** — `Deposited(100)`, `Deposited(500)`, `Withdrawn(100)`. The current state is a derived value, computed by folding events from left to right.

This distinction has profound consequences:

- **Complete audit trail.** Every state change is recorded as an immutable event. You can reconstruct the state at any point in time by replaying events up to that moment.
- **Recovery after failure.** When an actor restarts (via supervision) or a process crashes and restarts, the actor replays its events from the journal and recovers its exact state. Supervision provides the restart; event sourcing provides the memory.
- **Separation of concerns.** The *decision* to change state (command handling) is separated from the *application* of the change (event handling). Commands can be rejected; events are facts that have already occurred and cannot be rejected.

Event sourcing introduces three distinct concepts:

- **Commands** — Messages from the outside world requesting a state change. "Deposit 100" is a command. It may be accepted or rejected (e.g., "insufficient funds" for a withdrawal).
- **Events** — Immutable records of what actually happened. "Deposited 100" is an event. Events are persisted to a journal and never modified.
- **State** — The current value derived from the event sequence. Computed by folding `on_event` over all persisted events from an initial state.

A regular Casty actor holds state in a closure — but closures cannot be replayed. Event sourcing requires a different behavior shape with two explicit functions:

- `on_event(state, event) -> state` — A pure, synchronous function that applies one event to the current state. Used both for live processing and for recovery (replay).
- `on_command(ctx, state, command) -> Behavior` — An async function that receives the current state and a command, decides what events to persist, and returns `Behaviors.persisted(events=[...])`.

```python
import asyncio
from dataclasses import dataclass
from typing import Any
from casty import ActorRef, ActorSystem, Behavior, Behaviors
from casty.actor import SnapshotEvery
from casty.journal import InMemoryJournal

# --- State ---

@dataclass(frozen=True)
class AccountState:
    balance: int
    tx_count: int

# --- Events (persisted to the journal) ---

@dataclass(frozen=True)
class Deposited:
    amount: int

@dataclass(frozen=True)
class Withdrawn:
    amount: int

type AccountEvent = Deposited | Withdrawn

# --- Commands (sent by the outside world) ---

@dataclass(frozen=True)
class Deposit:
    amount: int

@dataclass(frozen=True)
class Withdraw:
    amount: int
    reply_to: ActorRef[str]

@dataclass(frozen=True)
class GetBalance:
    reply_to: ActorRef[AccountState]

type AccountCommand = Deposit | Withdraw | GetBalance

# --- Event handler (pure — used for replay and live updates) ---

def on_event(state: AccountState, event: AccountEvent) -> AccountState:
    match event:
        case Deposited(amount):
            return AccountState(balance=state.balance + amount, tx_count=state.tx_count + 1)
        case Withdrawn(amount):
            return AccountState(balance=state.balance - amount, tx_count=state.tx_count + 1)
    return state

# --- Command handler (async — decides what events to persist) ---

async def on_command(ctx: Any, state: AccountState, cmd: AccountCommand) -> Any:
    match cmd:
        case Deposit(amount):
            return Behaviors.persisted(events=[Deposited(amount)])
        case Withdraw(amount, reply_to) if state.balance >= amount:
            reply_to.tell("ok")
            return Behaviors.persisted(events=[Withdrawn(amount)])
        case Withdraw(_, reply_to):
            reply_to.tell(f"insufficient funds (balance={state.balance})")
            return Behaviors.same()
        case GetBalance(reply_to):
            reply_to.tell(state)
            return Behaviors.same()
    return Behaviors.unhandled()

# --- Entity factory ---

journal = InMemoryJournal()

def bank_account(entity_id: str) -> Behavior[AccountCommand]:
    return Behaviors.event_sourced(
        entity_id=entity_id,
        journal=journal,
        initial_state=AccountState(balance=0, tx_count=0),
        on_event=on_event,
        on_command=on_command,
        snapshot_policy=SnapshotEvery(n_events=100),
    )

# --- Recovery demonstration ---

async def main() -> None:
    # Phase 1: Normal operations
    async with ActorSystem(name="bank") as system:
        account = system.spawn(bank_account("acc-001"), "account")
        await asyncio.sleep(0.1)

        account.tell(Deposit(1000))
        account.tell(Deposit(500))
        await asyncio.sleep(0.1)

        state = await system.ask(
            account, lambda r: GetBalance(reply_to=r), timeout=2.0
        )
        print(f"Balance: {state.balance}, transactions: {state.tx_count}")
        # Balance: 1500, transactions: 2

    # Phase 2: Recovery — new actor, same journal
    async with ActorSystem(name="bank") as system:
        account = system.spawn(bank_account("acc-001"), "account")
        await asyncio.sleep(0.1)

        state = await system.ask(
            account, lambda r: GetBalance(reply_to=r), timeout=2.0
        )
        print(f"Recovered balance: {state.balance}, transactions: {state.tx_count}")
        # Recovered balance: 1500, transactions: 2

asyncio.run(main())
```

In Phase 2, a new actor is spawned with the same `entity_id`. The framework automatically loads the latest snapshot (if any), replays events from the journal since that snapshot, and reconstructs the state. The actor resumes exactly where it left off.

The `SnapshotEvery(n_events=100)` policy periodically saves the current state to the journal. Without snapshots, recovery requires replaying every event from the beginning — acceptable for entities with few events, but expensive for long-lived entities with thousands.

The `EventJournal` protocol is storage-agnostic. `InMemoryJournal` is included for testing and development. For production, implement the protocol for any database backend — the interface requires four methods: `persist`, `load`, `save_snapshot`, and `load_snapshot`.

## Cluster Sharding

A single process has limits — memory, CPU, network connections. When a system manages millions of entities (bank accounts, user sessions, IoT sensors), no single machine can host them all. **Cluster sharding** solves this by partitioning entities across multiple nodes and routing messages transparently.

Consider a network of 100,000 temperature sensors reporting readings every second. Each sensor is modeled as an actor that aggregates its readings. No single machine can host all 100,000 actors — but if they are partitioned into 256 shards distributed across 8 nodes, each node manages roughly 12,500 sensors. When a sensor reports a reading, the cluster routes the message to whichever node owns that sensor's shard. If nodes are added or removed, shards are rebalanced automatically.

Cluster sharding has three components:

- **Shards.** A logical partition of the entity space. Entity IDs are hashed deterministically to a shard number (Casty uses MD5). The number of shards is fixed at creation time and should be significantly larger than the expected number of nodes — this ensures rebalancing is granular (individual shards move between nodes, not entire ranges).
- **Coordinator.** Decides which node owns which shard. Uses a least-shard-first allocation strategy: when a shard is accessed for the first time, it is assigned to the node currently hosting the fewest shards. The coordinator is replicated across the cluster with a leader/follower topology — the leader makes allocation decisions, followers cache allocations and forward unknown shards to the leader.
- **Region.** Each node runs a region that manages local entities. When a message arrives for a shard owned by this node, the region spawns the entity actor (if it doesn't exist yet) and delivers the message. Messages for shards owned by other nodes are forwarded over the network.

Underneath the sharding layer, Casty implements a **gossip protocol** for cluster state propagation, a **phi accrual failure detector** (Hayashibara et al.) for identifying unresponsive nodes, and **vector clocks** for resolving conflicting state during network partitions.

```python
import asyncio
from dataclasses import dataclass
from casty import ActorContext, ActorRef, Behavior, Behaviors, ShardEnvelope
from casty.sharding import ClusteredActorSystem

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
            seed_nodes=[("127.0.0.1", 25521)],
        ) as node1,
        ClusteredActorSystem(
            name="bank", host="127.0.0.1", port=25521,
            seed_nodes=[("127.0.0.1", 25520)],
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

Every node in the cluster runs the same code — only `host` and `port` differ. `ShardEnvelope(entity_id, message)` wraps a message with the entity ID for routing. The proxy actor on each node caches shard-to-node mappings and forwards messages to the correct region, whether local or remote.

`ClusteredActorSystem` extends `ActorSystem`. Spawning a `ShardedBehavior` (produced by `Behaviors.sharded()`) returns an `ActorRef[ShardEnvelope[M]]`. All other spawns work identically to the local `ActorSystem`. This means existing actors that don't need distribution require no changes when moving to a clustered deployment.

## Shard Replication

Sharding solves the capacity problem but introduces a new failure mode: if a node goes down, all entities hosted on that node become unavailable. **Replication** addresses this by maintaining passive copies of each entity on other nodes.

The primary processes commands and pushes persisted events to its replicas after each command. Replicas maintain their own copy of the event journal and can be promoted to primary if the original node fails. This requires the entity to use event sourcing — replication operates on the event stream, not on raw state.

The `min_acks` parameter controls the consistency/latency trade-off:

| `min_acks` | Behavior |
|------------|----------|
| `0` | Fire-and-forget replication. Lowest latency, but recent events may be lost if the primary fails before replication completes. |
| `1+` | The primary waits for N replica acknowledgments before confirming the command. Stronger durability at the cost of increased latency. |

```python
from casty import Behaviors
from casty.replication import ReplicationConfig

accounts = node.spawn(
    Behaviors.sharded(
        account_entity,
        num_shards=100,
        replication=ReplicationConfig(
            replicas=2,        # 2 passive replicas per entity
            min_acks=1,        # wait for 1 replica ack before confirming
            ack_timeout=5.0,   # seconds to wait for acks
        ),
    ),
    "accounts",
)
```

The coordinator allocates primary and replicas on different nodes. When a node fails, the failure detector triggers a `NodeDown` event, and the coordinator promotes the replica with the highest event sequence number to primary. With three nodes and `replicas=2`, every entity has a primary on one node and replicas on the other two — the cluster tolerates the loss of any single node without data loss.

## Distributed Data Structures

For common patterns that don't require custom entity actors, Casty provides higher-level data structures built on top of cluster sharding. Each structure is backed by sharded actors, optionally persistent via event sourcing.

Every concept from the previous sections — actors, functional state, event sourcing, cluster sharding — converges here. A `Counter`, for example, is a sharded actor with a predefined message protocol, distributed transparently across the cluster.

```python
import asyncio
from dataclasses import dataclass
from casty.sharding import ClusteredActorSystem
from casty.journal import InMemoryJournal

@dataclass(frozen=True)
class User:
    name: str
    email: str

async def main() -> None:
    async with ClusteredActorSystem(
        name="my-app", host="127.0.0.1", port=25520,
        seed_nodes=[("127.0.0.1", 25521)],
    ) as system:
        d = system.distributed()

        # Counter
        views = d.counter("page-views", shards=50)
        await views.increment(100)
        await views.decrement(10)
        value = await views.get()        # 90

        # Dict — one sharded entity per key
        users = d.map[str, User]("users", shards=50)
        await users.put("alice", User("Alice", "alice@example.com"))
        user = await users.get("alice")  # User(name='Alice', email='alice@example.com')
        await users.contains("alice")    # True
        await users.delete("alice")      # True (existed)

        # Set
        tags = d.set[str]("active-tags", shards=50)
        await tags.add("python")         # True (added)
        await tags.add("python")         # False (already present)
        await tags.size()                # 1

        # Queue (FIFO)
        jobs = d.queue[str]("work-queue", shards=50)
        await jobs.enqueue("task-1")
        await jobs.peek()                # "task-1" (does not remove)
        await jobs.dequeue()             # "task-1" (removes)
        await jobs.dequeue()             # None (empty)

asyncio.run(main())
```

All structures accept `shards` (default `100`) for distribution granularity and `timeout` (default `5.0s`) for operation timeouts. The same structure can be accessed from any node in the cluster — use the same `name` and `shards`.

To make structures persistent, pass an `EventJournal` to the `distributed()` facade. Every mutation is persisted as an event and replayed on recovery:

```python
d = system.distributed(journal=InMemoryJournal())
counter = d.counter("hits")
await counter.increment(10)
# Process restarts → replays Incremented(10) → recovers value=10
```

These structures are intentionally simple. For domain-specific entities — bank accounts, user sessions, IoT sensor aggregators — define custom behaviors using `Behaviors.sharded()` and `Behaviors.event_sourced()`. The distributed data structures exist for the common cases that don't warrant a custom actor.

## Casty as a Cluster Backend

Most Python libraries that need distributed coordination depend on external services. Celery requires Redis or RabbitMQ. Distributed lock managers need etcd or ZooKeeper. Service meshes depend on Consul. Each external dependency adds operational complexity — deployment, monitoring, version compatibility, network configuration.

Casty provides the same cluster capabilities as an embeddable Python library with zero external dependencies. No broker to deploy, no configuration server to manage, no serialization protocol to learn. A library author can add `casty` to their dependencies and get cluster membership, leader election, failure detection, work distribution, and state replication out of the box. An application developer can add clustering to an existing system without introducing new infrastructure.

The following sections illustrate these capabilities through the lens of building a distributed task queue — a scenario where Celery would typically require Redis and a separate worker process.

### Cluster Formation

`ClusteredActorSystem` establishes a cluster through seed nodes. Once started, nodes discover each other automatically via the gossip protocol. The event stream publishes membership events as nodes join and leave:

```python
from casty import MemberUp, MemberLeft
from casty.sharding import ClusteredActorSystem

async with ClusteredActorSystem(
    name="task-queue",
    host="10.0.0.1",
    port=25520,
    seed_nodes=[("10.0.0.2", 25520), ("10.0.0.3", 25520)],
) as system:
    system.event_stream.subscribe(
        MemberUp, lambda e: log.info(f"Worker joined: {e.member.address}")
    )
    system.event_stream.subscribe(
        MemberLeft, lambda e: log.info(f"Worker left: {e.member.address}")
    )
```

Every node in the cluster runs the same code. There is no distinction between "broker" and "worker" — every node is both. The cluster forms automatically as nodes start and discover each other through the seed list.

### Work Distribution

`Behaviors.sharded()` partitions work across nodes by entity ID. For a task queue, each task type or queue name can be an entity, and the cluster handles routing transparently:

```python
from casty import Behaviors, ShardEnvelope

def task_worker(entity_id: str) -> Behavior[TaskMsg]:
    def idle() -> Behavior[TaskMsg]:
        async def receive(ctx, msg):
            match msg:
                case SubmitTask(payload, reply_to):
                    result = await execute(payload)
                    reply_to.tell(TaskResult(result))
                    return idle()
        return Behaviors.receive(receive)
    return idle()

tasks = system.spawn(Behaviors.sharded(task_worker, num_shards=256), "tasks")

# Submit from any node — routing is transparent
tasks.tell(ShardEnvelope("queue:emails", SubmitTask(send_welcome, reply_to)))
tasks.tell(ShardEnvelope("queue:reports", SubmitTask(generate_pdf, reply_to)))
```

The entity ID determines which node processes the task. Tasks with the same queue name always land on the same node, providing natural ordering guarantees. Tasks with different queue names are distributed across the cluster.

### Failure Handling

When a node becomes unreachable, the phi accrual failure detector identifies it and the coordinator reallocates its shards to surviving nodes. No manual intervention, no external health check service:

```python
from casty import UnreachableMember

system.event_stream.subscribe(
    UnreachableMember,
    lambda e: log.warning(f"Node unreachable: {e.member.address}, shards will be reallocated"),
)
```

If the task worker uses event sourcing, reallocated entities replay their journal on the new node and resume exactly where they left off. Tasks in progress at the time of failure are recovered automatically — the new primary replays persisted events and the worker continues from its last known state.

### Distributed Barriers

`system.barrier(name, n)` blocks until `n` nodes reach the same named barrier, then releases all simultaneously:

```python
await system.barrier("work-complete", num_nodes)
# all nodes have reached this point before any proceeds
await system.barrier("shutdown", num_nodes)
```

Backed by sharded entities — no external coordination service. Each named barrier can be reused across multiple rounds.

### State Without External Storage

Event sourcing combined with replication provides durable distributed state without a database. The primary persists events to its journal and pushes them to replicas on other nodes. If the primary fails, a replica is promoted with its full event history intact:

```python
from casty import Behaviors
from casty.journal import InMemoryJournal
from casty.replication import ReplicationConfig

journal = InMemoryJournal()  # replace with a database-backed journal in production

tasks = system.spawn(
    Behaviors.sharded(
        lambda eid: Behaviors.event_sourced(
            entity_id=eid,
            journal=journal,
            initial_state=QueueState(pending=(), completed=()),
            on_event=apply_event,
            on_command=handle_command,
        ),
        num_shards=256,
        replication=ReplicationConfig(replicas=1, min_acks=1),
    ),
    "tasks",
)
```

No Redis. No RabbitMQ. No ZooKeeper. The cluster stack is the library — membership, leader election, failure detection, sharding, replication, and distributed data structures, all in pure Python, all in a single `pip install`.

## Configuration

Every parameter shown in previous sections — mailbox capacity, supervision strategy, gossip interval, failure detector threshold — can be set programmatically. But when deploying the same codebase across environments (dev, staging, production), hardcoding these values becomes impractical. Casty supports TOML-based configuration via a `casty.toml` file, inspired by Akka's `application.conf`.

Configuration is always optional. When absent, all parameters use the same defaults as the programmatic API. When present, the TOML file produces the existing dataclasses — `ClusterConfig`, `ReplicationConfig`, etc. — through a pure loader function. No global state, no singletons, no magic.

### File Structure

A complete `casty.toml` covering all configurable aspects:

```toml
[system]
name = "my-app"

# --- Global defaults (apply to all actors) ---
[defaults.mailbox]
capacity = 1000
strategy = "drop_new"         # drop_new | drop_oldest | backpressure

[defaults.supervision]
strategy = "restart"          # restart | stop | escalate
max_restarts = 3
within_seconds = 60.0

[defaults.sharding]
num_shards = 256

[defaults.replication]
replicas = 2
min_acks = 1
ack_timeout = 5.0

# --- Cluster ---
[cluster]
host = "0.0.0.0"
port = 25520
seed_nodes = ["node1:25520", "node2:25520"]
roles = ["worker"]

[cluster.gossip]
interval = 1.0

[cluster.heartbeat]
interval = 0.5
availability_check_interval = 2.0

[cluster.failure_detector]
threshold = 8.0
max_sample_size = 200
min_std_deviation_ms = 100.0
acceptable_heartbeat_pause_ms = 0.0
first_heartbeat_estimate_ms = 1000.0

# --- Per-actor overrides ---
[actors.orders]
sharding = { num_shards = 512 }
replication = { replicas = 3, min_acks = 2 }

[actors.my-worker]
mailbox = { capacity = 5000, strategy = "backpressure" }
supervision = { strategy = "stop" }

[actors."child-\\d+"]
mailbox = { capacity = 100, strategy = "drop_oldest" }
```

Every key is optional. A minimal `casty.toml` can be just:

```toml
[system]
name = "my-app"
```

### Loading Configuration

`load_config()` accepts an explicit path or discovers `casty.toml` automatically by walking up from the current working directory (like `pyproject.toml`):

```python
from casty import load_config

# Auto-discovery — walks up from CWD looking for casty.toml
config = load_config()

# Explicit path
config = load_config(Path("infra/casty.toml"))
```

For a local `ActorSystem`, pass the config to the constructor:

```python
from casty import ActorSystem, load_config

config = load_config()

async with ActorSystem(config=config) as system:
    ref = system.spawn(my_behavior(), "my-actor")
    # mailbox and supervision come from casty.toml
```

For a `ClusteredActorSystem`, use `from_config` to derive host, port, seed nodes, and cluster tuning from the `[cluster]` section:

```python
from casty import load_config
from casty.sharding import ClusteredActorSystem

config = load_config()

async with ClusteredActorSystem.from_config(config) as system:
    # host, port, seed_nodes, roles, gossip interval,
    # heartbeat interval, failure detector — all from casty.toml
    ...
```

Programmatic overrides take precedence over the file — useful for dynamic ports in tests or container orchestration:

```python
async with ClusteredActorSystem.from_config(
    config,
    host="10.0.0.5",
    port=0,  # OS-assigned port
) as system:
    ...
```

### Per-Actor Overrides

Actor keys under `[actors.*]` are matched against the actor name at `spawn()` time using `re.fullmatch()`. Simple names like `orders` match exactly. Regex patterns use quoted TOML keys:

```toml
# Exact match — only the actor named "orders"
[actors.orders]
mailbox = { capacity = 5000 }

# Regex — matches "sensor-001", "sensor-042", etc.
[actors."sensor-\\d+"]
mailbox = { capacity = 100, strategy = "drop_oldest" }
```

Resolution order: **per-actor override > `[defaults.*]` > dataclass defaults**. First matching pattern wins (definition order in TOML).

```toml
[defaults.mailbox]
capacity = 1000
strategy = "drop_new"

[actors.orders]
mailbox = { capacity = 5000 }
# strategy inherits "drop_new" from [defaults.mailbox]
```

Internal actors (names starting with `_`) are never resolved against config — they always use framework defaults.

## Contributing

```bash
git clone https://github.com/gabfssilva/casty
cd casty
uv sync
uv run pytest                    # run tests
uv run pyright src/casty/        # type checking (strict mode)
uv run ruff check src/ tests/    # lint
uv run ruff format src/ tests/   # format
```

## License

MIT — see [LICENSE](LICENSE) for details.
