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
