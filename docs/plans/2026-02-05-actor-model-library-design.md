# Casty — Actor Model Library Design

Python actor model library inspired by Akka Typed. Functional/behavior-based API, typed messages, hierarchical supervision, asyncio runtime.

## Core Concept: Behaviors

A `Behavior[M]` describes how an actor processes messages of type `M`. Each message processed returns the next behavior — state transitions without mutability.

```python
from dataclasses import dataclass
from casty import ActorContext, ActorRef, Behavior, Behaviors

@dataclass(frozen=True)
class Greet:
    name: str

@dataclass(frozen=True)
class GetCount:
    reply_to: ActorRef[int]

type GreeterMsg = Greet | GetCount

def greeter(count: int = 0) -> Behavior[GreeterMsg]:
    async def receive(ctx: ActorContext[GreeterMsg], msg: GreeterMsg) -> Behavior[GreeterMsg]:
        match msg:
            case Greet(name):
                print(f"Hello {name}!")
                return greeter(count + 1)
            case GetCount(reply_to):
                reply_to.tell(count)
                return Behaviors.same()

    return Behaviors.receive(receive)
```

**Built-in behaviors:**

- `Behaviors.receive(fn)` — active behavior that processes messages
- `Behaviors.setup(fn)` — executed once on actor creation, receives `ActorContext`, returns initial behavior
- `Behaviors.same()` — keep current behavior
- `Behaviors.stopped()` — signal actor should stop
- `Behaviors.unhandled()` — signal message was not handled (goes to dead letters)
- `Behaviors.restart()` — request self-restart

State is always immutable — lives in closures, transitions via new function calls.

## Lifecycle Hooks

Lifecycle hooks are behavior wrappers — composed around the main behavior, not method overrides.

```python
def greeter(count: int = 0) -> Behavior[GreeterMsg]:
    async def receive(ctx: ActorContext[GreeterMsg], msg: GreeterMsg) -> Behavior[GreeterMsg]:
        match msg:
            case Greet(name):
                return greeter(count + 1)
            case GetCount(reply_to):
                reply_to.tell(count)
                return Behaviors.same()

    return Behaviors.setup(lambda ctx: (
        Behaviors.with_lifecycle(
            Behaviors.receive(receive),
            pre_start=lambda ctx: print("Starting greeter"),
            post_stop=lambda ctx: print("Greeter stopped"),
            pre_restart=lambda ctx, exc: print(f"Restarting due to {exc}"),
            post_restart=lambda ctx: print("Restarted"),
        )
    ))
```

- `Behaviors.with_lifecycle(behavior, ...)` — wraps a behavior with optional hooks
- All hooks are optional
- Composition allows stacking (logging wrapper, metrics wrapper, etc.)

## ActorContext

The `ActorContext[M]` is the actor's interface to the system. Received in `setup` and `receive`.

```python
def supervisor() -> Behavior[SupervisorMsg]:
    async def setup(ctx: ActorContext[SupervisorMsg]) -> Behavior[SupervisorMsg]:
        worker_ref: ActorRef[WorkerMsg] = ctx.spawn(worker(), "worker-1")
        pool = [ctx.spawn(worker(), f"worker-{i}") for i in range(5)]

        async def receive(ctx: ActorContext[SupervisorMsg], msg: SupervisorMsg) -> Behavior[SupervisorMsg]:
            match msg:
                case Dispatch(work):
                    pool[0].tell(work)
                    return Behaviors.same()

        return Behaviors.receive(receive)

    return Behaviors.setup(setup)
```

**API:**

| Method | Description |
|---|---|
| `ctx.spawn(behavior, name)` | Create child actor, returns typed `ActorRef` |
| `ctx.self` | `ActorRef[M]` of the actor itself |
| `ctx.stop(child_ref)` | Stop a child actor |
| `ctx.watch(ref)` | Watch another actor's death (receives `Terminated`) |
| `ctx.unwatch(ref)` | Stop watching |
| `ctx.log` | Logger with actor context (path, name) |

Hierarchy forms naturally via `ctx.spawn`. Parent death stops all children. `watch` allows observing any actor's death.

## ActorRef

`ActorRef[M]` is an opaque, typed handle. Only exposes `tell(msg: M)`. No access to actor internals, no path, no status — just a typed address.

```python
ref: ActorRef[GreeterMsg] = ctx.spawn(greeter(), "greeter")
ref.tell(Greet("Gabriel"))
```

Pyright enforces message type at analysis time.

## Communication: tell vs ask

**`tell`** — fire-and-forget, used between actors:

```python
ref.tell(Greet("Gabriel"))
```

**`ask`** — bridge between external world and actor system. Lives on `ActorSystem`, NOT on `ActorRef`:

```python
async with ActorSystem() as system:
    ref = system.spawn(greeter(), "greeter")
    count: int = await system.ask(ref, GetCount, timeout=5.0)
```

Internally, `ask` creates an ephemeral temporary actor that:
1. Sends the message to the target (injecting its own ref as `reply_to`)
2. Awaits the response with timeout
3. Self-destructs

`ask` between actors is intentionally not supported — it causes deadlocks. If an actor needs a response from another, the pattern is to send `reply_to: ActorRef[Response]` via `tell`.

## Supervision

Supervision is the parent's responsibility. When a child fails (exception in `receive`), the parent's strategy decides the outcome.

```python
from casty import SupervisionStrategy, OneForOneStrategy, Directive

class Directive(Enum):
    restart = auto()   # restart the actor
    stop = auto()      # stop the actor permanently
    escalate = auto()  # propagate failure to supervisor above

strategy = OneForOneStrategy(
    max_restarts=3,
    within=60.0,
    decider=lambda exc: (
        Directive.restart if isinstance(exc, ConnectionError)
        else Directive.stop
    ),
)

def supervisor() -> Behavior[SupervisorMsg]:
    async def setup(ctx: ActorContext[SupervisorMsg]) -> Behavior[SupervisorMsg]:
        ctx.spawn(worker(), "worker-1")
        return Behaviors.receive(handle)

    return Behaviors.supervise(Behaviors.setup(setup), strategy)
```

- `Behaviors.supervise(behavior, strategy)` — wraps a behavior with supervision for its children
- `SupervisionStrategy` is an open interface — implement `decide(exception) -> Directive` for custom strategies
- Default: `OneForOneStrategy` (restart only the failed child)
- `AllForOne`, `RestForOne` are future extensions

## ActorSystem

Entry point. Manages root guardian, event stream, external bridge. Also an async context manager.

```python
async with ActorSystem(name="my-system") as system:
    ref = system.spawn(greeter(), "greeter")
    count = await system.ask(ref, GetCount, timeout=5.0)

    found: ActorRef | None = system.lookup("/greeter")

    system.event_stream.subscribe(DeadLetter, dead_letter_handler)
    system.event_stream.subscribe(ActorStarted, started_handler)
```

**Event stream** — typed pub/sub for system events:

| Event | When |
|---|---|
| `ActorStarted(ref)` | Actor started |
| `ActorStopped(ref)` | Actor stopped |
| `ActorRestarted(ref, exception)` | Actor restarted |
| `DeadLetter(msg, intended_ref)` | Message sent to dead/nonexistent actor |
| `UnhandledMessage(msg, ref)` | Actor returned `Behaviors.unhandled()` |

## Mailbox

Each actor has a mailbox. Default is unbounded, configurable per actor.

```python
from casty import Mailbox, MailboxOverflowStrategy

# Default — unbounded
ref = ctx.spawn(worker(), "worker-1")

# Bounded with overflow strategy
ref = ctx.spawn(
    worker(),
    "worker-1",
    mailbox=Mailbox(
        capacity=1000,
        overflow=MailboxOverflowStrategy.drop_oldest,
    ),
)
```

**Overflow strategies:**

| Strategy | Behavior |
|---|---|
| `drop_oldest` | Discard oldest message in queue |
| `drop_new` | Discard incoming message |
| `backpressure` | `tell` becomes awaitable, suspends until space available |

Processing is serial — one message at a time per actor, no internal concurrency.

Internally wraps `asyncio.Queue` (unbounded) or `asyncio.Queue(maxsize=N)` (bounded).

## Project Structure

```
casty/
├── pyproject.toml
├── src/
│   └── casty/
│       ├── __init__.py          # re-exports
│       ├── actor.py             # Behavior, Behaviors
│       ├── context.py           # ActorContext
│       ├── ref.py               # ActorRef[M]
│       ├── system.py            # ActorSystem
│       ├── mailbox.py           # Mailbox, MailboxOverflowStrategy
│       ├── supervision.py       # SupervisionStrategy, OneForOneStrategy, Directive
│       ├── events.py            # EventStream, DeadLetter, ActorStarted, etc.
│       └── messages.py          # Terminated, system messages
└── tests/
    ├── test_behavior.py
    ├── test_context.py
    ├── test_ref.py
    ├── test_system.py
    ├── test_mailbox.py
    ├── test_supervision.py
    └── test_events.py
```

All public API exported flat from `casty` top-level.

## Implementation Notes

- Target Python 3.13+
- asyncio as the runtime
- pyright strict for type checking
- Frozen dataclasses for all messages
- No external dependencies for core
- Clustering is a future concern — all design decisions should not preclude it but should not optimize for it either
