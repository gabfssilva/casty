<p align="center">
  <img src="logo.png" alt="Casty" width="200">
</p>

<p align="center">
  <strong>Minimalist, type-safe actor framework for Python 3.12+</strong>
</p>

<p align="center">
  <a href="https://pypi.org/project/casty/"><img src="https://img.shields.io/pypi/v/casty.svg" alt="PyPI"></a>
  <a href="https://pypi.org/project/casty/"><img src="https://img.shields.io/pypi/pyversions/casty.svg" alt="Python"></a>
  <a href="https://github.com/gabfssilva/casty/actions"><img src="https://img.shields.io/github/actions/workflow/status/gabfssilva/casty/test.yml" alt="Tests"></a>
  <a href="https://github.com/gabfssilva/casty/blob/main/LICENSE"><img src="https://img.shields.io/github/license/gabfssilva/casty.svg" alt="License"></a>
</p>

<p align="center">
  <code>pip install casty</code>
</p>

## What is Casty?

Casty is an actor framework that makes concurrent programming simple. Instead of dealing with threads, locks, and shared state, you write small independent units called **actors** that communicate through **messages**.

**Why actors?**

- **No shared state** — Each actor owns its data. No locks, no race conditions.
- **Message-driven** — Actors communicate asynchronously through messages.
- **Fault-tolerant** — Actors can supervise each other and recover from failures.
- **Scalable** — Same code runs on one machine or across a cluster.

**Why Casty?**

- **Pythonic** — Actors are async functions, not classes. Uses `async for` and pattern matching.
- **Simple** — One decorator, one concept. No boilerplate.
- **Batteries included** — Clustering with SWIM protocol works out of the box.

## Quick Start

```python
from dataclasses import dataclass
from casty import actor, ActorSystem, Mailbox

@dataclass
class Increment:
    amount: int

@dataclass
class Get:
    pass

@actor
async def counter(*, mailbox: Mailbox[Increment | Get]):
    count = 0
    async for msg, ctx in mailbox:
        match msg:
            case Increment(amount):
                count += amount
            case Get():
                await ctx.reply(count)

async def main():
    async with ActorSystem() as system:
        ref = await system.actor(counter(), name="counter")
        await ref.send(Increment(5))
        await ref.send(Increment(3))
        result = await ref.ask(Get())  # 8
```

**What just happened?**

1. **Messages are dataclasses** — `Increment` carries data, `Get` is a query
2. **Actors hold state** — `count` lives inside the actor, safe from race conditions
3. **Pattern matching** — `match msg` dispatches to the right handler
4. **Two ways to communicate** — `send()` is fire-and-forget, `ask()` waits for a reply

## Core Concepts

### Messages

Messages are plain dataclasses. Group related messages with type aliases:

```python
@dataclass
class Deposit:
    amount: float

@dataclass
class Withdraw:
    amount: float

@dataclass
class GetBalance:
    pass

type BankMsg = Deposit | Withdraw | GetBalance
```

### Actors

Actors are async functions decorated with `@actor`. They receive a `Mailbox` and process messages in a loop:

```python
@actor
async def bank_account(*, mailbox: Mailbox[BankMsg]):
    balance = 0.0
    async for msg, ctx in mailbox:
        match msg:
            case Deposit(amount):
                balance += amount
            case Withdraw(amount):
                balance -= amount
            case GetBalance():
                await ctx.reply(balance)

# For explicit state management with persistence support:
from casty import State

@actor
async def counter(state: State[int], *, mailbox: Mailbox[Increment | Get]):
    async for msg, ctx in mailbox:
        match msg:
            case Increment(n):
                state.value += n
            case Get():
                await ctx.reply(state.value)

# Create with initial state
ref = await system.actor(counter(State(0)), name="counter")
```

### Actor References

When you create an actor, you get back a reference (`ActorRef`). Use it to send messages:

```python
ref = await system.actor(bank_account(), name="my-account")
await ref.send(Deposit(100))           # fire-and-forget
balance = await ref.ask(GetBalance())  # request-response
```

### Context

Every message comes with a `ctx` that lets you:

- `ctx.reply(value)` — respond to `ask()` calls
- `ctx.forward(msg, to=ref)` — forward message to another actor, preserving original sender
- `ctx.actor(...)` — spawn child actors
- `ctx.schedule(...)` — schedule delayed/periodic messages
- `ctx.sender` — reference to who sent the message

## Building Blocks

### Sending Messages

Two ways to communicate with actors:

```python
# Fire-and-forget — don't wait for response
await ref.send(Increment(1))

# Request-response — wait for reply
count = await ref.ask(Get())
```

Shorthand operators for cleaner code:

```python
await (ref >> Increment(1))   # same as send()
count = await (ref << Get())  # same as ask()
```

### Child Actors

Actors can spawn children. Children are supervised by their parent:

```python
@actor
async def parent(*, mailbox: Mailbox[StartWork]):
    async for msg, ctx in mailbox:
        match msg:
            case StartWork(task):
                child = await ctx.actor(worker(), name=f"worker-{task.id}")
                await child.send(task)
```

Children's lifecycle is tied to their parent — when a parent stops, children stop too.

### Scheduling

Send messages in the future or at regular intervals:

```python
# One-time delay (seconds)
await ctx.schedule(Reminder("Check status"), delay=30.0)

# Periodic — returns a cancel function
cancel = await ctx.schedule(Tick(), every=1.0)
# later...
await cancel()

# With explicit sender (replies go to sender instead of scheduler)
await ctx.schedule(Ping(), delay=1.0, sender=some_ref)
```

### Supervision

Actors crash. Supervisors decide what happens next:

```python
from casty import supervised, Restart, Stop

@supervised(strategy=Restart(max_retries=3))
@actor
async def resilient_worker(*, mailbox: Mailbox[Job]):
    async for msg, ctx in mailbox:
        # if this crashes, actor restarts automatically
        process(msg)
```

Strategies:

| Strategy | Behavior |
|----------|----------|
| `Restart(max_retries=n, within=60)` | Restart the actor up to n times within time window |
| `Stop()` | Stop the actor permanently |
| `Escalate()` | Escalate to parent supervisor |

Scopes:

| Scope | Behavior |
|-------|----------|
| `OneForOne()` | Only restart the failed actor (default) |
| `AllForOne()` | Restart all siblings when one fails |

## Going Distributed

Same actors, multiple machines. Casty uses production-grade distributed systems protocols:

- **SWIM** — Scalable failure detection with direct probes, indirect probes, and piggybacked membership gossip
- **Consistent Hashing** — Actors distributed across nodes with virtual nodes for even distribution
- **Quorum Replication** — Configurable write consistency (async, quorum, all)

### Local Development

Test clustering locally with `DevelopmentCluster`:

```python
from casty.cluster import DevelopmentCluster

async with DevelopmentCluster(nodes=3) as cluster:
    ref = await cluster.actor(counter(), name="counter")
    await (ref >> Increment(100))
```

### Production Setup

Connect real nodes across machines:

```python
from casty import ActorSystem

async with ActorSystem.clustered(
    node_id="node-1",
    host="0.0.0.0",
    port=8001,
    seeds=["node-2.example.com:8001"],
) as system:
    ref = await system.actor(counter(), name="counter")
```

### How It Works

```
┌──────────────┐       ┌──────────────┐       ┌──────────────┐
│    Node 1    │◄─────►│    Node 2    │◄─────►│    Node 3    │
│              │ SWIM  │              │ SWIM  │              │
│ ┌──────────┐ │       │ ┌──────────┐ │       │ ┌──────────┐ │
│ │ Actor A  │ │       │ │ Actor B  │ │       │ │ Actor C  │ │
│ └──────────┘ │       │ └──────────┘ │       │ └──────────┘ │
└──────────────┘       └──────────────┘       └──────────────┘
         ▲                                            │
         │            message to Actor A              │
         └────────────────────────────────────────────┘
               (routed automatically via hash ring)
```

Actors are distributed using consistent hashing. Messages are routed automatically — you don't need to know where an actor lives.

### Replicated Actors

For fault tolerance and high availability, replicate actors across nodes:

```python
@actor(clustered=True, replicas=3, write_quorum="quorum")
async def replicated_service(state: State[int], *, mailbox: Mailbox[Msg]):
    async for msg, ctx in mailbox:
        if ctx.is_leader:  # Only leader modifies state
            state.value += 1
        await ctx.reply(state.value)
```

Write quorum options:
- `"async"` — Fire-and-forget (no consistency guarantee)
- `"quorum"` — Wait for majority of replicas
- `"all"` — Wait for all replicas
- `int(n)` — Wait for exactly N acknowledgments

The framework automatically elects a leader using consistent hashing. Only the leader processes writes; replicas receive state updates.

## Examples

The [examples/](examples/) folder contains runnable examples:

| Example | Description |
|---------|-------------|
| [01-counter.py](examples/01-counter.py) | Basic counter with `State[int]`, pattern matching, `send()` vs `ask()` |
| [02-workers.py](examples/02-workers.py) | Parent-child delegation, work distribution, result aggregation |
| [03-scheduling.py](examples/03-scheduling.py) | Traffic light state machine with delayed self-scheduling |
| [04-supervision.py](examples/04-supervision.py) | Fault-tolerant worker with `@supervised`, crash recovery |
| [05-cluster.py](examples/05-cluster.py) | Distributed counter across multiple nodes with `@actor(clustered=True)` |

Run any example with:

```bash
uv run python examples/01-counter.py
```

## Contributing

```bash
git clone https://github.com/gabfssilva/casty
cd casty
uv sync
uv run pytest
```

Run tests in parallel for speed:

```bash
uv run pytest -n auto
```

See an issue? Open a PR. All contributions welcome.

## License

MIT — see [LICENSE](LICENSE) for details.
