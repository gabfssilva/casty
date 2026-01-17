<p align="center">
  <img src="logo.png" alt="Casty" width="200">
</p>

<p align="center">
  <a href="https://pypi.org/project/casty/"><img src="https://img.shields.io/pypi/v/casty.svg" alt="PyPI"></a>
  <a href="https://pypi.org/project/casty/"><img src="https://img.shields.io/pypi/pyversions/casty.svg" alt="Python"></a>
  <a href="https://github.com/gabfssilva/casty/blob/main/LICENSE"><img src="https://img.shields.io/github/license/gabfssilva/casty.svg" alt="License"></a>
  <a href="https://github.com/gabfssilva/casty"><img src="https://img.shields.io/github/stars/gabfssilva/casty.svg" alt="Stars"></a>
  <a href="https://pypi.org/project/casty/"><img src="https://img.shields.io/pypi/dm/casty.svg" alt="Downloads"></a>
</p>

# Casty

**A minimalist, type-safe actor framework for Python 3.12+ with built-in distributed clustering.**

Casty brings the battle-tested Actor Model to Python with modern type hints, automatic state replication, and a clean API that scales from single-process applications to distributed clusters without changing your code.

---

## Table of Contents

1. [Why Actors?](#why-actors)
2. [Installation](#installation)
3. [Quick Start](#quick-start)
4. [Understanding Actors](#understanding-actors)
5. [Message Patterns](#message-patterns)
6. [Behavior Switching (State Machines)](#behavior-switching-state-machines)
7. [Combining Actor References](#combining-actor-references)
8. [Spawning and Hierarchies](#spawning-and-hierarchies)
9. [Supervision and Fault Tolerance](#supervision-and-fault-tolerance)
10. [Going Distributed](#going-distributed)
11. [Sharded Actors](#sharded-actors)
12. [Singleton Actors](#singleton-actors)
13. [State Replication](#state-replication)
14. [Configuration Reference](#configuration-reference)

---

## Why Actors?

Concurrent programming is hard. Shared mutable state, locks, race conditions, and deadlocks make it notoriously difficult to write correct concurrent code. The Actor Model, pioneered by Carl Hewitt in 1973 and popularized by Erlang and Akka, offers an elegant solution.

**The core insight is simple:** instead of sharing memory between threads, actors communicate exclusively through messages. Each actor:

- Has its own private state that no other actor can access directly
- Processes messages one at a time, sequentially
- Can create other actors, send messages, and decide how to handle the next message

This eliminates entire categories of bugs. No locks, no race conditions on shared state, no deadlocks from lock ordering. When you need to coordinate, you send a message and wait for a response.

Casty implements this model with Python's modern type system, giving you compile-time safety for your message types while keeping the API minimal and Pythonic.

---

## Installation

```bash
pip install casty
```

Or with uv:

```bash
uv add casty
```

Casty requires Python 3.12+ for its generic type syntax (`class Actor[M]`).

---

## Quick Start

Here's a complete example that demonstrates the core concepts:

```python
import asyncio
from dataclasses import dataclass
from casty import Actor, ActorSystem, Context

# Define your messages as dataclasses for type safety
@dataclass
class Greet:
    name: str

@dataclass
class GetGreetingCount:
    pass

# Define an actor by extending Actor[M] where M is your message type(s)
class Greeter(Actor[Greet | GetGreetingCount]):
    def __init__(self):
        self.count = 0

    async def receive(self, msg: Greet | GetGreetingCount, ctx: Context) -> None:
        match msg:
            case Greet(name):
                print(f"Hello, {name}!")
                self.count += 1
            case GetGreetingCount():
                await ctx.reply(self.count)

async def main():
    async with ActorSystem() as system:
        # Spawn an actor and get a typed reference
        greeter = await system.spawn(Greeter)

        # Send messages (fire-and-forget)
        await greeter.send(Greet("Alice"))
        await greeter.send(Greet("Bob"))

        # Request-response pattern
        count = await greeter.ask(GetGreetingCount())
        print(f"Total greetings: {count}")  # Output: Total greetings: 2

asyncio.run(main())
```

---

## Understanding Actors

### The Actor Lifecycle

When you spawn an actor, Casty creates an isolated execution context for it. The actor goes through a well-defined lifecycle:

```
spawn() → on_start() → [receive messages] → on_stop()
```

You can hook into this lifecycle by overriding methods:

```python
class DatabaseActor(Actor[Query]):
    async def on_start(self) -> None:
        """Called once when the actor starts."""
        self.connection = await connect_to_database()

    async def on_stop(self) -> None:
        """Called when the actor is stopping."""
        await self.connection.close()

    async def receive(self, msg: Query, ctx: Context) -> None:
        result = await self.connection.execute(msg.sql)
        await ctx.reply(result)
```

### Type-Safe References

When you spawn an actor, you get back an `ActorRef[M]` that's parameterized by the message type:

```python
greeter: ActorRef[Greet | GetGreetingCount] = await system.spawn(Greeter)

# The type system ensures you can only send valid messages
await greeter.send(Greet("Alice"))  # OK
await greeter.send("invalid")       # Type error!
```

This catches message type mismatches at development time rather than runtime.

### The Context Object

The `Context` object passed to `receive` provides access to the actor's environment:

```python
async def receive(self, msg: CreateChild, ctx: Context) -> None:
    # Spawn child actors
    child = await ctx.spawn(WorkerActor)

    # Reply to ask() calls
    await ctx.reply(some_result)

    # Access the actor system
    another_actor = await ctx.system.lookup("some-name")

    # Access self reference
    my_ref = ctx.self_ref
```

---

## Message Patterns

### Fire-and-Forget with `send()`

The simplest pattern is fire-and-forget. You send a message and continue immediately:

```python
await actor.send(DoWork(task_id=123))
# Continues immediately, doesn't wait for processing
```

Use this when you don't need confirmation or a result. It's efficient and non-blocking.

### Request-Response with `ask()`

When you need a response, use the ask pattern:

```python
result = await actor.ask(GetStatus(), timeout=5.0)
```

Under the hood, `ask()` creates a `Future`, sends the message with a reference to that future, and awaits the result. The actor responds by calling `await ctx.reply()`.

**Important:** Every `ask()` should have a corresponding `await ctx.reply()` in the actor, or the caller will timeout.

```python
class Calculator(Actor[Add | Multiply]):
    async def receive(self, msg: Add | Multiply, ctx: Context) -> None:
        match msg:
            case Add(a, b):
                await ctx.reply(a + b)  # Always reply to ask() calls
            case Multiply(a, b):
                await ctx.reply(a * b)
```

### Choosing Between Patterns

| Pattern | Use When | Characteristics |
|---------|----------|-----------------|
| `send()` | Fire-and-forget commands, events, notifications | Fast, non-blocking, no backpressure |
| `ask()` | Queries, operations requiring confirmation | Blocks caller, provides result/error |

---

## Behavior Switching (State Machines)

Actors can dynamically change how they handle messages using `become()` and `unbecome()`. This is useful for implementing state machines, protocol handlers, or any scenario where an actor's behavior depends on its current state.

### Using `become()` and `unbecome()`

```python
@dataclass
class Activate:
    pass

@dataclass
class Deactivate:
    pass

@dataclass
class Process:
    data: str

class StateMachine(Actor[Activate | Deactivate | Process]):
    def __init__(self):
        self.processed = []

    async def receive(self, msg, ctx: Context) -> None:
        # Default: inactive state - only respond to Activate
        match msg:
            case Activate():
                print("Activating...")
                ctx.become(self.active)  # Switch to active behavior
            case Process(data):
                print(f"Ignored (inactive): {data}")

    async def active(self, msg, ctx: Context) -> None:
        # Active state - process messages
        match msg:
            case Deactivate():
                print("Deactivating...")
                ctx.unbecome()  # Revert to receive()
            case Process(data):
                print(f"Processing: {data}")
                self.processed.append(data)

async def main():
    async with ActorSystem() as system:
        machine = await system.spawn(StateMachine)

        await machine.send(Process("ignored"))  # Ignored (inactive)
        await machine.send(Activate())          # Switch to active
        await machine.send(Process("hello"))    # Processing: hello
        await machine.send(Process("world"))    # Processing: world
        await machine.send(Deactivate())        # Switch back to inactive
        await machine.send(Process("ignored"))  # Ignored (inactive) again
```

### Behavior Stack

By default, `become()` pushes behaviors onto a stack:

```python
ctx.become(behavior1)  # Stack: [behavior1]
ctx.become(behavior2)  # Stack: [behavior1, behavior2]
ctx.unbecome()         # Stack: [behavior1]
ctx.unbecome()         # Stack: [] → back to receive()
```

Use `discard_old=True` to replace instead of stack:

```python
ctx.become(behavior1)                    # Stack: [behavior1]
ctx.become(behavior2, discard_old=True)  # Stack: [behavior2] (replaced)
```

---

## Combining Actor References

When you have multiple actors that handle different message types, you can combine their references using the `|` operator:

```python
@dataclass
class LogMessage:
    text: str

@dataclass
class MetricEvent:
    name: str
    value: float

class Logger(Actor[LogMessage]):
    async def receive(self, msg: LogMessage, ctx: Context) -> None:
        print(f"LOG: {msg.text}")

class MetricsCollector(Actor[MetricEvent]):
    async def receive(self, msg: MetricEvent, ctx: Context) -> None:
        print(f"METRIC: {msg.name}={msg.value}")

async def main():
    async with ActorSystem() as system:
        logger = await system.spawn(Logger)
        metrics = await system.spawn(MetricsCollector)

        # Combine refs - messages route automatically by type
        combined = logger | metrics

        await combined.send(LogMessage("Hello"))        # → Logger
        await combined.send(MetricEvent("cpu", 0.75))   # → MetricsCollector
```

This is useful when you want a single reference that can handle multiple message types, routing each to the appropriate actor.

---

## Spawning and Hierarchies

Actors can spawn other actors, creating natural hierarchies:

```python
class Supervisor(Actor[StartWorkers]):
    async def receive(self, msg: StartWorkers, ctx: Context) -> None:
        # Spawn child actors
        self.workers = [
            await ctx.spawn(Worker, name=f"worker-{i}")
            for i in range(msg.count)
        ]
```

Children spawned through `ctx.spawn()` are supervised by their parent. When a parent stops, all its children are stopped automatically.

### Named Actors and Lookup

You can give actors names for later lookup:

```python
# Spawn with a name
cache = await system.spawn(CacheActor, name="main-cache")

# Look up by name from anywhere
cache = await system.lookup("main-cache")
if cache:
    await cache.send(CacheSet("key", "value"))
```

---

## Supervision and Fault Tolerance

In real systems, failures happen. The Actor Model embraces this with the "let it crash" philosophy: instead of defensive programming with try/catch everywhere, let actors fail and have supervisors decide what to do.

### Supervision Strategies

When a child actor fails, its supervisor can:

- **RESTART**: Restart the failed actor (default)
- **STOP**: Stop the actor permanently
- **ESCALATE**: Propagate the failure upward

```python
from casty import supervised, SupervisionStrategy

@supervised(
    strategy=SupervisionStrategy.RESTART,
    max_restarts=3,
    within_seconds=60.0,
)
class UnreliableWorker(Actor[Task]):
    async def receive(self, msg: Task, ctx: Context) -> None:
        # If this raises, the supervisor will restart us
        await process(msg)
```

### Restart Behavior

When an actor is restarted:

1. `pre_restart(exc, msg)` is called on the old instance
2. A new instance is created
3. `post_restart(exc)` is called on the new instance
4. The actor resumes processing messages

```python
class StatefulWorker(Actor[Task]):
    def __init__(self):
        self.processed = 0

    async def pre_restart(self, exc: Exception, msg: Task | None) -> None:
        # Save state before restart, log the failure, etc.
        logger.error(f"Failed after {self.processed} tasks: {exc}")

    async def post_restart(self, exc: Exception) -> None:
        # Reinitialize after restart
        self.processed = 0
```

### Backoff

To prevent rapid restart loops, configure exponential backoff:

```python
@supervised(
    strategy=SupervisionStrategy.RESTART,
    max_restarts=5,
    within_seconds=60.0,
    backoff_initial=0.1,      # Start with 100ms delay
    backoff_max=30.0,         # Cap at 30 seconds
    backoff_multiplier=2.0,   # Double each time
)
class RetryingWorker(Actor[Task]):
    ...
```

### Multi-Child Strategies

When multiple children are supervised, you can control how failures affect siblings:

- **ONE_FOR_ONE** (default): Only restart the failed child
- **ONE_FOR_ALL**: Restart all children when one fails
- **REST_FOR_ONE**: Restart the failed child and all children spawned after it

```python
from casty import SupervisorConfig, SupervisionStrategy, MultiChildStrategy

config = SupervisorConfig(
    strategy=SupervisionStrategy.RESTART,
    multi_child=MultiChildStrategy.ONE_FOR_ALL,  # Restart all on failure
)

# Apply to a parent actor
@supervised(multi_child=MultiChildStrategy.ONE_FOR_ALL)
class Coordinator(Actor[Msg]):
    async def receive(self, msg, ctx: Context) -> None:
        # If any child fails, all children restart together
        self.workers = [await ctx.spawn(Worker) for _ in range(5)]
```

---

## Going Distributed

So far, everything runs in a single process. But what happens when you need:

- **Horizontal scaling** across multiple machines?
- **Fault tolerance** when machines fail?
- **Data locality** for performance?

Casty provides two distributed APIs depending on your needs:

```python
# Local system (single process)
async with ActorSystem() as system:
    ...

# Clustered system - automatic actor registration in cluster
async with ActorSystem.clustered(
    host="0.0.0.0",
    port=7946,
    seeds=["node2:7946", "node3:7946"],
) as system:
    # Named actors are automatically registered for remote discovery
    worker = await system.spawn(Worker, name="worker-1")

    # Get reference to remote actor
    remote = await system.lookup("worker-2")
    ...

# Distributed system - lower-level with explicit replication control
async with ActorSystem.clustered(
    host="0.0.0.0",
    port=8001,
    seeds=["node2:8001", "node3:8001"],
) as system:
    ...
```

### How It Works

Casty's distributed architecture uses proven distributed systems protocols:

**SWIM Protocol (Failure Detection)**
- Scalable Weakly-consistent Infection-style Membership protocol
- Periodic health probes between random node pairs
- Indirect probes through other nodes when direct probes fail
- Lifeguard suspicion mechanism for handling network delays
- O(1) failure detection overhead per node

**Gossip Protocol (State Dissemination)**
- Epidemic-style state propagation across the cluster
- Vector clocks for causal ordering of updates
- Eventually consistent actor registry for discovery
- Pattern-based subscriptions for state changes

**Consistent Hashing (Entity Routing)**
- Routes messages to the correct node deterministically
- Minimal redistribution when nodes join/leave (~1/N keys move)
- Virtual nodes for even load distribution
- Preference lists for replica placement

This architecture means all operations are fast and decentralized—no consensus bottleneck for message routing.

### Cluster Formation

Nodes discover each other through seed nodes:

```python
# Node 1 (first node, no seeds needed)
system1 = ActorSystem.clustered("0.0.0.0", 8001)

# Node 2 (joins via Node 1)
system2 = ActorSystem.clustered("0.0.0.0", 8002, seeds=["node1:8001"])

# Node 3 (can use any existing node as seed)
system3 = ActorSystem.clustered("0.0.0.0", 8003, seeds=["node1:8001"])
```

Once connected, nodes share membership information via gossip, so every node knows about every other node.

---

## Sharded Actors

Sharded actors (also called "virtual actors" or "grains") are Casty's solution for managing large numbers of stateful entities distributed across a cluster.

### The Problem

Imagine building a game server with millions of player sessions, or an IoT platform with millions of devices. You can't spawn millions of actors upfront—you need actors created on-demand and distributed across your cluster.

### The Solution

Sharded actors are addressed by a type and an ID. When you send a message to `players["user-123"]`, Casty:

1. Hashes "user-123" to determine which node owns it
2. Routes the message to that node
3. Creates the actor if it doesn't exist
4. Delivers the message

```python
@dataclass
class UpdateScore:
    delta: int

@dataclass
class GetScore:
    pass

class Player(Actor[UpdateScore | GetScore]):
    def __init__(self, entity_id: str):
        self.entity_id = entity_id
        self.score = 0

    async def receive(self, msg: UpdateScore | GetScore, ctx: Context) -> None:
        match msg:
            case UpdateScore(delta):
                self.score += delta
            case GetScore():
                await ctx.reply(self.score)

async def main():
    async with ActorSystem.clustered("0.0.0.0", 8001) as system:
        # Create a sharded actor type
        players = await system.spawn(Player, name="players", sharded=True)

        # Send to specific entities - actors created on-demand
        await players["alice"].send(UpdateScore(100))
        await players["bob"].send(UpdateScore(50))

        # Query specific entities
        alice_score = await players["alice"].ask(GetScore())
        print(f"Alice's score: {alice_score}")  # 100
```

### Consistent Hashing

Casty uses consistent hashing to map entity IDs to nodes. This means:

- The same entity ID always routes to the same node (until topology changes)
- Adding/removing nodes only moves ~1/N of the entities
- Replicas are placed on consecutive nodes in the hash ring

```
Hash Ring:
    Node A ──────────── Node B
       │                   │
       │     Entity X      │
       │    (hashes here)  │
       │         │         │
       └─────────┴─────────┘
              Node C
```

---

## Singleton Actors

Sometimes you need exactly one instance of an actor across the entire cluster—a cache coordinator, a job scheduler, or a configuration manager.

```python
class JobScheduler(Actor[ScheduleJob | GetPendingJobs]):
    def __init__(self):
        self.pending_jobs = []

    async def receive(self, msg, ctx: Context) -> None:
        match msg:
            case ScheduleJob(job):
                self.pending_jobs.append(job)
            case GetPendingJobs():
                await ctx.reply(self.pending_jobs)

async def main():
    async with ActorSystem.clustered("0.0.0.0", 8001) as system:
        # Spawn a singleton - only one exists cluster-wide
        scheduler = await system.spawn(
            JobScheduler,
            name="job-scheduler",
            singleton=True,
        )

        await scheduler.send(ScheduleJob(my_job))
```

### How Singletons Work

1. The cluster assigns the singleton to a node (tracked via gossip)
2. All nodes discover where the singleton lives via the gossip protocol
3. Messages are automatically routed to the owning node
4. If the owner fails (detected by SWIM), the singleton is reassigned to another node

This provides reliable singleton management with automatic failover.

---

## State Replication

Distributed systems must handle node failures. If a node crashes, what happens to the state of actors running on it?

Casty solves this with **state replication**: after each message, an actor's state is automatically replicated to backup nodes.

### Automatic State Serialization

By default, Casty serializes all public instance attributes:

```python
class Account(Actor[Deposit | Withdraw | GetBalance]):
    def __init__(self, entity_id: str):
        self.entity_id = entity_id
        self.balance = 0          # Automatically replicated
        self._cache = {}          # NOT replicated (starts with _)

    async def receive(self, msg, ctx: Context) -> None:
        match msg:
            case Deposit(amount):
                self.balance += amount
            case Withdraw(amount):
                self.balance -= amount
            case GetBalance():
                await ctx.reply(self.balance)
```

After each message, `{entity_id: "...", balance: ...}` is sent to replica nodes.

### Excluding Fields

To exclude specific fields from replication:

```python
class CachedService(Actor[Query]):
    __casty_exclude_fields__ = {"cache", "temp_results"}

    def __init__(self, entity_id: str):
        self.entity_id = entity_id
        self.cache = {}           # Excluded - not replicated
        self.temp_results = []    # Excluded - not replicated
        self.query_count = 0      # Included - replicated
```

### Custom Serialization

For complex cases, override `get_state()` and `set_state()`:

```python
class ComplexActor(Actor[Msg]):
    def __init__(self, entity_id: str):
        self.entity_id = entity_id
        self.data = SomeComplexObject()

    def get_state(self) -> dict:
        return {
            "entity_id": self.entity_id,
            "data": self.data.serialize(),
        }

    def set_state(self, state: dict) -> None:
        self.entity_id = state["entity_id"]
        self.data = SomeComplexObject.deserialize(state["data"])
```

### Replication Modes

Configure how replication behaves with `ReplicationConfig`:

```python
from casty.persistence import ReplicationConfig

# Async replication (default) - fast, eventual consistency
config = ReplicationConfig(factor=3)

# Wait for 1 replica to acknowledge
config = ReplicationConfig(factor=3, sync_count=1)

# Wait for quorum (e.g., 2 of 3 replicas)
config = ReplicationConfig(factor=3, sync_count=2)

# Wait for ALL replicas (strongest consistency, highest latency)
config = ReplicationConfig(factor=3, sync_count=-1)

# ReplicationConfig can be used with sharded actors
# for configuring state replication behavior
```

**Choosing a Replication Mode:**

| Mode | `sync_count` | Consistency | Latency | Use Case |
|------|--------------|-------------|---------|----------|
| Async | `0` | Eventual | Lowest | High-throughput, loss-tolerant |
| Sync One | `1` | Single backup guaranteed | Low | Balance of safety and speed |
| Sync Quorum | `N/2+1` | Majority confirmed | Medium | Strong consistency needs |
| Sync All | `-1` | All replicas confirmed | Highest | Critical data, rare writes |

---

## Configuration Reference

### ActorSystem.clustered()

The API for distributed systems with automatic actor discovery:

```python
system = ActorSystem.clustered(
    host="0.0.0.0",           # Bind address
    port=7946,                 # Bind port
    seeds=["host:port", ...],  # Seed nodes for bootstrapping
    node_id="unique-id",       # Optional persistent node identifier
)
```

### ClusterConfig

Fine-tune cluster behavior:

```python
from casty.cluster.config import ClusterConfig
from casty.swim import SwimConfig

# Use production presets
config = ClusterConfig.production()

# Or customize
config = ClusterConfig(
    bind_host="0.0.0.0",
    bind_port=7946,
    seeds=["node1:7946", "node2:7946"],
    swim_config=SwimConfig.production(),
    connect_timeout=5.0,       # Connection timeout
    handshake_timeout=5.0,     # Handshake timeout
    ask_timeout=30.0,          # Remote ask timeout
)

# For testing, use development presets
dev_config = ClusterConfig.development()
```

### ReplicationConfig

```python
from casty.persistence import ReplicationConfig

config = ReplicationConfig(
    factor=3,        # Total replicas (including primary)
    sync_count=0,    # 0=async, N=wait for N, -1=wait for all
    timeout=5.0,     # Timeout for sync replication
)
```

### SupervisorConfig

```python
from casty import SupervisorConfig, SupervisionStrategy

config = SupervisorConfig(
    strategy=SupervisionStrategy.RESTART,  # RESTART, STOP, or ESCALATE
    max_restarts=3,                         # Max restarts allowed
    within_seconds=60.0,                    # Time window for max_restarts
    backoff_initial=0.1,                    # Initial backoff delay
    backoff_max=30.0,                       # Maximum backoff delay
    backoff_multiplier=2.0,                 # Backoff multiplication factor
)
```

---

## Architecture Deep Dive

Casty's distributed architecture is built entirely on the actor model—even the cluster infrastructure is implemented as actors.

### SWIM Protocol (Failure Detection)

SWIM (Scalable Weakly-consistent Infection-style Membership) provides efficient failure detection:

1. **Periodic probes**: Each node probes random peers at regular intervals
2. **Indirect probes**: If direct probe fails, ask K other nodes to probe the target
3. **Lifeguard suspicion**: Nodes enter SUSPECT state before being declared DEAD
4. **Adaptive timing**: Protocol period adjusts based on network conditions

The protocol scales O(1) per node—each node does constant work regardless of cluster size.

### Gossip Protocol (State Dissemination)

The gossip protocol provides eventually consistent state across the cluster:

- **Actor registry**: Disseminates actor name → node mappings
- **Vector clocks**: Track causality for conflict resolution
- **Push-pull hybrid**: Combines epidemic (push) and anti-entropy (pull)
- **Pattern subscriptions**: React to state changes matching patterns

### Consistent Hashing (Entity Routing)

**Consistent Hashing** maps entity IDs to nodes deterministically:

- When you send to `accounts["user-123"]`, the ID is hashed to find the primary node
- Virtual nodes (default 150 per physical node) ensure even distribution
- Preference lists determine replica placement
- Only ~1/N entities move when nodes join/leave

### Transport Layer

All network communication uses TCP with:

- **Protocol multiplexing**: Single port handles SWIM, Actor messages, Gossip, and Handshakes
- **Backpressure support**: Write acknowledgment prevents overwhelming receivers
- **TLS support**: Optional encryption via `TLSConfig`

### Why This Architecture?

This design provides:

- **No single point of failure**: Fully decentralized, no leader election needed
- **Low latency**: Message routing is deterministic via consistent hashing
- **Scalability**: O(1) overhead per node for failure detection
- **Simplicity**: Everything is an actor, making the system composable and testable

---

## Examples

### Chat Room Server

```python
@dataclass
class Join:
    user_id: str

@dataclass
class Leave:
    user_id: str

@dataclass
class Message:
    user_id: str
    text: str

@dataclass
class GetMembers:
    pass

class ChatRoom(Actor[Join | Leave | Message | GetMembers]):
    def __init__(self, entity_id: str):
        self.room_id = entity_id
        self.members: set[str] = set()
        self.history: list[tuple[str, str]] = []

    async def receive(self, msg, ctx: Context) -> None:
        match msg:
            case Join(user_id):
                self.members.add(user_id)
            case Leave(user_id):
                self.members.discard(user_id)
            case Message(user_id, text):
                self.history.append((user_id, text))
                # In real app: broadcast to connected clients
            case GetMembers():
                await ctx.reply(list(self.members))

async def main():
    async with ActorSystem.clustered("0.0.0.0", 8001) as system:
        rooms = await system.spawn(
            ChatRoom,
            name="rooms",
            sharded=True,
        )

        await rooms["general"].send(Join("alice"))
        await rooms["general"].send(Join("bob"))
        await rooms["general"].send(Message("alice", "Hello everyone!"))

        members = await rooms["general"].ask(GetMembers())
        print(f"Members: {members}")  # ['alice', 'bob']
```

### Distributed Counter

```python
@dataclass
class Increment:
    amount: int = 1

@dataclass
class GetCount:
    pass

class Counter(Actor[Increment | GetCount]):
    def __init__(self, entity_id: str):
        self.entity_id = entity_id
        self.count = 0

    async def receive(self, msg, ctx: Context) -> None:
        match msg:
            case Increment(amount):
                self.count += amount
            case GetCount():
                await ctx.reply(self.count)

async def main():
    async with ActorSystem.clustered("0.0.0.0", 8001) as system:
        counters = await system.spawn(
            Counter,
            name="counters",
            sharded=True,
        )
        for i in range(100):
            await counters["hits"].send(Increment())

        count = await counters["hits"].ask(GetCount())
        print(f"Total hits: {count}")  # 100
```

---

## Contributing

Contributions are welcome! Please see our [GitHub repository](https://github.com/gabfssilva/casty) for:

- Issue tracker
- Pull request guidelines
- Development setup instructions

---

## License

MIT License - see LICENSE file for details.

---

## Acknowledgments

Casty is inspired by:

- **Erlang/OTP**: The original actor model implementation with supervision trees
- **Akka**: Bringing actors to the JVM with a clean API
- **Microsoft Orleans**: Virtual actors (grains) and transparent distribution
- **Proto.Actor**: Modern actor framework design principles

The distributed systems components build on decades of research:

- **SWIM**: Scalable failure detection by Abhinandan Das et al.
- **Gossip Protocols**: Epidemic algorithms for state dissemination
- **Consistent Hashing**: David Karger et al.'s load balancing technique
