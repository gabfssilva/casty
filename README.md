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
6. [Spawning and Hierarchies](#spawning-and-hierarchies)
7. [Supervision and Fault Tolerance](#supervision-and-fault-tolerance)
8. [Going Distributed](#going-distributed)
9. [Sharded Actors](#sharded-actors)
10. [Singleton Actors](#singleton-actors)
11. [State Replication](#state-replication)
12. [Configuration Reference](#configuration-reference)

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
                ctx.reply(self.count)

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
        ctx.reply(result)
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
    ctx.reply(some_result)

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

Under the hood, `ask()` creates a `Future`, sends the message with a reference to that future, and awaits the result. The actor responds by calling `ctx.reply()`.

**Important:** Every `ask()` should have a corresponding `ctx.reply()` in the actor, or the caller will timeout.

```python
class Calculator(Actor[Add | Multiply]):
    async def receive(self, msg: Add | Multiply, ctx: Context) -> None:
        match msg:
            case Add(a, b):
                ctx.reply(a + b)  # Always reply to ask() calls
            case Multiply(a, b):
                ctx.reply(a * b)
```

### Choosing Between Patterns

| Pattern | Use When | Characteristics |
|---------|----------|-----------------|
| `send()` | Fire-and-forget commands, events, notifications | Fast, non-blocking, no backpressure |
| `ask()` | Queries, operations requiring confirmation | Blocks caller, provides result/error |

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

---

## Going Distributed

So far, everything runs in a single process. But what happens when you need:

- **Horizontal scaling** across multiple machines?
- **Fault tolerance** when machines fail?
- **Data locality** for performance?

Casty's distributed mode addresses all of these with a single API change:

```python
# Local system
async with ActorSystem() as system:
    ...

# Distributed system - same API!
async with ActorSystem.distributed(
    host="0.0.0.0",
    port=8001,
    seeds=["node2:8001", "node3:8001"],
) as system:
    ...
```

### How It Works

Casty's distributed architecture separates concerns into two planes:

**Control Plane (Raft Consensus)**
- Manages cluster membership (which nodes are in the cluster)
- Tracks singleton actor locations
- Stores entity type configurations
- Provides strong consistency for metadata

**Data Plane (Consistent Hashing + Gossip)**
- Routes messages to the correct node
- Detects node failures via SWIM protocol
- Handles automatic failover
- Optimizes for low latency

This separation means most operations (sending messages, querying actors) don't need consensus, keeping latency low while still providing consistency where it matters.

### Cluster Formation

Nodes discover each other through seed nodes:

```python
# Node 1 (first node, no seeds needed)
system1 = ActorSystem.distributed("0.0.0.0", 8001)

# Node 2 (joins via Node 1)
system2 = ActorSystem.distributed("0.0.0.0", 8002, seeds=["node1:8001"])

# Node 3 (can use any existing node as seed)
system3 = ActorSystem.distributed("0.0.0.0", 8003, seeds=["node1:8001"])
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
                ctx.reply(self.score)

async def main():
    async with ActorSystem.distributed("0.0.0.0", 8001) as system:
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
                ctx.reply(self.pending_jobs)

async def main():
    async with ActorSystem.distributed("0.0.0.0", 8001) as system:
        # Spawn a singleton - only one exists cluster-wide
        scheduler = await system.spawn(
            JobScheduler,
            name="job-scheduler",
            singleton=True,
        )

        await scheduler.send(ScheduleJob(my_job))
```

### How Singletons Work

1. The cluster leader (via Raft consensus) assigns the singleton to a node
2. All nodes know where the singleton lives via the replicated cluster state
3. Messages are automatically routed to the owning node
4. If the owner fails, the leader reassigns the singleton to another node

This provides strong consistency for singleton ownership while keeping message routing fast.

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
                ctx.reply(self.balance)
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

# Use when spawning
accounts = await system.spawn(
    Account,
    name="accounts",
    sharded=True,
    replication=ReplicationConfig(factor=3, sync_count=1),
)
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

### ActorSystem.distributed()

```python
system = ActorSystem.distributed(
    host="0.0.0.0",           # Bind address
    port=8001,                 # Bind port
    seeds=["host:port", ...],  # Seed nodes for discovery
    node_id="unique-id",       # Persistent node identifier
    raft_config=RaftConfig(),  # Raft timing configuration
)
```

### RaftConfig

Fine-tune Raft consensus timing:

```python
from casty.cluster.control_plane import RaftConfig

config = RaftConfig(
    heartbeat_interval=0.3,     # Leader heartbeat frequency
    election_timeout_min=1.0,   # Minimum election timeout
    election_timeout_max=2.0,   # Maximum election timeout
)
```

For testing, use faster timeouts:

```python
FAST_CONFIG = RaftConfig(
    heartbeat_interval=0.05,
    election_timeout_min=0.1,
    election_timeout_max=0.2,
)
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

### Control Plane: Raft Consensus

The control plane uses the Raft consensus algorithm to maintain consistent cluster state across all nodes. This includes:

- **Cluster membership**: Which nodes are part of the cluster
- **Singleton assignments**: Which node owns each singleton actor
- **Entity type registry**: Configuration for sharded actor types

Raft ensures that even if nodes fail or the network partitions, the cluster state remains consistent. A leader is elected, and all changes go through the leader to guarantee ordering.

### Data Plane: Consistent Hashing + SWIM

The data plane handles the high-frequency operations: routing messages to actors and detecting failures.

**Consistent Hashing** maps entity IDs to nodes. When you send to `accounts["user-123"]`, the ID is hashed to determine the primary node and replicas. This provides:

- Deterministic routing without central coordination
- Minimal redistribution when nodes join/leave
- Natural load balancing across nodes

**SWIM (Scalable Weakly-consistent Infection-style Membership)** protocol detects node failures through:

1. Periodic health probes between random node pairs
2. Indirect probes through other nodes when direct probes fail
3. Suspicion mechanism to handle temporary network issues
4. Protocol distributed failure detection scales O(1) per node

### Why Two Planes?

Separating control and data planes provides the best of both worlds:

- **Control plane** (Raft): Strong consistency for rare but critical operations
- **Data plane** (Hashing + Gossip): Low latency for frequent operations

Most actor messages never touch the control plane—they're routed directly via consistent hashing. Only cluster membership changes and singleton assignments need consensus.

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
                ctx.reply(list(self.members))

async def main():
    async with ActorSystem.distributed("0.0.0.0", 8001) as system:
        rooms = await system.spawn(
            ChatRoom,
            name="rooms",
            sharded=True,
            replication=ReplicationConfig(factor=2, sync_count=1),
        )

        await rooms["general"].send(Join("alice"))
        await rooms["general"].send(Join("bob"))
        await rooms["general"].send(Message("alice", "Hello everyone!"))

        members = await rooms["general"].ask(GetMembers())
        print(f"Members: {members}")  # ['alice', 'bob']
```

### Distributed Counter with Consistency Guarantee

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
                ctx.reply(self.count)

async def main():
    async with ActorSystem.distributed("0.0.0.0", 8001) as system:
        # Strong consistency: wait for quorum
        counters = await system.spawn(
            Counter,
            name="counters",
            sharded=True,
            replication=ReplicationConfig(factor=3, sync_count=2),
        )

        # These writes are confirmed by 2/3 replicas before returning
        for i in range(100):
            await counters["hits"].send(Increment())

        count = await counters["hits"].ask(GetCount())
        print(f"Total hits: {count}")  # Guaranteed: 100
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

- **Raft**: Diego Ongaro and John Ousterhout's consensus algorithm
- **SWIM**: Scalable failure detection by Abhinandan Das et al.
- **Consistent Hashing**: David Karger et al.'s load balancing technique
