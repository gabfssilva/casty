# Casty

**Casty** is a minimalist, type-safe actor library for Python 3.12+ with distributed cluster support. It implements the actor model elegantly, combining ease of use with type safety through Python's modern advanced type system.

## What is the Actor Model?

The actor model is a concurrent programming paradigm where the fundamental unit of computation is the **actor**. Each actor is an isolated entity that has its own internal state, processes messages sequentially through a mailbox, and can create other actors or send messages to known actors. There is no shared memory between actors — all communication happens exclusively through asynchronous message passing.

This model drastically simplifies concurrent programming by eliminating classic problems like race conditions and deadlocks that arise with locks and shared memory. Casty brings this paradigm to the Python ecosystem in an idiomatic way, leveraging asyncio and the modern type system.

## Installation

Using uv:

```bash
uv add casty
```

Or using pip:

```bash
pip install casty
```

Casty requires Python 3.12 or higher due to its use of PEP 695 generics syntax.

## Core Concepts

### Actors and Messages

In Casty, you define an actor by creating a class that inherits from `Actor[M]`, where `M` is the type of message the actor accepts. Messages are defined as dataclasses, ensuring they are immutable data structures that are easily serializable.

```python
from dataclasses import dataclass
from casty import Actor, Context

@dataclass
class Greet:
    name: str

class Greeter(Actor[Greet]):
    async def receive(self, msg: Greet, ctx: Context[Greet]) -> None:
        print(f"Hello, {msg.name}!")
```

The generic type `Actor[Greet]` is not just documentation — it guarantees at development time that only messages of type `Greet` can be sent to this actor. If you try to send a different type, your editor and type checker will alert you.

### The Actor System

The `ActorSystem` is the runtime that manages the lifecycle of actors. It is responsible for creating actors, delivering messages to their mailboxes, and coordinating graceful shutdown. Every Casty program starts by creating an actor system.

Both `ActorSystem` and `DistributedActorSystem` implement the async context manager protocol, which is the recommended way to use them. This ensures that all actors are properly shut down and resources are released when the context exits, even if an exception occurs.

```python
import asyncio
from casty import ActorSystem

async def main():
    async with ActorSystem() as system:
        # Create an actor and get a reference to it
        greeter = await system.spawn(Greeter, name="greeter")

        # Send a message
        await greeter.send(Greet("World"))

        # Wait a bit for the message to be processed
        await asyncio.sleep(0.1)

    # shutdown() is called automatically when exiting the context

asyncio.run(main())
```

The `spawn` method creates a new instance of the actor and returns an `ActorRef[M]` — an opaque reference that allows sending messages to the actor. You never interact directly with the actor instance; all communication goes through the reference.

### Actor References

The `ActorRef` is the safe way to communicate with an actor. It offers two communication patterns:

The **fire-and-forget** pattern with `send()` sends a message and returns immediately, without waiting for a response. It's useful for notifications and commands where you don't need confirmation.

```python
await greeter.send(Greet("Alice"))
```

The **request-response** pattern with `ask()` sends a message and waits for a response from the actor. This is implemented internally using asyncio futures, and you can specify a timeout.

```python
from dataclasses import dataclass
from casty import Actor, Context

@dataclass
class Increment:
    amount: int = 1

@dataclass
class GetCount:
    pass

class Counter(Actor[Increment | GetCount]):
    def __init__(self):
        self.count = 0

    async def receive(self, msg: Increment | GetCount, ctx: Context) -> None:
        match msg:
            case Increment(amount):
                self.count += amount
            case GetCount():
                ctx.reply(self.count)

# Usage
counter = await system.spawn(Counter, name="counter")
await counter.send(Increment(5))
await counter.send(Increment(3))
value = await counter.ask(GetCount(), timeout=5.0)  # Returns 8
```

Note the use of union types (`Increment | GetCount`) for actors that accept multiple message types. Python 3.10+'s pattern matching combines perfectly with this pattern.

### Context and Actor Hierarchy

The `Context` is passed to the `receive` method and provides important functionality during message processing. Through it, you can respond to `ask` requests and create child actors.

```python
@dataclass
class CreateWorker:
    id: str

class Supervisor(Actor[CreateWorker]):
    async def receive(self, msg: CreateWorker, ctx: Context) -> None:
        # Create a child actor
        worker = await ctx.spawn(Worker, name=f"worker-{msg.id}")
        ctx.reply(worker)  # Return the reference to whoever asked
```

Actors created via `ctx.spawn()` are children of the current actor. This establishes a hierarchy that can be used for supervision and failure management.

### Actor Lifecycle

Each actor has lifecycle hooks that you can override to execute code at specific moments:

```python
class DatabaseActor(Actor[Query]):
    async def on_start(self) -> None:
        """Called once when the actor starts."""
        self.connection = await create_db_connection()
        print("Connection established")

    async def on_stop(self) -> None:
        """Called when the actor is gracefully shut down."""
        await self.connection.close()
        print("Connection closed")

    def on_error(self, exc: Exception) -> bool:
        """
        Called when an exception occurs during receive().
        Return True to continue processing messages.
        Return False to terminate the actor.
        """
        if isinstance(exc, TemporaryError):
            print(f"Temporary error, continuing: {exc}")
            return True
        print(f"Fatal error, shutting down: {exc}")
        return False

    async def receive(self, msg: Query, ctx: Context) -> None:
        result = await self.connection.execute(msg.sql)
        ctx.reply(result)
```

The `on_error` method is particularly important for building resilient systems. It allows you to decide, for each type of exception, whether the actor should continue operating or be terminated.

## Distributed Clusters

One of Casty's most powerful features is native support for distributed clusters. You can create systems where actors on different machines communicate transparently.

### Creating a Cluster

To create a distributed system, use the `distributed()` factory method or instantiate `DistributedActorSystem` directly:

```python
from casty import ActorSystem, DistributedActorSystem

# Using the factory method
system = ActorSystem.distributed(
    host="192.168.1.10",
    port=8001,
    seeds=["192.168.1.11:8001", "192.168.1.12:8001"],
    expected_cluster_size=3
)

# Or directly
system = DistributedActorSystem(
    host="192.168.1.10",
    port=8001,
    seeds=["192.168.1.11:8001", "192.168.1.12:8001"],
    expected_cluster_size=3
)
```

The `seeds` parameter specifies the addresses of other cluster nodes for initial connection. The `expected_cluster_size` tells how many nodes the cluster should have before starting leader election.

### Starting the Server

The easiest way to run a distributed system is using the async context manager. When you enter the context, the TCP server starts automatically in the background, connects to seed nodes, and begins the leader election process.

```python
async def main():
    async with DistributedActorSystem("0.0.0.0", 8001, expected_cluster_size=3) as system:
        # The server is already running in the background
        await system.spawn(MyService, name="my-service")

        # Do your work here...
        await asyncio.sleep(60)

    # shutdown() and cleanup are called automatically

asyncio.run(main())
```

Alternatively, if you need more control over the server lifecycle, you can call `serve()` directly. This method starts the TCP server, connects to seeds, participates in the peer discovery protocol, and starts the leader election loop. It blocks until `shutdown()` is called.

```python
async def main():
    system = DistributedActorSystem("0.0.0.0", 8001, expected_cluster_size=3)
    await system.spawn(MyService, name="my-service")

    # Start the server (blocks until shutdown)
    await system.serve()
```

### Remote Actor Discovery

To find an actor on any node in the cluster, use the `lookup()` method:

```python
# This code can run on any node in the cluster
service = await system.lookup("my-service")

# From here, you can use service normally
# Casty handles serialization and network communication
await service.send(ProcessRequest(data="hello"))
response = await service.ask(GetStatus())
```

The `lookup()` first checks the local registry. If the actor is not found locally, it sends a lookup request to all connected peers. When found, it returns a `RemoteRef` that behaves exactly like a local `ActorRef`.

### Peer Discovery Protocol

Casty implements a gossip protocol for peer discovery. When a new node connects to a seed:

1. The new node announces its presence to the seed
2. The seed responds with the list of all known peers
3. The new node connects to all peers in the list
4. Each existing peer also adds the new node to its list

This creates a full mesh topology where all nodes are connected to all others, ensuring that messages can be routed directly between any two nodes.

### Raft Consensus Protocol

Casty implements the complete Raft consensus algorithm, providing strong consistency guarantees for distributed state. The implementation includes leader election, log replication, and state persistence.

#### Leader Election

In a cluster, only one node is the leader at any time, and the others are followers. Election happens automatically when the cluster is formed or when the current leader fails (detected by the absence of heartbeats).

```python
# Check if this node is the leader
if system.is_leader:
    print(f"This node is the leader")
else:
    print(f"The current leader is {system.leader_id}")
```

The election algorithm includes the **election restriction**: a candidate can only win an election if its log is at least as up-to-date as the majority of the cluster. This ensures that the elected leader always has all committed entries.

#### Log Replication

The leader maintains a replicated log of commands. When you want to execute a command with strong consistency, use the `append_command()` method:

```python
# Only the leader can append commands
if system.is_leader:
    success = await system.append_command({"action": "set", "key": "x", "value": 42})
    if success:
        print("Command replicated to majority and committed")
```

The leader:
1. Appends the command to its local log
2. Sends `AppendEntries` RPCs to all followers in parallel
3. Waits for a majority of nodes to acknowledge replication
4. Commits the entry once a majority has replicated it
5. Applies the command to the state machine

Followers perform a **consistency check** on each `AppendEntries` request, ensuring their log matches the leader's before appending new entries. If there's a mismatch, the leader decrements `next_index` for that follower and retries until logs converge.

#### State Machine Application

To integrate with your application's state machine, set an apply callback:

```python
async def apply_command(command: dict) -> None:
    """Called for each committed command in order."""
    match command:
        case {"action": "set", "key": key, "value": value}:
            state[key] = value
        case {"action": "delete", "key": key}:
            state.pop(key, None)

system.set_apply_callback(apply_command)
```

Commands are applied in log order, and only after they are committed (replicated to a majority). This guarantees that all nodes see the same sequence of state changes.

#### State Persistence

Casty persists critical Raft state to disk to survive crashes:

```python
system = DistributedActorSystem(
    "127.0.0.1",
    8001,
    expected_cluster_size=3,
    persistence_dir="/var/lib/casty/data",  # Enable persistence
    node_id="node-1",  # Use consistent node_id across restarts
)
```

The following state is persisted with `fsync` before responding to any RPC:
- **current_term**: The latest term the server has seen
- **voted_for**: The candidate that received this node's vote in the current term
- **log entries**: All log entries with their terms and commands

This ensures that after a crash and restart, the node can rejoin the cluster and continue from where it left off.

#### Snapshots

For clusters with large logs, Casty supports snapshotting to compact the log:

```python
async def create_snapshot() -> bytes:
    """Serialize current state machine state."""
    return json.dumps(state).encode()

async def restore_snapshot(data: bytes) -> None:
    """Restore state machine from snapshot."""
    state.clear()
    state.update(json.loads(data))

system.set_snapshot_callbacks(create_snapshot, restore_snapshot)

# Later, when log grows large:
if system.is_leader:
    await system.create_snapshot()
```

When a follower is far behind (its `next_index` points before the leader's first log entry), the leader sends an `InstallSnapshot` RPC instead of log entries.

### Singleton Actors

Singleton actors are cluster-wide unique actors — only one instance exists across the entire cluster, regardless of which node you spawn it from. This is useful for coordinating global state, managing shared resources, or implementing services that should only run once.

```python
from casty import DistributedActorSystem

@dataclass
class GetConfig:
    pass

class ConfigManager(Actor[GetConfig]):
    def __init__(self):
        self.config = {"feature_x": True, "max_connections": 100}

    async def receive(self, msg: GetConfig, ctx: Context) -> None:
        ctx.reply(self.config)

async def main():
    async with DistributedActorSystem("0.0.0.0", 8001, expected_cluster_size=3) as system:
        # Spawn a singleton - only one instance will exist cluster-wide
        config = await system.spawn(ConfigManager, name="config", singleton=True)

        # From any node, you can get a reference to the singleton
        config_ref = await system.lookup("config", singleton=True)

        # If the singleton exists on another node, you get a RemoteRef
        current_config = await config_ref.ask(GetConfig())
```

Key characteristics of singleton actors:
- **Unique**: Only one instance exists across the cluster
- **Transparent location**: You get either a local `ActorRef` or a `RemoteRef` depending on where the singleton lives
- **Raft-replicated registry**: The singleton registry is replicated via Raft for consistency
- **Automatic routing**: Messages are automatically routed to the node hosting the singleton

### Cluster Sharding

Cluster Sharding allows you to distribute actors across multiple nodes based on entity IDs. This is perfect for scenarios where you have many entities (users, accounts, devices, etc.) that need to be distributed for scalability, while maintaining strong consistency through Raft.

```python
from dataclasses import dataclass
from casty import Actor, Context, DistributedActorSystem

@dataclass
class Deposit:
    amount: float

@dataclass
class GetBalance:
    pass

class Account(Actor[Deposit | GetBalance]):
    def __init__(self, entity_id: str):
        self.entity_id = entity_id
        self.balance = 0.0

    async def receive(self, msg: Deposit | GetBalance, ctx: Context) -> None:
        match msg:
            case Deposit(amount):
                self.balance += amount
            case GetBalance():
                ctx.reply(self.balance)

async def main():
    async with DistributedActorSystem("0.0.0.0", 8001, expected_cluster_size=3) as system:
        # Create a sharded actor type with 100 shards
        accounts = await system.spawn(Account, name="account", shards=100)

        # Access entities using subscription syntax
        await accounts["user-1"].send(Deposit(100))
        await accounts["user-1"].send(Deposit(50))

        balance = await accounts["user-1"].ask(GetBalance())
        assert balance == 150.0

        # Different entities can be on different nodes
        await accounts["user-2"].send(Deposit(200))
        await accounts["user-3"].send(Deposit(300))
```

#### How Sharding Works

1. **Shard Allocation**: When you spawn with `shards=N`, Casty creates N shards and distributes them across cluster nodes using round-robin allocation via Raft consensus.

2. **Entity Routing**: Each entity ID is mapped to a shard using `hash(entity_id) % num_shards`. The same entity ID always routes to the same shard.

3. **Message Ordering**: Messages go through the Raft log, ensuring consistent ordering across the cluster. This guarantees that two messages to the same entity are applied in the order they were sent.

4. **Lazy Entity Creation**: Entity actors are created on-demand when they receive their first message. The actor receives the `entity_id` as a constructor parameter.

5. **Response Routing**: For `ask()` requests, the response bypasses Raft and goes directly back to the requesting node for lower latency.

#### ShardedRef and EntityRef

The `spawn(..., shards=N)` method returns a `ShardedRef` instead of an `ActorRef`:

```python
# ShardedRef provides access to entities
accounts = await system.spawn(Account, name="account", shards=100)

# Get an EntityRef for a specific entity
user_ref = accounts["user-1"]

# EntityRef supports send and ask
await user_ref.send(Deposit(100))
balance = await user_ref.ask(GetBalance())

# You can also use ShardedRef directly
await accounts.send("user-1", Deposit(100))
balance = await accounts.ask("user-1", GetBalance())
```

#### Distributed Sharding

Sharding works seamlessly in distributed clusters. Actor and message types are automatically imported on other nodes when needed:

```python
async with node1, node2, node3:
    # Wait for leader election
    leader = ...  # wait for leader election

    # Spawn sharded actor - types are auto-imported on all nodes
    accounts = await leader.spawn(Account, name="account", shards=100)

    # Entities are distributed across nodes automatically
    await accounts["user-1"].send(Deposit(100))  # May route to any node
```

The auto-import mechanism uses Python's `importlib` to dynamically load classes by their fully qualified name. This means actor and message classes must be importable (i.e., defined in a proper Python module, not in `__main__` or a notebook).

### State Replication

Sharded entity state is automatically replicated to backup nodes in memory, enabling state restoration on failover. This ensures entities survive node crashes without losing their state — **no configuration required**.

```python
async with DistributedActorSystem("0.0.0.0", 8001, expected_cluster_size=3) as system:
    accounts = await system.spawn(Account, name="account", shards=100)

    # State is automatically replicated to 2 backup nodes (default)
    await accounts["user-1"].send(Deposit(100))

    # If this node crashes, the entity will be recreated on another node
    # with its state restored from backup
```

To customize replication behavior, pass a `ReplicationConfig`:

```python
from casty import DistributedActorSystem, ReplicationConfig

# Disable replication (for development/testing)
config = ReplicationConfig(memory_replicas=0)

# Or increase backup copies
config = ReplicationConfig(memory_replicas=3)

async with DistributedActorSystem(
    "0.0.0.0", 8001,
    expected_cluster_size=3,
    replication=config,
) as system:
    ...
```

#### How State Replication Works

1. **State Extraction**: After processing each message, the entity's state is serialized (using the actor's `__dict__` by default, excluding private fields).

2. **Replication via Raft**: The serialized state is appended to the Raft log with a version number, ensuring consistent ordering.

3. **Backup Storage**: Backup nodes store the state in an in-memory `BackupStateManager` with LRU eviction.

4. **State Restoration**: When an entity is recreated (after failover), Casty first checks for backup state and restores it before processing new messages.

#### Customizing State Serialization (Optional)

By default, state replication works automatically without any changes to your actor class. Casty serializes all fields from `__dict__` except those starting with `_`:

```python
# This works automatically - no special inheritance needed
class Account(Actor[Deposit | GetBalance]):
    def __init__(self, entity_id: str):
        self.entity_id = entity_id
        self.balance = 0.0        # Replicated automatically
        self.name = "test"        # Replicated automatically
        self._cache = {}          # Ignored (starts with _)
```

For more control over which fields are persisted, you can optionally use the `PersistentActorMixin` or `@persistent` decorator:

```python
from casty import Actor, Context, PersistentActorMixin, persistent

# Option 1: Using mixin with explicit exclusion
class Account(PersistentActorMixin, Actor[Deposit | GetBalance]):
    __casty_exclude_fields__ = {"temp_data", "metrics"}

    def __init__(self, entity_id: str):
        self.entity_id = entity_id
        self.balance = 0.0      # Persisted
        self.temp_data = None   # Excluded explicitly
        self.metrics = {}       # Excluded explicitly

# Option 2: Using decorator with whitelist (only these fields)
@persistent("balance", "transaction_count")
class Counter(Actor[Increment | GetCount]):
    def __init__(self, entity_id: str):
        self.entity_id = entity_id      # Not persisted
        self.balance = 0.0              # Persisted
        self.transaction_count = 0      # Persisted
        self.temp_data = None           # Not persisted

# Option 3: Full control with protocol methods
class ComplexActor(Actor[SomeMessage]):
    def __casty_get_state__(self) -> dict:
        return {"important": self.important_state}

    def __casty_set_state__(self, state: dict) -> None:
        self.important_state = state["important"]
        self._derived = self._compute_derived()  # Recompute derived fields
```

#### Replication Presets

For common use cases, Casty provides preset configurations:

```python
from casty import ReplicationConfig, ReplicationPresets

# Development - no replication overhead
config = ReplicationPresets.development()

# High performance - async replication
config = ReplicationPresets.high_performance()

# Balanced (recommended) - 2 replicas in memory
config = ReplicationPresets.balanced()

# High durability - for critical data
config = ReplicationPresets.high_durability()
```

### Message Serialization

For communication between nodes, messages need to be serialized. Casty uses msgpack and automatically supports dataclasses and Python primitive types (int, float, str, bool, None, bytes, list, dict).

```python
@dataclass
class ComplexMessage:
    id: int
    name: str
    data: dict
    tags: list[str]

# Works automatically in remote communication
await remote_actor.send(ComplexMessage(
    id=42,
    name="test",
    data={"key": "value"},
    tags=["tag1", "tag2"]
))
```

For custom types, you can register them with the system for better performance:

```python
system.register_type(ComplexMessage)
```

This adds the type to the internal registry, avoiding the need for dynamic import during deserialization.

## Complete Example: Distributed Chat System

Here's a more complete example that demonstrates several Casty features:

```python
import asyncio
from dataclasses import dataclass
from casty import Actor, Context, DistributedActorSystem

# Message definitions
@dataclass
class Join:
    username: str

@dataclass
class Leave:
    username: str

@dataclass
class Message:
    username: str
    content: str

@dataclass
class GetUsers:
    pass

# The ChatRoom actor manages a chat room
class ChatRoom(Actor[Join | Leave | Message | GetUsers]):
    def __init__(self, room_name: str):
        self.room_name = room_name
        self.users: set[str] = set()

    async def on_start(self) -> None:
        print(f"Room '{self.room_name}' started")

    async def on_stop(self) -> None:
        print(f"Room '{self.room_name}' closed with {len(self.users)} users")

    async def receive(self, msg: Join | Leave | Message | GetUsers, ctx: Context) -> None:
        match msg:
            case Join(username):
                self.users.add(username)
                print(f"[{self.room_name}] {username} joined the room")

            case Leave(username):
                self.users.discard(username)
                print(f"[{self.room_name}] {username} left the room")

            case Message(username, content):
                print(f"[{self.room_name}] {username}: {content}")

            case GetUsers():
                ctx.reply(list(self.users))

async def run_node(host: str, port: int, seeds: list[str], is_main: bool):
    """Run a cluster node."""
    async with DistributedActorSystem(host, port, seeds, expected_cluster_size=2) as system:
        # The main node creates the chat room
        if is_main:
            await system.spawn(ChatRoom, name="general", room_name="General")

        # Wait for cluster to be ready
        await asyncio.sleep(2)

        # Any node can interact with the room
        room = await system.lookup("general")

        node_user = f"user-{port}"
        await room.send(Join(node_user))
        await room.send(Message(node_user, f"Hello from port {port}!"))

        users = await room.ask(GetUsers())
        print(f"Users in room: {users}")

        # Keep running for a while
        await asyncio.sleep(5)

        await room.send(Leave(node_user))

    # shutdown() and cleanup are called automatically

# Run two nodes
async def main():
    await asyncio.gather(
        run_node("127.0.0.1", 8001, [], is_main=True),
        run_node("127.0.0.1", 8002, ["127.0.0.1:8001"], is_main=False),
    )

if __name__ == "__main__":
    asyncio.run(main())
```

## Internal Architecture

For those interested in understanding how Casty works internally, here's an overview of the architecture.

### Module Structure

The code is organized into well-defined layers. The core layer contains `actor.py` with the fundamental primitives (Actor, ActorRef, Context, Envelope) and `system.py` with the local runtime. The cluster layer in `cluster/` extends the system with distributed capabilities through specialized mixins.

The mixins follow the single responsibility principle:
- **`PeerMixin`**: Manages TCP connections between nodes and the gossip protocol
- **`MessagingMixin`**: Implements lookup and remote message sending
- **`SingletonMixin`**: Handles cluster-wide singleton actors with Raft-replicated registry

The Multi-Raft layer (`cluster/multiraft/`) provides:
- **`RaftGroup`**: Self-contained Raft consensus implementation for one group
- **`MultiRaftManager`**: Manages multiple independent Raft groups with batched messaging

The sharding system adds:
- **`ShardedRef`** / **`EntityRef`**: Type-safe references for sharded entities
- **Shard allocation**: Round-robin distribution via Raft consensus
- **Entity lifecycle**: Lazy creation and message routing

The persistence layer (`cluster/persistence.py`) provides:
- **`BackupStateManager`**: In-memory backup state storage with LRU eviction
- **`ReplicationConfig`**: Configurable replication strategies and modes
- **State protocols**: `PersistentActorMixin` and `@persistent` for custom serialization

The `DistributedActorSystem` composes all these functionalities into a cohesive distributed actor runtime.

### The Envelope Pattern

Internally, messages are encapsulated in an `Envelope` before being placed in the actor's mailbox. The envelope carries the original message and, optionally, a future for the ask pattern. This allows the system to implement request-response without exposing this complexity to the user.

### Transport Protocol

Communication between nodes uses TCP with a simple binary protocol. Each message on the network is prefixed with its size (length-prefix framing), allowing the receiver to know exactly how many bytes to read. The format is: message type (1 byte), actor name length (2 bytes), actor name, payload length (4 bytes), and msgpack-serialized payload.

Message types include:
- `ACTOR_MSG` (1): Regular actor messages
- `LOOKUP_REQ/RES` (2-3): Actor discovery
- `PEER_LIST` (4): Gossip protocol
- `ASK_REQ/RES` (5-6): Request-response pattern
- `VOTE_REQUEST/RESPONSE` (7-8): Raft election
- `HEARTBEAT` (9): Legacy heartbeat (now uses AppendEntries)
- `APPEND_ENTRIES_REQ/RES` (12-13): Log replication
- `INSTALL_SNAPSHOT_REQ/RES` (14-15): Snapshot transfer

### Type Safety in Depth

The use of generics in `Actor[M]`, `ActorRef[M]`, and `Context[M]` creates a chain of types that flows from the moment of actor definition to message sending. When you do `await system.spawn(MyActor)`, the system infers that the return is `ActorRef[MyMessage]` based on `MyActor`'s definition. This allows type checkers like mypy and pyright to detect errors at development time.

## Design Philosophy

Casty was designed with several principles in mind.

**Minimalism over features**: The library does one thing well — the actor model. It doesn't try to be a complete framework with HTTP, databases, or other integrations. You compose Casty with other libraries as needed.

**Type safety without boilerplate**: Python 3.12+'s type system allows expressing powerful constraints without complex decorators or verbose configuration. A simple `Actor[MyMessage]` captures the intent clearly and verifiably.

**Location transparency**: The code that interacts with a local actor is identical to code that interacts with a remote actor. This allows you to develop and test locally, then distribute without changes to business code.

**Failures as first-class citizens**: The `on_error` method and lifecycle hooks acknowledge that failures happen. Instead of hiding this, Casty gives you the tools to handle failures explicitly.

**Strong consistency when needed**: The full Raft implementation provides linearizable semantics for distributed state, ensuring all nodes see the same sequence of committed operations.

## Known Limitations

**No backpressure**: Actor mailboxes have no backpressure mechanism. If an actor receives messages faster than it can process them, the queue grows indefinitely. For high-load production systems, consider implementing rate limiting at the application level.

**Static cluster membership**: Adding or removing nodes from a running cluster (membership changes) is not yet supported. The cluster size must be known at startup.

**State replication is in-memory only**: Sharded entity state is replicated to backup nodes in memory, surviving individual node crashes. However, if all nodes restart simultaneously, state is lost. Disk persistence (SQLite, PostgreSQL, etc.) is planned for future releases.

**Classes must be importable**: Actor and message classes must be defined in proper Python modules (not `__main__` or notebooks) for the auto-import mechanism to work in distributed clusters.

## Contributing

Contributions are welcome! Casty is a young project with plenty of room for improvements. Some areas of interest include:

- **Hierarchical supervision**: Erlang/Elixir-style supervision trees for fault tolerance
- **Metrics and observability**: Built-in instrumentation for monitoring cluster health
- **Dynamic membership**: Adding/removing nodes from running clusters (joint consensus)
- **Disk persistence**: SQLite, PostgreSQL, or other storage backends for durable state
- **Shard rebalancing**: Automatic redistribution when nodes join/leave
- **Performance optimizations**: Batching, compression, and connection pooling

## License

MIT License - see the LICENSE file for details.
