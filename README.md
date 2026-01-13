# Casty

**Casty** is a minimalist, type-safe actor library for Python 3.12+ with distributed cluster support. It implements the actor model elegantly, combining ease of use with type safety through Python's modern advanced type system.

## What is the Actor Model?

The actor model is a concurrent programming paradigm where the fundamental unit of computation is the **actor**. Each actor is an isolated entity that has its own internal state, processes messages sequentially through a mailbox, and can create other actors or send messages to known actors. There is no shared memory between actors — all communication happens exclusively through asynchronous message passing.

This model drastically simplifies concurrent programming by eliminating classic problems like race conditions and deadlocks that arise with locks and shared memory. Casty brings this paradigm to the Python ecosystem in an idiomatic way, leveraging asyncio and the modern type system.

## Installation

```bash
pip install casty
```

Or using uv:

```bash
uv add casty
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
- **`ElectionMixin`**: Handles leader election with election restriction
- **`PersistenceMixin`**: Manages durable state (term, voted_for, log) with fsync
- **`LogReplicationMixin`**: Implements AppendEntries RPC and commit advancement
- **`SnapshotMixin`**: Handles snapshot creation and InstallSnapshot RPC

The `DistributedActorSystem` inherits from all of them, composing the functionalities.

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

There is no backpressure in actor mailboxes. If an actor receives messages faster than it can process them, the queue grows indefinitely. For high-load production systems, consider implementing rate limiting at the application level.

Membership changes (adding/removing nodes from a running cluster) are not yet supported. The cluster size must be known at startup.

## Contributing

Contributions are welcome! Casty is a young project with plenty of room for improvements. Some areas of interest include Erlang/Elixir-style hierarchical supervision support, metrics and observability, membership changes (joint consensus), and performance optimizations.

## License

MIT License - see the LICENSE file for details.
