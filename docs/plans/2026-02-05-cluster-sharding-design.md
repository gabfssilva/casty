# Casty Cluster Sharding Design

## Goal

Distribute actors (entities) across multiple nodes with automatic sharding,
rebalancing, and location transparency. Everything is an actor.

## Architecture Layers

```
┌─────────────────────────────────┐
│  4. Cluster Sharding            │  entities distributed across nodes
├─────────────────────────────────┤
│  3. Cluster Membership          │  gossip, phi accrual, leader
├─────────────────────────────────┤
│  2. Remoting                    │  pluggable transport + serialization
├─────────────────────────────────┤
│  1. Addressable ActorRef        │  refs with address, not just Callable
└─────────────────────────────────┘
```

## Design Principles

- **Actor-first**: cluster, transport, gossip, heartbeat, coordinator — all actors
- **Pluggable**: Protocol interfaces for transport and serialization; stdlib defaults
- **At-most-once delivery**: no delivery guarantees at transport level
- **Location transparency**: `ref.tell(msg)` works identically for local and remote
- **Extensible**: hooks for entity persistence and consistency, not implemented in v1

---

## Layer 1: Addressable ActorRef

### ActorAddress

```python
@dataclass(frozen=True)
class ActorAddress:
    system: str          # ActorSystem name
    host: str | None     # None = local
    port: int | None     # None = local
    path: str            # "/greeter/worker-1"

    @property
    def is_local(self) -> bool:
        return self.host is None

    def to_uri(self) -> str:
        # casty://my-system@localhost:25520/greeter/worker-1
        # casty://my-system/greeter/worker-1  (local)
        ...

    @staticmethod
    def from_uri(uri: str) -> ActorAddress: ...
```

### ActorRef (refactored)

```python
class MessageTransport(Protocol):
    def deliver(self, address: ActorAddress, msg: Any) -> None: ...

@dataclass(frozen=True)
class ActorRef[M]:
    address: ActorAddress
    _transport: MessageTransport

    def tell(self, msg: M) -> None:
        self._transport.deliver(self.address, msg)
```

### LocalTransport

Replaces the current Callable approach. Puts messages directly in the mailbox.

```python
class LocalTransport:
    _cells: dict[str, ActorCell]  # path → cell

    def deliver(self, address: ActorAddress, msg: Any) -> None:
        cell = self._cells.get(address.path)
        if cell:
            cell._deliver(msg)
        else:
            # dead letter
```

### ActorRef Serialization

- **On wire**: only `ActorAddress` is serialized (URI string)
- **On deserialization**: `system.resolve(address)` creates ActorRef with correct transport
  - Local address → LocalTransport
  - Remote address → RemoteTransport

---

## Layer 2: Remoting

### Serialization (pluggable)

```python
class Serializer(Protocol):
    def serialize(self, obj: Any) -> bytes: ...
    def deserialize(self, data: bytes) -> Any: ...
```

**Default**: `JsonSerializer` using stdlib `json` + type registry.

**Type Registry**: maps type names → classes for deserialization.

```python
class TypeRegistry:
    _types: dict[str, type]

    def register[T](self, cls: type[T]) -> None:
        self._types[f"{cls.__module__}.{cls.__qualname__}"] = cls

    def resolve(self, name: str) -> type: ...
```

### Message Envelope

```python
@dataclass(frozen=True)
class MessageEnvelope:
    target: str      # URI of target actor
    sender: str      # URI of sender (for reply_to)
    payload: bytes   # serialized message
    type_hint: str   # type name for deserialization
```

### Transport (pluggable)

```python
class Transport(Protocol):
    async def start(self, handler: InboundHandler) -> None: ...
    async def send(self, address: ActorAddress, envelope: bytes) -> None: ...
    async def stop(self) -> None: ...

class InboundHandler(Protocol):
    async def on_message(self, envelope: bytes) -> None: ...
```

**Default**: `TcpTransport` using `asyncio.start_server` / `asyncio.open_connection`.

### TransportActor (actor-first)

Wraps the Transport protocol as an actor in the system:

```python
# Messages
@dataclass(frozen=True)
class SendRemote:
    envelope: MessageEnvelope

@dataclass(frozen=True)
class InboundMessage:
    envelope: MessageEnvelope
```

The TransportActor:
- Owns the TcpTransport
- Manages connection pool (one persistent TCP connection per remote node)
- Handles reconnect with exponential backoff (1s, 2s, 4s... max 30s)
- On connection failure: buffers messages (bounded), retries, then dead-letters
- Publishes `RemoteDeadLetter` events on the EventStream

### Connection Management

- One persistent connection per remote node (full-duplex TCP)
- Backpressure via `writer.drain()` (asyncio native)
- Outbound buffer per connection with configurable overflow strategy

---

## Layer 3: Cluster Membership

### Data Structures

```python
class MemberStatus(Enum):
    joining = "joining"
    up = "up"
    leaving = "leaving"
    down = "down"
    removed = "removed"

@dataclass(frozen=True)
class NodeAddress:
    host: str
    port: int

@dataclass(frozen=True)
class Member:
    address: NodeAddress
    status: MemberStatus
    roles: frozenset[str]

@dataclass(frozen=True)
class VectorClock:
    versions: dict[NodeAddress, int]
    # merge, compare, increment operations

@dataclass(frozen=True)
class ClusterState:
    members: frozenset[Member]
    unreachable: frozenset[NodeAddress]
    version: VectorClock
```

### Phi Accrual Failure Detector

```python
@dataclass
class PhiAccrualFailureDetector:
    threshold: float = 8.0
    max_sample_size: int = 200
    min_std_deviation_ms: float = 100.0
    acceptable_heartbeat_pause_ms: float = 0.0
    first_heartbeat_estimate_ms: float = 1000.0

    def heartbeat(self, node: NodeAddress) -> None: ...
    def phi(self, node: NodeAddress) -> float: ...
    def is_available(self, node: NodeAddress) -> bool: ...
```

Uses sliding window of heartbeat intervals, normal distribution CDF via
`math.erf`, `statistics.mean/stdev`. Pure stdlib.

### GossipActor

Actor that runs the gossip protocol:

- Periodically (every ~1s) selects random peer, sends ClusterState
- On receiving gossip: merges via vector clock, updates local state
- After convergence: notifies leader logic
- Publishes membership events (MemberUp, MemberLeft, etc.) on EventStream

### HeartbeatActor

Actor that manages failure detection:

- Sends heartbeat to all known members every 1s
- Feeds responses into PhiAccrualFailureDetector
- Periodically checks phi for all members
- Reports unreachable nodes to GossipActor

### Leader Logic

Deterministic: node with lowest address among `Up` members. No election.

After gossip convergence, leader:
- Moves `Joining → Up`
- Moves `Leaving → Exiting → Removed`
- Triggers shard rebalancing

### Seed Nodes and Join

```python
@dataclass(frozen=True)
class ClusterConfig:
    host: str
    port: int
    seed_nodes: list[tuple[str, int]]
    roles: frozenset[str] = frozenset()
```

Join protocol:
1. New node sends JoinRequest to seed nodes
2. First seed that responds accepts the join
3. Gossip spreads `Joining` status
4. After convergence, leader moves to `Up`

### Split Brain Resolution

**Keep Majority** (default):
- Partition with >50% nodes survives
- <50% self-downs
- 50-50 tie: lowest address wins

Pluggable via Protocol for future strategies.

### Cluster Events (on EventStream)

```python
@dataclass(frozen=True)
class MemberUp:
    member: Member

@dataclass(frozen=True)
class MemberLeft:
    member: Member

@dataclass(frozen=True)
class UnreachableMember:
    member: Member

@dataclass(frozen=True)
class ReachableMember:
    member: Member
```

---

## Layer 4: Cluster Sharding

### User API

```python
@dataclass(frozen=True)
class ShardEnvelope[M]:
    entity_id: str
    message: M

shard_region: ActorRef[ShardEnvelope[UserCmd]] = cluster.init_sharding(
    type_key="User",
    entity_factory=user_entity,       # (entity_id: str) -> Behavior[M]
    num_shards=100,
    passivate_after=timedelta(minutes=5),  # optional
)

# Usage — transparent, location doesn't matter
shard_region.tell(ShardEnvelope("user-123", GetProfile(reply_to)))
```

### ShardCoordinator Actor (singleton on leader)

Maintains the global shard allocation map: `shard_id → NodeAddress`.

- Handles `GetShardLocation` requests from ShardRegions
- Runs allocation strategy on topology changes
- Initiates rebalancing via HandOff protocol

### ShardRegion Actor (on each participating node)

Routes messages to the correct node:

1. Extract `entity_id` and `shard_id` from ShardEnvelope
2. `shard_id = hash(entity_id) % num_shards`
3. Check local cache: `shard_id → NodeAddress`
4. If unknown: ask ShardCoordinator, buffer messages
5. If local: forward to local Shard actor
6. If remote: forward to remote ShardRegion

### Shard Actor (manages a group of entities)

Each shard is an actor that owns its entities:

- `entities: dict[str, ActorRef]`
- Creates entities on demand via `entity_factory(entity_id)`
- Tracks entity activity for passivation
- Handles HandOff: stops accepting messages, stops entities, confirms

### Allocation Strategy (pluggable)

```python
class ShardAllocationStrategy(Protocol):
    def allocate(
        self, shard_id: int, current_allocations: dict[int, NodeAddress]
    ) -> NodeAddress: ...

    def rebalance(
        self, current_allocations: dict[int, NodeAddress],
        members: frozenset[Member],
    ) -> dict[int, NodeAddress]: ...
```

**Default**: `LeastShardStrategy` — new shards go to node with fewest shards.

### Rebalance Protocol

```
ShardCoordinator decides: move Shard X from Node A → Node B
  1. Send HandOff(shard_id) to Node A's ShardRegion
  2. Node A stops accepting new messages for Shard X (buffers them)
  3. Node A stops all entities in Shard X
  4. Node A confirms ShardStopped(shard_id)
  5. ShardCoordinator updates map: Shard X → Node B
  6. Buffered messages are re-sent (now route to Node B)
  7. Node B creates entities on demand as messages arrive
```

### Passivation

Entities idle for longer than `passivate_after` are stopped. The Shard actor
tracks last message timestamp per entity and periodically sweeps.

When a message arrives for a passivated entity, it is recreated via
`entity_factory(entity_id)`.

---

## Future Extensions (not in v1, design must not preclude)

### Entity State Persistence

Hook for saving/loading entity state on passivation/activation:

```python
# Future API sketch — NOT implemented in v1
class EntityPersistence(Protocol):
    async def save(self, entity_id: str, state: Any) -> None: ...
    async def load(self, entity_id: str) -> Any | None: ...

cluster.init_sharding(
    ...,
    persistence=SomePersistenceImpl(),  # optional
)
```

The entity_factory already receives entity_id, so it can load state from
external storage. Passivation can be extended with a pre-stop hook.

### Consistency During Rebalance

The HandOff protocol with buffering minimizes message loss during rebalance.
For stronger guarantees (exactly-once, at-least-once), a future Reliable
Delivery layer can be added on top, similar to Akka's approach.

---

## Implementation Streams (for parallel execution)

Dependencies:

```
Stream A ──→ Stream B ──┐
                        ├──→ Stream E ──→ Stream F
Stream A ──→ Stream C ──┘
Stream D ────────────────┘
```

### Stream A: Addressable ActorRef (foundation, everything depends on this)

Files: `ref.py`, `_cell.py`, `system.py`, `context.py`

1. Create `ActorAddress` dataclass with URI parsing
2. Create `MessageTransport` Protocol
3. Create `LocalTransport` implementation
4. Refactor `ActorRef` from Callable to address + transport
5. Update `ActorCell` to generate addressed refs
6. Update `ActorSystem.resolve(address)` method
7. Update all existing tests

### Stream B: Serialization (after A, parallel with C and D)

Files: new `serialization.py`

1. `Serializer` Protocol
2. `TypeRegistry` for type name → class mapping
3. `JsonSerializer` default implementation
4. `ActorRef` serialization (address URI on wire, resolve on deserialize)
5. Tests for round-trip serialization

### Stream C: Transport (after A, parallel with B and D)

Files: new `transport.py`, new `_transport_actor.py`

1. `Transport` Protocol + `InboundHandler` Protocol
2. `MessageEnvelope` dataclass
3. `TcpTransport` implementation (asyncio streams)
4. Connection pool with reconnect + exponential backoff
5. `TransportActor` — wraps transport as an actor
6. `RemoteTransport` (MessageTransport impl that sends via TransportActor)
7. Tests with localhost TCP

### Stream D: Phi Accrual Failure Detector (independent, parallel with B and C)

Files: new `failure_detector.py`

1. `PhiAccrualFailureDetector` class (pure math, no deps)
2. Sliding window heartbeat history
3. Normal distribution CDF via math.erf
4. Tests with simulated heartbeat patterns

### Stream E: Cluster Membership (after B + C + D)

Files: new `cluster.py`, new `_gossip_actor.py`, new `_heartbeat_actor.py`

1. `ClusterState`, `Member`, `VectorClock`, `MemberStatus`
2. `GossipActor` — gossip protocol, state merge, convergence
3. `HeartbeatActor` — heartbeat send/receive, feeds failure detector
4. Leader logic (deterministic, no election)
5. Seed node join protocol
6. `SplitBrainResolver` Protocol + `KeepMajority` default
7. Cluster events on EventStream
8. `ClusterConfig` dataclass
9. Integration tests with multiple ActorSystems on localhost

### Stream F: Cluster Sharding (after E)

Files: new `sharding.py`, new `_shard_region_actor.py`,
       new `_shard_coordinator_actor.py`, new `_shard_actor.py`

1. `ShardEnvelope` message type
2. `ShardAllocationStrategy` Protocol + `LeastShardStrategy`
3. `ShardCoordinatorActor` — singleton on leader, manages shard map
4. `ShardRegionActor` — per-node router, caches shard locations
5. `ShardActor` — manages entity group, passivation
6. `cluster.init_sharding()` API
7. HandOff rebalance protocol
8. Entity passivation with configurable timeout
9. Integration tests with multi-node sharding
