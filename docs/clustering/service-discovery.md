# Service Discovery

In a cluster, knowing which **nodes** exist isn't enough. You need to know which **services** are running and where. A payment processor on node-2, three chat handlers spread across the cluster, a monitoring agent that just came online — without discovery, every caller needs hardcoded paths and node addresses.

The **receptionist** solves this. Actors register themselves under typed `ServiceKey`s, and other actors discover them via `Find` (one-shot) or `Subscribe` (continuous). The registry propagates through the cluster's topology actor — no extra round-trips, no external service registry.

```python
PAYMENT_KEY: ServiceKey[PaymentMsg] = ServiceKey("payment")

async with ClusteredActorSystem(...) as system:
    ref = system.spawn(
        Behaviors.discoverable(payment_actor(), key=PAYMENT_KEY),
        "payment",
    )

    listing = await system.lookup(PAYMENT_KEY)
    for instance in listing.instances:
        instance.ref.tell(ProcessPayment(amount=100))
```

## Behaviors.discoverable()

The most common pattern — spawn an actor and register it — is a single call with `Behaviors.discoverable()`:

```python
ref = system.spawn(
    Behaviors.discoverable(my_behavior, key=MY_KEY),
    "my-service",
)
```

This is equivalent to:

```python
ref = system.spawn(my_behavior, "my-service")
system.receptionist.tell(Register(key=MY_KEY, ref=ref))
```

`discoverable()` composes with other wrappers:

```python
ref = system.spawn(
    Behaviors.discoverable(
        Behaviors.supervise(my_behavior, OneForOneStrategy()),
        key=MY_KEY,
    ),
    "supervised-service",
)
```

Deregistration is automatic — when the actor stops, the receptionist removes it from the registry and notifies all subscribers.

## Register and Find

For cases where you need manual control — registering an existing actor, or registering via `ctx.spawn()` — use `Register` directly. `Find` performs a one-shot query that returns the current `Listing` — a frozen set of all known instances across all nodes.

```python
@dataclass(frozen=True)
class Ping:
    reply_to: ActorRef[str]

PING_KEY: ServiceKey[Ping] = ServiceKey("ping")

async with ClusteredActorSystem(...) as system:
    ref = system.spawn(ping_actor(), "ping-service")
    system.receptionist.tell(Register(key=PING_KEY, ref=ref))

    listing: Listing[Ping] = await system.ask(
        system.receptionist,
        lambda r: Find(key=PING_KEY, reply_to=r),
        timeout=3.0,
    )

    for instance in listing.instances:
        reply = await system.ask(
            instance.ref,
            lambda r: Ping(reply_to=r),
            timeout=2.0,
        )
```

`system.lookup(ServiceKey(...))` is a convenience that wraps `Find` + `ask` in a single call.

## Subscribe for Continuous Updates

`Find` gives you a snapshot. `Subscribe` gives you a live stream — the subscriber immediately receives the current `Listing`, then gets notified every time an instance is added or removed.

```python
def presence_monitor() -> Behavior[Listing[ChatMsg]]:
    def active(prev: frozenset[str]) -> Behavior[Listing[ChatMsg]]:
        async def receive(
            ctx: ActorContext[Listing[ChatMsg]], msg: Listing[ChatMsg],
        ) -> Behavior[Listing[ChatMsg]]:
            current = frozenset(i.ref.address.path for i in msg.instances)
            for path in current - prev:
                print(f"joined: {path}")
            for path in prev - current:
                print(f"left: {path}")
            return active(current)

        return Behaviors.receive(receive)

    return active(frozenset())

monitor = system.spawn(presence_monitor(), "monitor")
system.receptionist.tell(Subscribe(key=CHAT_KEY, reply_to=monitor))
```

The monitor reacts to changes — no polling, no timers. When a user actor registers on any node in the cluster, the monitor receives an updated `Listing` within one topology push cycle.

## Auto-Deregister on Stop

When a registered actor stops, it is automatically deregistered. The receptionist subscribes to `ActorStopped` events on the EventStream and removes the entry. Subscribers are notified with an updated `Listing` that no longer includes the stopped actor.

```python
ref = system.spawn(
    Behaviors.discoverable(user_actor("Alice"), key=USER_KEY),
    "alice",
)

ref.tell(Leave())
```

No explicit `Deregister` needed — stopping the actor is enough. `Deregister` exists for cases where you want to remove an actor from the registry while keeping it alive.

## How It Works

The receptionist is a regular actor spawned by `ClusteredActorSystem` at startup, accessible via `system.receptionist`. It maintains two sets of entries:

- **Local entries** — actors registered on this node via `Register`.
- **Cluster entries** — actors on remote nodes, received via gossip.

When a `Register` arrives, the receptionist adds a `ServiceEntry` to its local set and forwards it to the topology actor for cluster-wide propagation. The topology actor includes registry entries in its gossip rounds, and remote nodes receive them through normal CRDT merge.

The registry is part of `TopologySnapshot`. `ClusterState` carries a `registry: frozenset[ServiceEntry]` field that merges with the same CRDT rules as membership — union of entries, pruning of entries from `down` nodes. The receptionist subscribes to topology updates and refreshes its remote entries whenever a new snapshot arrives. No additional network messages, no extra protocol.

## Cross-Node Discovery

A service registered on node A becomes discoverable from node B after gossip convergence:

```python
# Node A
ref = system_a.spawn(
    Behaviors.discoverable(echo_actor(), key=ECHO_KEY),
    "echo",
)

# Node B (after topology propagation)
listing = await system_b.lookup(ECHO_KEY)
for instance in listing.instances:
    instance.ref.tell(Echo("hello"))
```

The `ActorRef` in the `Listing` is a remote ref — `tell()` transparently serializes the message and sends it over TCP. No special handling needed by the caller.

## Discovery from ClusterClient

External processes that don't join the cluster can still discover services via `ClusterClient.lookup()`. The topology snapshot pushed to the client includes the service registry, so lookups are local reads with no extra round-trip:

```python
async with ClusterClient(
    contact_points=[("10.0.1.10", 25520)],
    system_name="my-cluster",
) as client:
    listing = client.lookup(ECHO_KEY)
    for instance in listing.instances:
        instance.ref.tell(Echo("hello from outside"))
```

See [Cluster Client](cluster-client.md) for the full API.

---

**Next:** [Shard Replication](shard-replication.md)
