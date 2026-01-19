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

Casty brings the Actor Model to Python with a clean, modern API. You define actors as classes, messages as dataclasses, and let Casty handle concurrency, message routing, and fault tolerance. The same code runs locally or distributed across a cluster — no rewrites needed.

Out of the box you get: type-safe message passing, supervision trees with automatic restarts, behavior switching for state machines, persistent actors with write-ahead logging, and a distributed runtime with consistent hashing, failure detection (SWIM), and state replication. All with a single dependency (msgpack) and full async/await support.

```bash
pip install casty
# or
uv add casty
```

---

## Why Actors?

Concurrent programming is hard. Shared state between threads leads to locks, race conditions, and deadlocks. The Actor Model solves this with a simple rule:

**Actors don't share memory. They communicate through messages.**

Each actor:
- Has private state that no one can access directly
- Processes one message at a time, sequentially
- Can create other actors and send messages

This eliminates entire categories of bugs. No locks, no race conditions on shared state. When you need coordination, you send a message and wait for a response.

The model was created in 1973, battle-tested in Erlang (telecom) and Akka (JVM). Casty brings it to Python with type safety and a modern API.

---

## Your First Actor

You work at an e-commerce company. Your task is to build a shopping cart that can add items, remove items, and calculate totals. With Casty, you model this as an actor that receives messages and maintains its own state.

First, define the messages your cart will understand. Each message is a simple dataclass describing an intent:

```python
from dataclasses import dataclass

@dataclass
class AddItem:
    sku: str
    price: float

@dataclass
class RemoveItem:
    sku: str

@dataclass
class GetTotal:
    pass

@dataclass
class Checkout:
    pass
```

Now the actor itself. It extends `Actor[M]` where `M` is the union of all message types it handles. The `receive` method is called for every incoming message, and pattern matching makes it easy to handle each case:

```python
from casty import Actor, Context

class ShoppingCart(Actor[AddItem | RemoveItem | GetTotal | Checkout]):
    def __init__(self):
        self.items: dict[str, float] = {}

    async def receive(self, msg, ctx: Context):
        match msg:
            case AddItem(sku, price):
                self.items[sku] = price
            case RemoveItem(sku):
                self.items.pop(sku, None)
            case GetTotal():
                await ctx.reply(sum(self.items.values()))
            case Checkout():
                total = sum(self.items.values())
                self.items.clear()
                await ctx.reply(total)
```

The state lives inside the actor. No other code can access `self.items` directly — all interaction happens through messages.

---

## Sending Messages

Your cart is ready. Now you need to use it. There are two ways to communicate with an actor: fire-and-forget with `send()`, and request-response with `ask()`.

When a customer adds an item to their cart, you don't need to wait for confirmation — you just send the message and move on:

```python
async with ActorSystem() as system:
    cart = await system.actor(ShoppingCart, name="cart")

    await cart.send(AddItem("SKU-123", 29.99))
    await cart.send(AddItem("SKU-456", 49.99))
```

But when it's time to display the total, you need to wait for an answer. That's what `ask()` does — it sends a message and waits for the actor to call `ctx.reply()`:

```python
    total = await cart.ask(GetTotal())
    print(f"Cart total: ${total:.2f}")  # Cart total: $79.98
```

The difference matters. `send()` is fast and non-blocking — the actor will process it eventually. `ask()` blocks until you get a response, which means you need to think about timeouts.

What if the actor is overloaded? What if something goes wrong? By default, `ask()` waits 5 seconds before raising a `TimeoutError`. You can adjust this:

```python
    total = await cart.ask(GetTotal(), timeout=10.0)
```

A good rule of thumb: use `send()` for commands and events, use `ask()` for queries where you need the result.

<details>
<summary><b>Calling ask() from inside an actor</b></summary>

When you call `ask()` from inside an actor's `receive()` method, that actor is blocked until the response arrives. No other messages will be processed while waiting.

This is usually fine for quick operations, but be careful with long-running asks or chains of asks between actors — you can create deadlocks if actor A is waiting for actor B while actor B is waiting for actor A.

For long-running operations, consider using `send()` with a callback message instead:

```python
async def receive(self, msg, ctx: Context):
    match msg:
        case ProcessOrder(order_id):
            # Instead of: result = await payment.ask(Charge(...))
            # Do this:
            await payment.send(Charge(order_id, ctx.self_ref))

        case ChargeComplete(order_id, success):
            # Handle the response asynchronously
            ...
```

</details>

---

## The Actor Lifecycle

Your cart works, but you realize you need to do some setup when it starts — maybe load saved items from a database — and cleanup when it stops.

Actors have a lifecycle. When you spawn an actor, Casty calls `on_start()` before it processes any messages. When the actor stops, `on_stop()` is called for cleanup:

```python
class ShoppingCart(Actor[AddItem | RemoveItem | GetTotal | Checkout]):
    def __init__(self, user_id: str):
        self.user_id = user_id
        self.items: dict[str, float] = {}

    async def on_start(self):
        # Load any saved cart items from database
        saved = await db.get_cart(self.user_id)
        if saved:
            self.items = saved

    async def on_stop(self):
        # Persist cart before shutdown
        await db.save_cart(self.user_id, self.items)

    async def receive(self, msg, ctx: Context):
        # ... same as before
```

The `Context` object passed to `receive()` is your gateway to the actor's environment. You've already seen `ctx.reply()` for responding to `ask()` calls. But it can do more:

```python
async def receive(self, msg, ctx: Context):
    match msg:
        case AddItem(sku, price):
            self.items[sku] = price

            # Get a reference to yourself
            my_ref = ctx.self_ref

            # Schedule a message for later
            await ctx.schedule(300.0, CartExpired())
```

---

## When Things Go Wrong

Your store is live. Customers are adding items, checking out, everything works. Then one day, the payment gateway times out and your cart actor crashes mid-checkout.

In traditional code, you'd wrap everything in try/except and hope you caught every edge case. The actor model takes a different approach: let it crash, and have someone else decide what to do.

This is supervision. Every actor can have a supervisor that watches it and reacts when it fails. The default behavior is to restart the actor:

```python
from casty import supervised, SupervisionStrategy

@supervised(
    strategy=SupervisionStrategy.RESTART,
    max_restarts=3,
    within_seconds=60.0,
)
class ShoppingCart(Actor[AddItem | RemoveItem | GetTotal | Checkout]):
    # ... same implementation
```

Now if your cart crashes, Casty will restart it automatically. If it crashes 3 times within 60 seconds, something is seriously wrong and the supervisor gives up.

You can also choose to stop permanently or escalate to a parent supervisor:

```python
@supervised(strategy=SupervisionStrategy.STOP)      # Give up immediately
@supervised(strategy=SupervisionStrategy.ESCALATE)  # Let parent decide
```

<details>
<summary><b>Restart hooks and backoff</b></summary>

When an actor restarts, you might need to save state or reinitialize connections. Override `pre_restart()` and `post_restart()`:

```python
async def pre_restart(self, exc: Exception, msg):
    # Called on the OLD instance before it dies
    await self.save_state_somewhere()

async def post_restart(self, exc: Exception):
    # Called on the NEW instance after creation
    await self.restore_state()
```

To prevent rapid restart loops, configure exponential backoff:

```python
@supervised(
    strategy=SupervisionStrategy.RESTART,
    max_restarts=5,
    within_seconds=60.0,
    backoff_initial=0.1,      # Start with 100ms
    backoff_max=30.0,         # Cap at 30 seconds
    backoff_multiplier=2.0,   # Double each time
)
```

</details>

---

## Don't Lose My Cart

Your supervision is working — crashed carts restart automatically. But there's a problem: when the cart restarts, it's empty. The customer had 5 items in their cart, and now they're gone.

You need persistence. Casty provides a write-ahead log (WAL) that records state changes. When an actor restarts, it recovers its state from the log:

```python
from casty import ActorSystem

async with ActorSystem() as system:
    cart = await system.actor(
        ShoppingCart,
        name="cart-user-123",
        durable=True,
    )

    await cart.send(AddItem("SKU-123", 29.99))
    # State is now persisted
```

If the process crashes and restarts, the cart will have its items back.

By default, Casty persists all public attributes (those not starting with `_`). If you have fields that shouldn't be persisted — like cached computations or temporary state — you can exclude them:

```python
class ShoppingCart(Actor[...]):
    __casty_exclude_fields__ = {"_cache", "_temp"}

    def __init__(self, user_id: str):
        self.user_id = user_id
        self.items: dict[str, float] = {}  # Persisted
        self._cache = {}                    # Not persisted
```

<details>
<summary><b>Custom serialization</b></summary>

For complex objects that don't serialize automatically, override `get_state()` and `set_state()`:

```python
from datetime import datetime

class ShoppingCart(Actor[...]):
    def __init__(self, user_id: str):
        self.user_id = user_id
        self.items: dict[str, float] = {}
        self.created_at = datetime.now()

    def get_state(self) -> dict:
        return {
            "user_id": self.user_id,
            "items": self.items,
            "created_at": self.created_at.isoformat(),
        }

    def set_state(self, state: dict):
        self.user_id = state["user_id"]
        self.items = state["items"]
        self.created_at = datetime.fromisoformat(state["created_at"])
```

</details>

---

## Clustered Actors

Your store grew. One server isn't enough anymore. You need to run your application across multiple machines, but you don't want to rewrite everything.

Casty makes this transition simple. Replace `ActorSystem()` with `ActorSystem.clustered()` and your actors can now live on any node:

```python
async with ActorSystem.clustered(
    host="0.0.0.0",
    port=8001,
    seeds=["node2:8001", "node3:8001"],
) as system:

    # This cart lives on this node only
    local_cart = await system.actor(ShoppingCart, name="cart-123")

    # This cart is registered in the cluster
    clustered_cart = await system.actor(
        ShoppingCart,
        name="cart-456",
        scope="cluster",
    )
```

From the outside, both refs work exactly the same way. You call `send()` and `ask()` as before. Casty handles routing — if the actor is remote, the message is serialized and sent over the network transparently.

Nodes find each other through seed nodes. Once connected, they share membership information via gossip, so every node knows about every other node:

```python
# Node 1 — first node, no seeds needed
system1 = ActorSystem.clustered("0.0.0.0", 8001)

# Node 2 — joins via Node 1
system2 = ActorSystem.clustered("0.0.0.0", 8002, seeds=["node1:8001"])

# Node 3 — can use any existing node as seed
system3 = ActorSystem.clustered("0.0.0.0", 8003, seeds=["node2:8002"])
```

Your code doesn't change. The same `ShoppingCart` class runs locally or in a cluster.

---

## Sharding Actors

You have millions of customers, each with their own cart. You need to distribute them across nodes so no single machine holds everything.

Casty provides `HashRing` for consistent hashing. You create shard actors on each node and a router that directs messages to the right shard:

```python
from casty import Actor, Context, LocalActorRef
from casty.cluster import DevelopmentCluster, HashRing


@dataclass
class AddItem:
    user_id: str
    sku: str
    price: float


@dataclass
class GetTotal:
    user_id: str


class CartShard(Actor[AddItem | GetTotal]):
    def __init__(self, shard_id: str):
        self.shard_id = shard_id
        self.carts: dict[str, dict[str, float]] = {}

    async def receive(self, msg, ctx: Context):
        match msg:
            case AddItem(user_id, sku, price):
                if user_id not in self.carts:
                    self.carts[user_id] = {}
                self.carts[user_id][sku] = price
            case GetTotal(user_id):
                cart = self.carts.get(user_id, {})
                await ctx.reply(sum(cart.values()))


class CartRouter(Actor[AddItem | GetTotal]):
    def __init__(self, shards: dict[str, LocalActorRef]):
        self.shards = shards
        self.ring = HashRing(virtual_nodes=100)
        for shard_id in shards:
            self.ring.add_node(shard_id)

    def _get_shard(self, user_id: str) -> LocalActorRef:
        shard_id = self.ring.get_node(user_id)
        return self.shards[shard_id]

    async def receive(self, msg, ctx: Context):
        match msg:
            case AddItem(user_id, _, _):
                await self._get_shard(user_id).send(msg)
            case GetTotal(user_id):
                result = await self._get_shard(user_id).ask(msg)
                await ctx.reply(result)
```

The `HashRing` ensures that the same user ID always routes to the same shard. When nodes join or leave, only ~1/N of the keys need to move.

<details>
<summary><b>Setting up a sharded cluster</b></summary>

```python
async with DevelopmentCluster(3) as (node0, node1, node2):
    # Create one shard per node
    shard0 = await node0.actor(CartShard, name="shard-0", shard_id="shard-0")
    shard1 = await node1.actor(CartShard, name="shard-1", shard_id="shard-1")
    shard2 = await node2.actor(CartShard, name="shard-2", shard_id="shard-2")

    shards = {"shard-0": shard0, "shard-1": shard1, "shard-2": shard2}
    router = await node0.actor(CartRouter, name="router", shards=shards)

    # Use the router — it handles distribution
    await router.send(AddItem("user-alice", "SKU-1", 29.99))
    await router.send(AddItem("user-bob", "SKU-2", 49.99))

    alice_total = await router.ask(GetTotal("user-alice"))
```

</details>

---

## State Replication

Your sharded carts are distributed, but what happens when a node crashes? The carts on that node are gone. You need replicas.

When you create a clustered actor, you can configure how many copies exist and how writes are acknowledged using `ClusterScope`:

```python
from casty.cluster import ClusterScope

async with ActorSystem.clustered(
    host="0.0.0.0",
    port=8001,
    seeds=["node2:8001", "node3:8001"],
) as system:

    cart = await system.actor(
        ShoppingCart,
        name="cart-123",
        scope=ClusterScope(
            replication=3,        # Keep 3 copies across nodes
            consistency='quorum', # Wait for majority to confirm
        ),
    )
```

Consistency levels control the trade-off between safety and speed:

| Level | Behavior | Use case |
|-------|----------|----------|
| `'one'` | Return after one node confirms | High throughput, eventual consistency |
| `'quorum'` | Wait for majority (2 of 3) | Balance of safety and speed |
| `'all'` | Wait for all replicas | Critical data, highest latency |

After each message, the actor's state is automatically serialized and sent to replica nodes. If the primary fails, a replica takes over with the latest state.

<details>
<summary><b>How replication works under the hood</b></summary>

Casty uses a primary-backup model:

1. Messages are routed to the primary node (determined by consistent hashing)
2. The primary processes the message and updates its state
3. State changes are sent to backup nodes asynchronously or synchronously
4. With `'quorum'`, the primary waits for N/2+1 acknowledgments before responding

Vector clocks track causality. When replicas have conflicting states (network partitions, concurrent updates), Casty uses last-writer-wins by default. For custom merge logic, implement the `Mergeable` protocol.

</details>

---

## Behavior Switching

Your cart has different states: active, checked out, expired. Instead of tracking state with flags and conditionals, you can switch the actor's behavior entirely.

```python
@dataclass
class AddItem:
    sku: str
    price: float

@dataclass
class Checkout:
    pass

@dataclass
class Reopen:
    pass

class ShoppingCart(Actor[AddItem | Checkout | Reopen]):
    def __init__(self):
        self.items: dict[str, float] = {}

    async def receive(self, msg, ctx: Context):
        # Active state — accepts items
        match msg:
            case AddItem(sku, price):
                self.items[sku] = price
            case Checkout():
                total = sum(self.items.values())
                ctx.become(self.checked_out)
                await ctx.reply(total)

    async def checked_out(self, msg, ctx: Context):
        # Checked out — only allows reopening
        match msg:
            case AddItem(_, _):
                await ctx.reply("Cart is closed")
            case Reopen():
                ctx.unbecome()  # Back to receive()
```

When you call `ctx.become(handler)`, all future messages go to that handler instead of `receive()`. Call `ctx.unbecome()` to revert to the previous behavior.

Behaviors stack by default. If you call `become()` twice, `unbecome()` pops one level. To replace instead of stack, use `ctx.become(handler, discard_old=True)`.

---

## Spawning Child Actors

Your cart needs to validate items against inventory before adding them. Instead of calling an external service directly, you spawn a child actor that handles validation:

```python
@dataclass
class ValidateItem:
    sku: str
    reply_to: LocalRef

@dataclass
class ItemValid:
    sku: str
    price: float

@dataclass
class ItemInvalid:
    sku: str
    reason: str

class InventoryValidator(Actor[ValidateItem]):
    async def receive(self, msg: ValidateItem, ctx: Context):
        # Check inventory (simplified)
        if msg.sku.startswith("SKU-"):
            await msg.reply_to.send(ItemValid(msg.sku, 29.99))
        else:
            await msg.reply_to.send(ItemInvalid(msg.sku, "Unknown SKU"))

class ShoppingCart(Actor[AddItem | ItemValid | ItemInvalid]):
    def __init__(self):
        self.items: dict[str, float] = {}
        self.validator: LocalRef | None = None

    async def on_start(self):
        self.validator = await self._ctx.actor(InventoryValidator, name="validator")

    async def receive(self, msg, ctx: Context):
        match msg:
            case AddItem(sku, _):
                await self.validator.send(ValidateItem(sku, ctx.self_ref))
            case ItemValid(sku, price):
                self.items[sku] = price
            case ItemInvalid(sku, reason):
                print(f"Cannot add {sku}: {reason}")
```

Children created via `ctx.actor()` are supervised by their parent. When the parent stops, all children stop automatically.

<details>
<summary><b>Child supervision</b></summary>

When a child crashes, the parent decides what happens based on its supervision strategy. By default, children are restarted. You can override this per-child:

```python
async def on_start(self):
    self.validator = await self._ctx.actor(
        InventoryValidator,
        name="validator",
        supervision=SupervisorConfig(
            strategy=SupervisionStrategy.RESTART,
            max_restarts=5,
        ),
    )
```

For coordinated restarts, use multi-child strategies:

```python
from casty import MultiChildStrategy

@supervised(multi_child=MultiChildStrategy.ONE_FOR_ALL)
class ShoppingCart(Actor[...]):
    # If any child fails, restart ALL children together
    ...
```

- `ONE_FOR_ONE`: Only restart the failed child (default)
- `ONE_FOR_ALL`: Restart all children when one fails
- `REST_FOR_ONE`: Restart the failed child and all children spawned after it

</details>

---

## Combining Actor References

You have separate actors for logging and metrics. Instead of keeping track of both references, you can combine them:

```python
@dataclass
class LogMessage:
    text: str

@dataclass
class MetricEvent:
    name: str
    value: float

class Logger(Actor[LogMessage]):
    async def receive(self, msg: LogMessage, ctx: Context):
        print(f"[LOG] {msg.text}")

class Metrics(Actor[MetricEvent]):
    async def receive(self, msg: MetricEvent, ctx: Context):
        print(f"[METRIC] {msg.name}={msg.value}")

async with ActorSystem() as system:
    logger = await system.actor(Logger, name="logger")
    metrics = await system.actor(Metrics, name="metrics")

    # Combine with | operator
    observer = logger | metrics

    # Messages route automatically by type
    await observer.send(LogMessage("User logged in"))
    await observer.send(MetricEvent("logins", 1))
```

The combined reference inspects each message's type and routes it to the actor that handles that type. You can combine as many refs as you need — they flatten automatically.

---

## Scheduled & Periodic Messages

Your cart should expire after 30 minutes of inactivity. Instead of running a background timer, schedule a message to yourself:

```python
@dataclass
class CartExpired:
    pass

class ShoppingCart(Actor[AddItem | GetTotal | CartExpired]):
    def __init__(self):
        self.items: dict[str, float] = {}
        self.expiry_task: str | None = None

    async def receive(self, msg, ctx: Context):
        match msg:
            case AddItem(sku, price):
                self.items[sku] = price
                # Reset expiry timer on activity
                if self.expiry_task:
                    await ctx.cancel_schedule(self.expiry_task)
                self.expiry_task = await ctx.schedule(
                    timeout=1800.0,  # 30 minutes
                    msg=CartExpired(),
                )
            case CartExpired():
                self.items.clear()
                print("Cart expired due to inactivity")
```

For recurring messages, use `ctx.tick()`:

```python
@dataclass
class SyncInventory:
    pass

class InventoryActor(Actor[SyncInventory]):
    def __init__(self):
        self.sync_sub: str | None = None

    async def on_start(self):
        # Sync every 60 seconds
        self.sync_sub = await self._ctx.tick(SyncInventory(), interval=60.0)

    async def on_stop(self):
        if self.sync_sub:
            await self._ctx.cancel_tick(self.sync_sub)

    async def receive(self, msg: SyncInventory, ctx: Context):
        await self.fetch_latest_inventory()
```

---

## The @on Decorator

Pattern matching in `receive()` works well, but for actors with many message types, the `@on` decorator offers a cleaner alternative:

```python
from casty import Actor, Context, on

class ShoppingCart(Actor[AddItem | RemoveItem | GetTotal | Checkout]):
    def __init__(self):
        self.items: dict[str, float] = {}

    @on(AddItem)
    async def handle_add(self, msg: AddItem, ctx: Context):
        self.items[msg.sku] = msg.price

    @on(RemoveItem)
    async def handle_remove(self, msg: RemoveItem, ctx: Context):
        self.items.pop(msg.sku, None)

    @on(GetTotal)
    async def handle_total(self, msg: GetTotal, ctx: Context):
        await ctx.reply(sum(self.items.values()))

    @on(Checkout)
    async def handle_checkout(self, msg: Checkout, ctx: Context):
        total = sum(self.items.values())
        self.items.clear()
        await ctx.reply(total)
```

Each handler is a separate method, named however you like. Casty dispatches incoming messages to the right handler based on their type.

You can mix both styles — use `receive()` as a fallback for messages that don't match any `@on` handler.

---

## Under the Hood

Casty's distributed layer is built on three proven protocols. You don't need to understand them to use Casty, but knowing how they work helps when debugging or tuning for production.

<details>
<summary><b>SWIM: Failure Detection</b></summary>

SWIM (Scalable Weakly-consistent Infection-style Membership) detects when nodes fail. Each node periodically probes a random peer:

1. Node A sends `Ping` to Node B
2. If B responds with `Ack` within timeout, B is alive
3. If no response, A asks K other nodes to probe B (indirect probe)
4. If still no response, B is marked as suspected
5. After a suspicion timeout, B is declared dead

This scales O(1) per node — each node does constant work regardless of cluster size. Failures are detected in O(log N) protocol periods.

```python
from casty.cluster import ClusterConfig

config = ClusterConfig(
    protocol_period=1.0,    # Probe interval
    ping_timeout=0.5,       # Direct probe timeout
    ping_req_fanout=3,      # Nodes for indirect probe
    suspicion_mult=5,       # Suspicion timeout multiplier
)
```

</details>

<details>
<summary><b>Gossip: State Dissemination</b></summary>

Gossip spreads information epidemically. When a node learns something new (actor registered, node failed), it tells random peers. They tell other peers. Information spreads exponentially.

Casty uses gossip for:
- Actor registry (which actors exist and where)
- Membership changes (joins, leaves, failures)
- State updates for replicated actors

Vector clocks track causality. Each update carries a logical timestamp so nodes can order events correctly even with network delays.

</details>

<details>
<summary><b>Consistent Hashing: Routing</b></summary>

Consistent hashing maps keys to nodes with minimal disruption when nodes join or leave. Imagine a ring of numbers from 0 to 2^32:

1. Each node is placed at multiple points on the ring (virtual nodes)
2. To find a key's owner, hash the key and walk clockwise to the first node
3. Adding a node only moves keys between its neighbors
4. Removing a node only moves its keys to the next node

With N nodes, adding or removing one moves only ~1/N of the keys.

```python
from casty.cluster import HashRing

ring = HashRing(virtual_nodes=150)
ring.add_node("node-1")
ring.add_node("node-2")
ring.add_node("node-3")

owner = ring.get_node("user-alice")  # Deterministic
```

</details>

---

## Quick Reference

### ActorSystem

```python
async with ActorSystem() as system:
    ref = await system.actor(MyActor, name="my-actor")             # Create actor
    ref = await system.actor(MyActor, name="my-actor")             # Get-or-create (same name = same actor)
    ref = await system.actor(MyActor, name="my-actor", durable=True)  # With persistence

    await system.shutdown()                                         # Graceful stop
```

### ActorSystem.clustered()

```python
from casty.cluster import ClusterScope

async with ActorSystem.clustered(
    host="0.0.0.0",
    port=8001,
    seeds=["node2:8001"],
) as system:
    ref = await system.actor(MyActor, name="actor", scope="cluster")
    ref = await system.actor(MyActor, name="actor", scope=ClusterScope(replication=3))
    ref = await system.actor(MyActor, name="actor", scope=ClusterScope(replication=3, consistency='quorum'))
```

### LocalRef

```python
await ref.send(msg)                    # Fire-and-forget
result = await ref.ask(msg)            # Request-response (5s timeout)
result = await ref.ask(msg, timeout=10.0)

combined = ref1 | ref2                 # Combine by type
```

### Context

```python
async def receive(self, msg, ctx: Context):
    await ctx.reply(result)            # Respond to ask()

    child = await ctx.actor(ChildActor, name="child")  # Create child actor
    await ctx.stop_child(child)

    ctx.become(self.other_behavior)
    ctx.unbecome()

    task_id = await ctx.schedule(10.0, LaterMsg())
    await ctx.cancel_schedule(task_id)

    sub_id = await ctx.tick(PeriodicMsg(), interval=1.0)
    await ctx.cancel_tick(sub_id)

    ctx.self_ref                       # Reference to self
    ctx.system                         # The ActorSystem
```

### Supervision

```python
from casty import supervised, SupervisionStrategy, MultiChildStrategy

@supervised(
    strategy=SupervisionStrategy.RESTART,  # RESTART | STOP | ESCALATE
    max_restarts=3,
    within_seconds=60.0,
    backoff_initial=0.1,
    backoff_max=30.0,
    backoff_multiplier=2.0,
    multi_child=MultiChildStrategy.ONE_FOR_ONE,  # ONE_FOR_ALL | REST_FOR_ONE
)
class MyActor(Actor[Msg]):
    ...
```

### Lifecycle Hooks

```python
class MyActor(Actor[Msg]):
    async def on_start(self):
        # Called before first message
        # Use self._ctx to access context
        pass

    async def on_stop(self):
        # Called on shutdown
        pass

    async def pre_restart(self, exc: Exception, msg):
        # Called on old instance before restart
        pass

    async def post_restart(self, exc: Exception):
        # Called on new instance after restart
        pass
```

---

## Installation

```bash
pip install casty
```

Or with uv:

```bash
uv add casty
```

Casty requires Python 3.12+ for generic syntax (`class Actor[M]`).

For better async performance, install uvloop (automatically used if available):

```bash
pip install uvloop
```

---

## Testing with DevelopmentCluster

For integration tests, `DevelopmentCluster` spins up a local multi-node cluster:

```python
from casty.cluster import DevelopmentCluster

async def test_distributed_cart():
    async with DevelopmentCluster(3) as (node0, node1, node2):
        cart = await node0.actor(ShoppingCart, name="cart", scope="cluster")
        await cart.send(AddItem("SKU-1", 29.99))

        # Actor is accessible from any node
        total = await cart.ask(GetTotal())
        assert total == 29.99
```

---

## Contributing

Contributions are welcome. See the [GitHub repository](https://github.com/gabfssilva/casty) for issues and pull requests.

```bash
git clone https://github.com/gabfssilva/casty
cd casty
uv sync
uv run pytest -n 16
```

---

## License

MIT License — see LICENSE file for details.

---

## Acknowledgments

Casty is inspired by:

- **Erlang/OTP**: The original actor model implementation with supervision trees
- **Akka**: Bringing actors to the JVM with a clean API
- **Microsoft Orleans**: Virtual actors and transparent distribution
- **Proto.Actor**: Modern actor framework design principles
