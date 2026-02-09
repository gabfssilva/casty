# Distributed Data Structures

For common patterns that don't require custom entity actors, Casty provides higher-level data structures and coordination primitives built on top of cluster sharding. Each structure is backed by sharded actors, optionally persistent via event sourcing.

Every concept from the previous sections — actors, functional state, event sourcing, cluster sharding — converges here. A `Counter`, for example, is a sharded actor with a predefined message protocol, distributed transparently across the cluster.

```python
import asyncio
from dataclasses import dataclass
from casty.sharding import ClusteredActorSystem
from casty.journal import InMemoryJournal

@dataclass(frozen=True)
class User:
    name: str
    email: str

async def main() -> None:
    async with ClusteredActorSystem(
        name="my-app", host="127.0.0.1", port=25520,
        node_id="node-1", seed_nodes=[("127.0.0.1", 25521)],
    ) as system:
        d = system.distributed()

        # Counter
        views = d.counter("page-views", shards=50)
        await views.increment(100)
        await views.decrement(10)
        value = await views.get()        # 90

        # Dict — one sharded entity per key
        users = d.map[str, User]("users", shards=50)
        await users.put("alice", User("Alice", "alice@example.com"))
        user = await users.get("alice")  # User(name='Alice', email='alice@example.com')
        await users.contains("alice")    # True
        await users.delete("alice")      # True (existed)

        # Set
        tags = d.set[str]("active-tags", shards=50)
        await tags.add("python")         # True (added)
        await tags.add("python")         # False (already present)
        await tags.size()                # 1

        # Queue (FIFO)
        jobs = d.queue[str]("work-queue", shards=50)
        await jobs.enqueue("task-1")
        await jobs.peek()                # "task-1" (does not remove)
        await jobs.dequeue()             # "task-1" (removes)
        await jobs.dequeue()             # None (empty)

        # Lock (distributed mutex)
        lock = d.lock("deploy-lock", shards=10)
        await lock.acquire()             # blocks until granted
        # ... critical section ...
        await lock.release()             # True (released)

        # Lock — non-blocking try
        got = await lock.try_acquire()   # True if free, False if held

        # Semaphore (bounded concurrency)
        sem = d.semaphore("db-pool", permits=5, shards=10)
        await sem.acquire()              # blocks until a permit is available
        # ... use connection ...
        await sem.release()              # True (released)

        # Barrier (distributed rendezvous)
        barrier = d.barrier("phase-1")
        await barrier.arrive(expected=3) # blocks until 3 nodes arrive

asyncio.run(main())
```

All structures accept `shards` (default `100`) for distribution granularity and `timeout` (default `5.0s`) for operation timeouts. The same structure can be accessed from any node in the cluster — use the same `name` and `shards`.

**Lock** provides mutual exclusion across the cluster. Each `Lock` instance has a unique owner (auto-generated UUID). `acquire()` blocks until the lock is granted (FIFO ordering among waiters). `try_acquire()` returns immediately with `True`/`False`. `release()` returns `False` if the caller is not the current holder — only the owner can release.

**Semaphore** generalizes Lock to bounded concurrency. `semaphore("name", permits=N)` allows up to N concurrent holders. The same owner can acquire multiple permits. When all permits are taken, `acquire()` blocks and `try_acquire()` returns `False`. Released permits are granted to waiters in FIFO order.

**Barrier** provides a distributed rendezvous point. `arrive(expected=N)` blocks until N participants have reached the barrier, then releases all simultaneously. Barriers are reusable — the same name can be used for multiple rounds. The `node_id` defaults to the system's `host:port` but can be overridden for custom identification.

To make data structures persistent, pass an `EventJournal` to the `distributed()` facade. Every mutation is persisted as an event and replayed on recovery:

```python
d = system.distributed(journal=InMemoryJournal())
counter = d.counter("hits")
await counter.increment(10)
# Process restarts → replays Incremented(10) → recovers value=10
```

These structures are intentionally simple. For domain-specific entities — bank accounts, user sessions, IoT sensor aggregators — define custom behaviors using `Behaviors.sharded()` and `Behaviors.event_sourced()`. The distributed data structures exist for the common cases that don't warrant a custom actor.

---

**Next:** [Casty as a Cluster Backend](cluster-backend.md)
