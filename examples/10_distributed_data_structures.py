"""Distributed Data Structures — Dict, Counter, Set, Queue on a 5-node cluster.

Five ClusteredActorSystem nodes in the same process.
Each structure is backed by sharded actors spread across all nodes.

    Node 1 (:25530)   Node 2 (:25531)   Node 3 (:25532)   Node 4 (:25533)   Node 5 (:25534)
    ┌──────────┐      ┌──────────┐      ┌──────────┐      ┌──────────┐      ┌──────────┐
    │ counters │      │ counters │      │ counters │      │ counters │      │ counters │
    │ maps     │ ◄──► │ maps     │ ◄──► │ maps     │ ◄──► │ maps     │ ◄──► │ maps     │
    │ sets     │      │ sets     │      │ sets     │      │ sets     │      │ sets     │
    │ queues   │      │ queues   │      │ queues   │      │ queues   │      │ queues   │
    └──────────┘      └──────────┘      └──────────┘      └──────────┘      └──────────┘
"""

import asyncio
from dataclasses import dataclass

from casty.sharding import ClusteredActorSystem

BASE_PORT = 25530
NUM_NODES = 5


@dataclass(frozen=True)
class User:
    name: str
    email: str


@dataclass(frozen=True)
class Job:
    id: int
    payload: str


async def main() -> None:
    print("=== Distributed Data Structures (5-node cluster) ===\n")

    ports = [BASE_PORT + i for i in range(NUM_NODES)]
    seed_nodes = [("127.0.0.1", p) for p in ports]

    # Start 5 nodes
    systems: list[ClusteredActorSystem] = []
    for port in ports:
        others = [(h, p) for h, p in seed_nodes if p != port]
        s = ClusteredActorSystem(
            name="ddata-cluster",
            host="127.0.0.1",
            port=port,
            seed_nodes=others,
        )
        await s.__aenter__()
        systems.append(s)

    await asyncio.sleep(0.3)  # let cluster form

    # Create Distributed facades — each node gets its own
    d1 = systems[0].distributed()
    d3 = systems[2].distributed()
    d5 = systems[4].distributed()

    await asyncio.sleep(0.3)  # let coordinators allocate shards

    # ── Counter ───────────────────────────────────────────────────────
    print("--- Counter ---")

    views = d1.counter("page-views", shards=50)
    likes = d3.counter("likes", shards=50)

    await views.increment(100)
    await views.increment(50)
    await likes.increment(42)

    # Read from a different node
    views_from_5 = d5.counter("page-views", shards=50)
    print(f"  page-views (written on node 1, read from node 5): {await views_from_5.get()}")
    print(f"  likes (written on node 3, read from node 1): {await d1.counter('likes', shards=50).get()}")

    # ── Dict ──────────────────────────────────────────────────────────
    print("\n--- Dict ---")

    users = d1.map[str, User]("users", shards=50)
    await users.put("alice", User("Alice", "alice@example.com"))
    await users.put("bob", User("Bob", "bob@example.com"))
    await users.put("carol", User("Carol", "carol@example.com"))

    # Read from different nodes
    users_from_3 = d3.map[str, User]("users", shards=50)
    users_from_5 = d5.map[str, User]("users", shards=50)

    alice = await users_from_3.get("alice")
    bob = await users_from_5.get("bob")
    print(f"  alice (written node 1, read node 3): {alice}")
    print(f"  bob   (written node 1, read node 5): {bob}")

    # Delete from node 3, verify from node 5
    await users_from_3.delete("carol")
    print(f"  carol exists after delete? {await users_from_5.contains('carol')}")

    # ── Set ───────────────────────────────────────────────────────────
    print("\n--- Set ---")

    tags = d1.set[str]("active-tags", shards=50)
    await tags.add("python")
    await tags.add("asyncio")
    await tags.add("actors")
    await tags.add("distributed")

    # Read from node 5
    tags_from_5 = d5.set[str]("active-tags", shards=50)
    print(f"  size (written node 1, read node 5): {await tags_from_5.size()}")
    print(f"  contains 'python': {await tags_from_5.contains('python')}")
    print(f"  contains 'java': {await tags_from_5.contains('java')}")

    # Remove from node 3
    tags_from_3 = d3.set[str]("active-tags", shards=50)
    await tags_from_3.remove("asyncio")
    print(f"  size after remove from node 3: {await tags_from_5.size()}")

    # ── Queue ─────────────────────────────────────────────────────────
    print("\n--- Queue ---")

    jobs = d1.queue[Job]("work-queue", shards=50)

    # Produce from node 1
    for i in range(5):
        await jobs.enqueue(Job(id=i, payload=f"task-{i}"))
    print(f"  enqueued 5 jobs from node 1, size: {await jobs.size()}")

    # Consume from node 5
    jobs_from_5 = d5.queue[Job]("work-queue", shards=50)
    print(f"  peek from node 5: {await jobs_from_5.peek()}")

    consumed = []
    while (job := await jobs_from_5.dequeue()) is not None:
        consumed.append(job)
    print(f"  consumed {len(consumed)} jobs from node 5: {[j.id for j in consumed]}")
    print(f"  queue empty? size={await jobs.size()}")

    # ── Cleanup ───────────────────────────────────────────────────────
    print("\n--- Shutting down cluster ---")
    for s in reversed(systems):
        await s.__aexit__(None, None, None)

    print("\n=== Done ===")


asyncio.run(main())
