"""Distributed Counter with CRDT-like Semantics.

Demonstrates:
- DevelopmentCluster with multiple nodes
- Counter replicated across all nodes
- Concurrent increments from different nodes
- State convergence via G-Counter (grow-only counter)

A G-Counter is a CRDT (Conflict-free Replicated Data Type) that ensures
all nodes eventually converge to the same value, even with concurrent
updates from different nodes.

Run with: uv run python examples/distributed/01-distributed-counter.py
"""

import asyncio
from dataclasses import dataclass

from casty import Actor, Context
from casty.cluster import DevelopmentCluster


@dataclass
class Increment:
    """Increment the counter by amount."""
    amount: int = 1


@dataclass
class GetCount:
    """Get the current counter value."""
    pass


@dataclass
class GetState:
    """Get the full CRDT state (for debugging)."""
    pass


@dataclass
class MergeState:
    """Merge state from another node (for CRDT convergence)."""
    node_counts: dict[str, int]


class GCounter(Actor[Increment | GetCount | GetState | MergeState]):
    """A G-Counter (Grow-only Counter) CRDT.

    Each node maintains its own counter. The total value is the sum of all
    node counters. This ensures convergence even with concurrent increments.
    """

    def __init__(self, node_id: str):
        self.node_id = node_id
        self.counts: dict[str, int] = {node_id: 0}

    async def receive(self, msg: Increment | GetCount | GetState | MergeState, ctx: Context):
        match msg:
            case Increment(amount):
                self.counts[self.node_id] = self.counts.get(self.node_id, 0) + amount
                print(f"  [{self.node_id}] Incremented by {amount}, local={self.counts[self.node_id]}")

            case GetCount():
                total = sum(self.counts.values())
                await ctx.reply(total)

            case GetState():
                await ctx.reply(dict(self.counts))

            case MergeState(node_counts):
                for node, count in node_counts.items():
                    self.counts[node] = max(self.counts.get(node, 0), count)
                print(f"  [{self.node_id}] Merged state: {self.counts}")


async def main():
    print("=== Distributed Counter (G-Counter CRDT) ===\n")

    async with DevelopmentCluster(3) as (node0, node1, node2):  # type: ignore[misc]
        print(f"Started 3-node cluster:")
        print(f"  - Node 0: {node0.node_id}")
        print(f"  - Node 1: {node1.node_id}")
        print(f"  - Node 2: {node2.node_id}")
        print()

        await asyncio.sleep(0.5)

        # Each node spawns its own local counter with a unique node_id
        counter0 = await node0.spawn(GCounter, node_id=node0.node_id)
        counter1 = await node1.spawn(GCounter, node_id=node1.node_id)
        counter2 = await node2.spawn(GCounter, node_id=node2.node_id)

        print("Phase 1: Concurrent increments from different nodes")
        print("-" * 50)

        # Concurrent increments from different nodes
        await asyncio.gather(
            counter0.send(Increment(10)),
            counter1.send(Increment(20)),
            counter2.send(Increment(30)),
        )

        # Each node sees only its local increment
        await asyncio.sleep(0.1)
        count0 = await counter0.ask(GetCount())
        count1 = await counter1.ask(GetCount())
        count2 = await counter2.ask(GetCount())

        print(f"\nBefore merge (local views):")
        print(f"  Node 0 sees: {count0}")
        print(f"  Node 1 sees: {count1}")
        print(f"  Node 2 sees: {count2}")

        print("\nPhase 2: State synchronization (simulating gossip)")
        print("-" * 50)

        # Get states from all nodes
        state0 = await counter0.ask(GetState())
        state1 = await counter1.ask(GetState())
        state2 = await counter2.ask(GetState())

        # Merge states across all nodes (simulating gossip protocol)
        # Each node receives states from other nodes
        await counter0.send(MergeState(state1))
        await counter0.send(MergeState(state2))

        await counter1.send(MergeState(state0))
        await counter1.send(MergeState(state2))

        await counter2.send(MergeState(state0))
        await counter2.send(MergeState(state1))

        await asyncio.sleep(0.1)

        # Now all nodes should converge to the same value
        count0 = await counter0.ask(GetCount())
        count1 = await counter1.ask(GetCount())
        count2 = await counter2.ask(GetCount())

        print(f"\nAfter merge (converged):")
        print(f"  Node 0 sees: {count0}")
        print(f"  Node 1 sees: {count1}")
        print(f"  Node 2 sees: {count2}")

        print("\nPhase 3: More concurrent updates")
        print("-" * 50)

        # More increments
        await counter0.send(Increment(5))
        await counter1.send(Increment(15))

        # Partial sync - only node0 and node1 exchange
        state0 = await counter0.ask(GetState())
        state1 = await counter1.ask(GetState())

        await counter0.send(MergeState(state1))
        await counter1.send(MergeState(state0))

        await asyncio.sleep(0.1)

        count0 = await counter0.ask(GetCount())
        count1 = await counter1.ask(GetCount())
        count2 = await counter2.ask(GetCount())

        print(f"\nPartial sync (node0 <-> node1):")
        print(f"  Node 0 sees: {count0} (has node1's update)")
        print(f"  Node 1 sees: {count1} (has node0's update)")
        print(f"  Node 2 sees: {count2} (still has old state)")

        # Full sync
        state0 = await counter0.ask(GetState())
        await counter2.send(MergeState(state0))

        count2 = await counter2.ask(GetCount())
        print(f"  Node 2 after sync: {count2}")

        print("\n=== Summary ===")
        print("G-Counter CRDT properties:")
        print("  - Eventual consistency: all nodes converge to same value")
        print("  - Partition tolerance: updates continue during network splits")
        print("  - Conflict-free: merge operation is commutative and idempotent")
        print(f"\nFinal converged value: {count2}")

        # Allow cluster to settle before shutdown
        await asyncio.sleep(0.2)


if __name__ == "__main__":
    asyncio.run(main())
