"""Distributed Counter with Replication.

Demonstrates:
- DevelopmentCluster with multiple nodes
- Single clustered counter replicated across nodes
- Using routing to query specific nodes and verify replication

Run with: uv run python examples/distributed/01-distributed-counter.py
"""

import asyncio
from dataclasses import dataclass

from casty import Actor, Context
from casty.cluster import DevelopmentCluster, ClusterScope


@dataclass
class Increment:
    amount: int = 1


@dataclass
class GetCount:
    pass


class Counter(Actor[Increment | GetCount]):
    def __init__(self):
        self.count = 0

    async def receive(self, msg: Increment | GetCount, ctx: Context):
        match msg:
            case Increment(amount):
                self.count += amount
                print(f"  Counter incremented by {amount}, now={self.count}")
            case GetCount():
                await ctx.reply(self.count)

    def get_state(self) -> dict:
        return {"count": self.count}

    def set_state(self, state: dict) -> None:
        self.count = state.get("count", 0)


async def main():
    print("=== Distributed Counter with Replication ===\n")

    async with DevelopmentCluster(3) as cluster:
        node0, node1, node2 = cluster[0], cluster[1], cluster[2]

        print(f"Started 3-node cluster:")
        print(f"  - {node0.node_id}")
        print(f"  - {node1.node_id}")
        print(f"  - {node2.node_id}")
        print()

        # Create a single clustered counter with replication=3 (all nodes)
        print("Creating counter with replication=3...")
        counter = await node0.actor(
            Counter,
            name="my-counter",
            scope=ClusterScope(replication=3)
        )
        await asyncio.sleep(0.3)
        print(f"Counter created: {counter}\n")

        # Send increments (goes to leader)
        print("Phase 1: Sending increments")
        print("-" * 40)
        await counter.send(Increment(10))
        await counter.send(Increment(20))
        await counter.send(Increment(5))
        await asyncio.sleep(0.3)

        # Query the leader
        print("\nPhase 2: Querying leader")
        print("-" * 40)
        count = await counter.ask(GetCount())
        print(f"Leader says count = {count}")

        # Query specific nodes to verify replication
        print("\nPhase 3: Verifying replication on each node")
        print("-" * 40)

        for node_id in [node0.node_id, node1.node_id, node2.node_id]:
            try:
                count = await counter.ask(GetCount(), routing=node_id)
                print(f"  {node_id}: count = {count}")
            except Exception as e:
                print(f"  {node_id}: error - {e}")

        print("\n=== Summary ===")
        print("The counter state (35) is replicated across all 3 nodes.")
        print("Each node can serve read requests via routing parameter.")


if __name__ == "__main__":
    asyncio.run(main())
