"""Example: Clustered Counter using DevelopmentCluster.

Shows both ways to use DevelopmentCluster:
1. Direct cluster usage (operations on random nodes)
2. Specific node access when needed
"""

import asyncio
from dataclasses import dataclass

from casty import Actor, Context
from casty.cluster import DevelopmentCluster


@dataclass
class Increment:
    amount: int


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
            case GetCount():
                await ctx.reply(self.count)


async def main():
    # DevelopmentCluster implements System protocol
    # Operations go to random nodes by default
    async with DevelopmentCluster(10) as cluster:
        print(f"Cluster with {len(cluster)} nodes")
        print(f"  - {cluster[0].node_id}")
        print(f"  - {cluster[1].node_id}")

        await asyncio.sleep(0.5)

        # Direct cluster usage - spawns on random node
        counter = await cluster.spawn(Counter, clustered=True, name="counter", replication=2, write_consistency=2)
        print(f"\nSpawned clustered counter (on random node)")

        # Operations go through cluster
        await cluster.spawn(Counter, clustered=True, name="counter")  # Gets existing
        await counter.send(Increment(10))
        await counter.send(Increment(5))
        await counter.send(Increment(3))

        result = await counter.ask(GetCount())
        print(f"Count: {result}")  # Should print 18

        # When you need a specific node, use cluster.node()
        print("\n--- Accessing specific node ---")
        counter2 = await cluster.node(1).spawn(Counter, clustered=True, name="counter")
        print(f"Got counter from {cluster.node(1).node_id}")
        result2 = await counter2.ask(GetCount())
        print(f"Count from node-1: {result2}")


if __name__ == "__main__":
    asyncio.run(main())
