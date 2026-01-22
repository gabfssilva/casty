"""Distributed Counter Example.

Demonstrates:
- DevelopmentCluster with multiple nodes
- Creating a single actor that lives on one node
- All nodes can access the same actor

Run with: uv run python examples/distributed/01-distributed-counter.py
"""

import asyncio
from dataclasses import dataclass

from casty import actor, Mailbox
from casty.cluster import DevelopmentCluster, DistributionStrategy


@dataclass
class Increment:
    amount: int = 1


@dataclass
class GetCount:
    pass


CounterMsg = Increment | GetCount


@actor
async def counter(*, mailbox: Mailbox[CounterMsg]):
    count = 0

    async for msg, ctx in mailbox:
        match msg:
            case Increment(amount):
                count += amount
                print(f"  Counter incremented by {amount}, now={count}")
            case GetCount():
                await ctx.reply(count)


async def main():
    # do not increase the timeout. 10s is more than enough
    async with asyncio.timeout(10):
        print("=== Distributed Counter Example ===\n")

        async with DevelopmentCluster(3, strategy=DistributionStrategy.CONSISTENT, debug=False) as cluster:
            node0, node1, node2 = cluster[0], cluster[1], cluster[2]

            print(f"Started 3-node cluster:")
            print(f"  - {node0.node_id}")
            print(f"  - {node1.node_id}")
            print(f"  - {node2.node_id}")
            print()

            # Create ONE counter via cluster (placed on one node by round-robin)
            print("Creating single counter via cluster...")
            counter_ref = await cluster.actor(counter(), name="counter")
            print()

            print("Phase 1: Sending increments")
            print("-" * 40)
            await counter_ref.send(Increment(10))
            counter_ref = await cluster.actor(counter(), name="counter")
            await counter_ref.send(Increment(20))
            counter_ref = await cluster.actor(counter(), name="counter")
            await counter_ref.send(Increment(5))
            counter_ref = await cluster.actor(counter(), name="counter")
            await counter_ref.send(Increment(100))
            counter_ref = await cluster.actor(counter(), name="counter")
            await counter_ref.send(Increment(1000))
            await asyncio.sleep(0.1)

            print("\nPhase 2: Querying counter")
            print("-" * 40)
            count = await counter_ref.ask(GetCount())
            print(f"  Total count = {count}")

            print("\n=== Summary ===")
            print("Single counter actor, accessed from cluster.")
            print(f"Final count: {count}")


if __name__ == "__main__":
    asyncio.run(main())

