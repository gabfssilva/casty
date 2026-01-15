"""Example: Replication in Casty

This example demonstrates how to use the Replication API for high-availability
actors across a distributed cluster.

Replication provides:
- Configurable replication factor (how many copies)
- Tunable consistency levels (Eventual, Quorum, Strong)
- Automatic failover via hash ring rebalancing
- Hinted handoff for offline replicas
"""

import asyncio
from dataclasses import dataclass

from casty import ActorSystem, Actor, Context
from casty.cluster import (
    Quorum,
    Strong,
    Eventual,
    Replication,
)


# ============================================================================
# Example Actors
# ============================================================================


@dataclass
class Increment:
    amount: int


@dataclass
class GetCount:
    pass


class Counter(Actor[Increment | GetCount]):
    """A simple counter actor for demonstration."""

    def __init__(self, entity_id: str = "") -> None:
        self.entity_id = entity_id
        self.count = 0

    async def receive(self, msg: Increment | GetCount, ctx: Context) -> None:
        match msg:
            case Increment(amount):
                self.count += amount
                print(f"Counter incremented: {self.count}")

            case GetCount():
                ctx.reply(self.count)


# ============================================================================
# Main Example
# ============================================================================


async def main() -> None:
    """Demonstrate replication with different consistency levels."""

    # Create 3 clustered systems
    async with ActorSystem.clustered("127.0.0.1", 7946) as sys1:
        async with ActorSystem.clustered(
            "127.0.0.1", 7947, seeds=["127.0.0.1:7946"]
        ) as sys2:
            async with ActorSystem.clustered(
                "127.0.0.1", 7948, seeds=["127.0.0.1:7946"]
            ) as sys3:
                await asyncio.sleep(0.5)

                print("\n✨ Cluster formed with 3 nodes")

                # =========================================================================
                # Example 1: Sharded actors with Quorum replication
                # =========================================================================
                print("\n📊 Example 1: Sharded actors with Quorum replication")
                print("-" * 60)

                # Spawn sharded counters with 3x replication, wait for quorum (2/3 acks)
                counters = await sys1.spawn(
                    Counter,
                    name="counters",
                    sharded=True,
                    replication=Replication(factor=3, consistency=Quorum()),
                )

                # Send message to entity - will be replicated to 3 nodes
                print("Sending: counters['counter-1'].send(Increment(5))")
                await counters["counter-1"].send(Increment(5))
                await asyncio.sleep(0.5)

                # Ask from any node
                count = await counters["counter-1"].ask(GetCount())
                print(f"Current count: {count}")

                # =========================================================================
                # Example 2: Fire-and-forget replication (Eventual consistency)
                # =========================================================================
                print("\n⚡ Example 2: Fire-and-forget replication (Eventual)")
                print("-" * 60)

                # Spawn with 3x replication but fire-and-forget (returns immediately)
                metrics = await sys1.spawn(
                    Counter,
                    name="metrics",
                    sharded=True,
                    replication=Replication(factor=3, consistency=Eventual()),
                )

                print("Sending: metrics['m1'].send(Increment(42))")
                await metrics["m1"].send(Increment(42))
                # Doesn't wait for replicas - returns immediately

                await asyncio.sleep(1)  # Let replication propagate
                count = await metrics["m1"].ask(GetCount())
                print(f"Metrics count: {count}")

                # =========================================================================
                # Example 3: No replication (default)
                # =========================================================================
                print("\n📍 Example 3: No replication (default)")
                print("-" * 60)

                # Spawn without replication - only on primary node
                local_counter = await sys1.spawn(
                    Counter,
                    name="local",
                    sharded=True,
                    # replication defaults to Replication(factor=1, consistency=Eventual())
                )

                print("Sending: local['l1'].send(Increment(10))")
                await local_counter["l1"].send(Increment(10))
                await asyncio.sleep(0.2)

                count = await local_counter["l1"].ask(GetCount())
                print(f"Local count: {count}")

                # =========================================================================
                # API Summary
                # =========================================================================
                print("\n📚 API Summary")
                print("-" * 60)
                print(
                    """
Replication(factor=N, consistency=LEVEL):
  - factor: 1=no replication, 2+=replicated
  - consistency: Eventual() | Quorum() | Strong() | AtLeast(n)

Usage:
  await system.spawn(
      MyActor,
      name="my-actor",
      sharded=True,
      replication=Replication(factor=3, consistency=Quorum()),
  )

Benefits:
  ✓ High Availability: Actor survives node failures
  ✓ Configurable: Choose consistency vs performance trade-off
  ✓ Transparent: No changes to actor code
  ✓ Automatic Failover: Hash ring rebalancing on failures
  ✓ Hinted Handoff: Automatic catch-up on recovery
                    """
                )

                print("\n✅ Done!")


if __name__ == "__main__":
    asyncio.run(main())
