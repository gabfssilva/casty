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
import time
from dataclasses import dataclass

from casty import Actor, Context
from casty.chaos import ChaosConfig
from casty.cluster import (
    Strong,
    Eventual,
    Replication,
    DevelopmentCluster, AtLeast,
)


def timed(name: str):
    """Context manager to time operations."""
    class Timer:
        def __init__(self):
            self.start = 0.0
            self.elapsed_ms = 0.0

        def __enter__(self):
            self.start = time.perf_counter()
            return self

        def __exit__(self, *_):
            self.elapsed_ms = (time.perf_counter() - self.start) * 1000
            print(f"  ⏱️  {name}: {self.elapsed_ms:.2f}ms")

    return Timer()


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

    print("\n🚀 Starting cluster with chaos (100ms latency, 20ms jitter)...")

    async with DevelopmentCluster(10, chaos=ChaosConfig.lan()) as systems:
        sys1, sys2, sys3, *_ = systems

        print(f"✨ Cluster formed with {len(systems)} nodes")

        # =========================================================================
        # Example 1: Sharded actors with Strong replication
        # =========================================================================
        factor=5
        print(f"\n📊 Example 1: Sharded actors with Strong replication (factor={factor})")
        print("-" * 60)

        # Spawn sharded counters with replication, wait for all nodes
        consistency = Strong()

        with timed("spawn sharded actor"):
            counters = await sys1.spawn(
                Counter,
                name="counters",
                sharded=True,
                replication=Replication(factor=factor, consistency=consistency),
            )

        print("Sending: counters['counter-1'].send(Increment(5))")
        with timed("send with Strong consistency"):
            await counters["counter-1"].send(Increment(5))

        # Pre-register the type on all nodes (one-time setup cost)
        refs = []
        for s in systems:
            c = await s.spawn(
                Counter,
                name="counters",
                sharded=True,
                replication=Replication(factor=factor, consistency=Strong()),
            )
            refs.append(c)

        # Now measure ONLY the ask time
        for i, c in enumerate(refs):
            with timed(f"ask GetCount (node-{i})"):
                count = await c["counter-1"].ask(GetCount())
            print(f"Current count: {count}")

        # =========================================================================
        # Example 2: Fire-and-forget replication (Eventual consistency)
        # =========================================================================
        print("\n⚡ Example 2: Fire-and-forget replication (Eventual)")
        print("-" * 60)

        # Spawn with 3x replication but fire-and-forget (returns immediately)
        with timed("spawn sharded actor"):
            metrics = await sys1.spawn(
                Counter,
                name="metrics",
                sharded=True,
                replication=Replication(factor=3, consistency=Eventual()),
            )

        print("Sending: metrics['m1'].send(Increment(42))")
        with timed("send with Eventual consistency"):
            await metrics["m1"].send(Increment(42))
        # Doesn't wait for replicas - returns immediately

        await asyncio.sleep(1)  # Let replication propagate
        with timed("ask GetCount"):
            count = await metrics["m1"].ask(GetCount())
        print(f"Metrics count: {count}")

        # =========================================================================
        # Example 3: No replication (default)
        # =========================================================================
        print("\n📍 Example 3: No replication (default)")
        print("-" * 60)

        # Spawn without replication - only on primary node
        with timed("spawn sharded actor"):
            local_counter = await sys1.spawn(
                Counter,
                name="local",
                sharded=True,
                # replication defaults to Replication(factor=1, consistency=Eventual())
            )

        print("Sending: local['l1'].send(Increment(10))")
        with timed("send (no replication)"):
            await local_counter["l1"].send(Increment(10))

        await asyncio.sleep(0.2)

        with timed("ask GetCount"):
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
