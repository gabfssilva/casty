"""Replicated Counter Example.

Demonstrates:
- @actor(replicated=3) for automatic replication
- Routing.ANY for reads, Routing.LEADER for writes
- State synchronization across cluster nodes

Run with: uv run python examples/distributed/08-replicated-counter.py
"""

import asyncio
from dataclasses import dataclass

from casty import actor, Mailbox
from casty.state import State
from casty.actor_config import Routing
from casty.cluster import DevelopmentCluster, Merge
from casty.serializable import serializable


@serializable
@dataclass
class Increment:
    amount: int = 1


@serializable
@dataclass
class Get:
    pass


type CounterMsg = Increment | Get | Merge[int]


@actor(replicated=3, routing={Get: Routing.LOCAL_FIRST, Increment: Routing.LOCAL_FIRST})
async def counter(*, mailbox: Mailbox[CounterMsg], state: State[int]):
    async for msg, ctx in mailbox:
        match msg:
            case Increment(amount):
                state.set(state.value + amount)
                print(f"  [{ctx.node_id}] Incremented by {amount}, now={state.value}")
                await ctx.reply(state.value)

            case Get():
                print(f"  [{ctx.node_id}] Get now={state.value}")
                await ctx.reply(state.value)

            case Merge(base, local, remote):
                base_val = base or 0
                merged = base_val + (local - base_val) + (remote - base_val)
                state.set(merged)
                print(f"  [{ctx.node_id}] Merged: {base_val} + ({local}-{base_val}) + ({remote}-{base_val}) = {merged}")


async def main():
    async with asyncio.timeout(10):
        print("=== Replicated Counter Example ===\n")

        async with DevelopmentCluster(10) as cluster:
            print("Creating replicated counter (replicas=3)...")
            ref = await cluster.actor(counter(state=0), name="counter")
            print()

            print("Phase 1: Writes go to LOCAL replica")
            print("-" * 40)
            await ref.ask(Increment(10))
            await ref.ask(Increment(10))
            await ref.ask(Increment(10))
            await ref.ask(Increment(10))
            await ref.ask(Increment(10))
            await ref.ask(Increment(5))

            await asyncio.sleep(1)

            print("\nPhase 2: Reads can go to ANY replica")
            print("-" * 40)
            count = await ref.ask(Get())
            print(f"  Count = {count}")

            print("\n=== Summary ===")
            print("Demonstrated: @actor(replicated=3), Routing.ANY/LEADER")


if __name__ == "__main__":
    asyncio.run(main())
