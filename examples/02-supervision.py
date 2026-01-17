"""Supervision example for Casty.

Demonstrates hierarchical supervision with restart strategies.

Run with:
    uv run python examples/02-supervision.py
"""

import asyncio
import random
from dataclasses import dataclass

from casty import Actor, ActorSystem, Context
from casty.supervision import (
    SupervisionStrategy,
    supervised,
)


@dataclass
class Process:
    """Message to process work that might fail."""
    item: str


@dataclass
class GetProcessed:
    """Get the list of processed items."""
    pass


# Supervised actor with automatic restart
@supervised(
    strategy=SupervisionStrategy.RESTART,
    max_restarts=1,
    within_seconds=60.0,
)
class UnreliableWorker(Actor[Process | GetProcessed]):
    """Worker that randomly fails.

    When it fails, the supervision system automatically restarts it.
    The `post_restart` hook preserves state across restarts.
    """

    def __init__(self):
        self.processed: list[str] = []
        self._fail_probability = 0.3

    async def receive(self, msg: Process | GetProcessed, ctx: Context) -> None:
        match msg:
            case Process(item):
                # Simulate random failures
                if random.random() < self._fail_probability:
                    print(f"  [Worker] Processing '{item}' - FAILED!")
                    raise RuntimeError(f"Random failure while processing {item}")

                print(f"  [Worker] Processing '{item}' - OK")
                self.processed.append(item)

            case GetProcessed():
                await ctx.reply(self.processed.copy())

    async def pre_restart(self, exc: Exception, msg: Process | GetProcessed | None) -> None:
        """Called before restart - save state."""
        print(f"  [Worker] pre_restart: saving {len(self.processed)} items")
        # State is preserved in self.processed

    async def post_restart(self, exc: Exception) -> None:
        """Called after restart - state is preserved."""
        print(f"  [Worker] post_restart: restored with {len(self.processed)} items")


async def main():
    print("=" * 60)
    print("Casty Supervision Example")
    print("=" * 60)
    print()

    async with ActorSystem() as system:
        # Spawn supervised worker
        worker = await system.spawn(UnreliableWorker)

        # Process items - some will fail and trigger restarts
        items = ["item-1", "item-2", "item-3", "item-4", "item-5"]

        for item in items:
            await worker.send(Process(item))
            await asyncio.sleep(0.1)  # Give time for processing

        # Wait for all processing
        await asyncio.sleep(0.5)

        # Get results
        processed = await worker.ask(GetProcessed())

        print()
        print("=" * 60)
        print(f"Successfully processed: {processed}")
        print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
