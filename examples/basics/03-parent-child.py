"""Parent-Child: Actor hierarchy with ctx.spawn().

Demonstrates:
- Parent spawning child actors with ctx.spawn()
- Delegating work to children
- Children reporting results back to parent
- Accessing ctx.children to list spawned children

Run with:
    uv run python examples/basics/03-parent-child.py
"""

import asyncio
from dataclasses import dataclass

from casty import Actor, ActorSystem, Context


# --- Messages ---

@dataclass
class ComputeSquare:
    """Request to compute the square of a number."""
    value: int


@dataclass
class SquareResult:
    """Result of squaring a number."""
    original: int
    result: int


@dataclass
class ProcessBatch:
    """Request parent to process a batch of numbers."""
    values: list[int]


@dataclass
class GetResults:
    """Request all collected results."""
    pass


@dataclass
class GetChildCount:
    """Request the number of child actors."""
    pass


# --- Child Actor ---

class Worker(Actor[ComputeSquare]):
    """Child worker that computes squares and reports back to parent."""

    def __init__(self, worker_id: int):
        self.worker_id = worker_id

    async def receive(self, msg: ComputeSquare, ctx: Context):
        match msg:
            case ComputeSquare(value):
                result = value * value
                print(f"  [Worker-{self.worker_id}] Computing {value}^2 = {result}")
                # Send result back to parent (ctx.parent is set automatically)
                if ctx.parent:
                    await ctx.parent.send(SquareResult(original=value, result=result))


# --- Parent Actor ---

class Coordinator(Actor[ProcessBatch | SquareResult | GetResults | GetChildCount]):
    """Parent actor that spawns workers and collects results."""

    def __init__(self):
        self.results: list[SquareResult] = []
        self.workers_spawned = 0

    async def receive(
        self,
        msg: ProcessBatch | SquareResult | GetResults | GetChildCount,
        ctx: Context,
    ):
        match msg:
            case ProcessBatch(values):
                print(f"[Coordinator] Processing batch of {len(values)} values")
                print(f"[Coordinator] Spawning {len(values)} worker children...")

                # Spawn a child worker for each value
                for i, value in enumerate(values):
                    # ctx.spawn() creates a child actor with this actor as parent
                    worker = await ctx.spawn(Worker, worker_id=i, name=f"worker-{i}")
                    self.workers_spawned += 1
                    # Delegate work to the child
                    await worker.send(ComputeSquare(value))

                print(f"[Coordinator] Current children: {len(ctx.children)}")

            case SquareResult(original, result):
                print(f"[Coordinator] Received result: {original}^2 = {result}")
                self.results.append(SquareResult(original, result))

            case GetResults():
                # Sort results by original value for consistent output
                sorted_results = sorted(self.results, key=lambda r: r.original)
                await ctx.reply(sorted_results)

            case GetChildCount():
                await ctx.reply(len(ctx.children))


async def main():
    print("=" * 60)
    print("Casty Parent-Child Example")
    print("=" * 60)
    print()

    async with ActorSystem() as system:
        # Spawn the coordinator (parent)
        coordinator = await system.spawn(Coordinator)

        # Ask coordinator to process a batch of numbers
        values = [2, 3, 5, 7, 11]
        await coordinator.send(ProcessBatch(values))

        # Wait for all workers to complete
        await asyncio.sleep(0.3)

        # Check how many children the coordinator has
        child_count = await coordinator.ask(GetChildCount())
        print(f"\n[Main] Coordinator has {child_count} children")

        # Get all results
        results = await coordinator.ask(GetResults())
        print(f"[Main] All results collected:")
        for r in results:
            print(f"  {r.original}^2 = {r.result}")

        # Verify correctness
        expected = {v: v * v for v in values}
        actual = {r.original: r.result for r in results}
        assert actual == expected, f"Mismatch! Expected {expected}, got {actual}"
        print("\n[Main] All results verified correct!")

    print()
    print("=" * 60)
    print("Done!")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
