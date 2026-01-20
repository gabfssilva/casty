"""Parent-Child: Actor hierarchy with ctx.actor().

Demonstrates:
- Parent spawning child actors with ctx.actor()
- Delegating work to children
- Children reporting results back to parent

Run with:
    uv run python examples/basics/03-parent-child.py
"""

import asyncio
from dataclasses import dataclass

from casty import actor, ActorSystem, Mailbox


@dataclass
class ComputeSquare:
    value: int


@dataclass
class SquareResult:
    original: int
    result: int


@dataclass
class ProcessBatch:
    values: list[int]


@dataclass
class GetResults:
    pass


@actor
async def worker(worker_id: int, *, mailbox: Mailbox[ComputeSquare]):
    async for msg, ctx in mailbox:
        match msg:
            case ComputeSquare(value):
                result = value * value
                print(f"  [Worker-{worker_id}] Computing {value}^2 = {result}")
                if ctx.sender:
                    await ctx.sender.send(SquareResult(original=value, result=result))


@actor
async def coordinator(*, mailbox: Mailbox[ProcessBatch | SquareResult | GetResults]):
    results: list[SquareResult] = []

    async for msg, ctx in mailbox:
        match msg:
            case ProcessBatch(values):
                print(f"[Coordinator] Processing batch of {len(values)} values")
                print(f"[Coordinator] Spawning {len(values)} worker children...")

                for i, value in enumerate(values):
                    child = await ctx.actor(worker(worker_id=i), name=f"worker-{i}")
                    await child.send(ComputeSquare(value), sender=ctx.self_id)

            case SquareResult(original, result):
                print(f"[Coordinator] Received result: {original}^2 = {result}")
                results.append(SquareResult(original, result))

            case GetResults():
                sorted_results = sorted(results, key=lambda r: r.original)
                await ctx.reply(sorted_results)


async def main():
    print("=" * 60)
    print("Casty Parent-Child Example")
    print("=" * 60)
    print()

    async with ActorSystem() as system:
        coord = await system.actor(coordinator(), name="coordinator")

        values = [2, 3, 5, 7, 11]
        await coord.send(ProcessBatch(values))

        await asyncio.sleep(0.3)

        results = await coord.ask(GetResults())
        print(f"\n[Main] All results collected:")
        for r in results:
            print(f"  {r.original}^2 = {r.result}")

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
