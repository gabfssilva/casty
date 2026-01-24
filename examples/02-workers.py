"""Workers: Parent-child actor hierarchy.

Demonstrates:
- Spawning child actors with ctx.actor()
- Parent delegating work to children
- Children reporting results back to parent

Run with:
    uv run python examples/02-workers.py
"""

import asyncio
from dataclasses import dataclass

from casty import actor, ActorSystem, Mailbox


@dataclass
class Process:
    value: int


@dataclass
class Result:
    original: int
    squared: int


@dataclass
class ProcessBatch:
    values: list[int]


@dataclass
class GetResults:
    pass


type WorkerMsg = Process
type CoordinatorMsg = ProcessBatch | Result | GetResults


@actor
async def worker(worker_id: int, *, mailbox: Mailbox[WorkerMsg]):
    async for msg, ctx in mailbox:
        match msg:
            case Process(value):
                result = value * value
                print(f"  Worker-{worker_id}: {value}² = {result}")
                await ctx.reply(Result(value, result))


@actor
async def coordinator(*, mailbox: Mailbox[CoordinatorMsg]):
    results: list[Result] = []

    async for msg, ctx in mailbox:
        match msg:
            case ProcessBatch(values):
                print(f"Spawning {len(values)} workers...")
                for i, value in enumerate(values):
                    child = await ctx.actor(worker(i), name=f"worker-{i}")
                    result = await child.ask(Process(value))
                    results.append(result)

            case GetResults():
                await ctx.reply(results)


async def main():
    async with ActorSystem() as system:
        coord = await system.actor(coordinator(), name="coordinator")

        await coord.send(ProcessBatch([2, 3, 5, 7, 11]))
        await asyncio.sleep(0.1)

        results = await coord.ask(GetResults())
        print(f"\nResults: {[(r.original, r.squared) for r in results]}")


if __name__ == "__main__":
    asyncio.run(main())
