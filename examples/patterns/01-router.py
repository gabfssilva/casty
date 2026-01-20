"""Router pattern: Work distribution across a pool of workers.

Demonstrates:
- Creating a pool of N worker actors
- Round-robin distribution strategy
- Workers reporting completion back to router
- Job tracking and completion notification

Run with:
    uv run python examples/patterns/01-router.py
"""

import asyncio
from dataclasses import dataclass

from casty import actor, ActorSystem, Mailbox, LocalActorRef


@dataclass
class Job:
    id: int
    payload: str


@dataclass
class JobResult:
    job_id: int
    worker_id: int
    result: str


@dataclass
class GetStats:
    pass


@dataclass
class RouterStats:
    jobs_dispatched: int
    jobs_completed: int
    workers: int


@actor
async def worker(worker_id: int, router: LocalActorRef[JobResult], *, mailbox: Mailbox[Job]):
    async for msg, ctx in mailbox:
        match msg:
            case Job(id, payload):
                print(f"  [Worker-{worker_id}] Processing job #{id}: {payload}")
                await asyncio.sleep(0.1)
                result = f"Processed '{payload}' by worker {worker_id}"
                await router.send(JobResult(id, worker_id, result))


@actor
async def router(num_workers: int = 3, *, mailbox: Mailbox[Job | JobResult | GetStats]):
    workers: list[LocalActorRef[Job]] = []
    current_index = 0
    jobs_dispatched = 0
    jobs_completed = 0

    async for msg, ctx in mailbox:
        if not workers:
            print(f"[Router] Starting with {num_workers} workers")
            for i in range(num_workers):
                w = await ctx.actor(
                    worker(worker_id=i, router=ctx._self_ref),
                    name=f"worker-{i}",
                )
                workers.append(w)
            print(f"[Router] All {num_workers} workers spawned")

        match msg:
            case Job() as job:
                w = workers[current_index]
                current_index = (current_index + 1) % len(workers)
                jobs_dispatched += 1
                print(f"[Router] Dispatching job #{job.id} to Worker-{current_index}")
                await w.send(job)

            case JobResult() as result:
                jobs_completed += 1
                print(f"[Router] Job #{result.job_id} completed: {result.result}")

            case GetStats():
                await ctx.reply(RouterStats(
                    jobs_dispatched=jobs_dispatched,
                    jobs_completed=jobs_completed,
                    workers=len(workers),
                ))


async def main():
    print("=" * 60)
    print("Router Pattern Example")
    print("=" * 60)
    print()

    async with ActorSystem() as system:
        r = await system.actor(router(num_workers=3), name="router")

        await asyncio.sleep(0.1)

        print()
        print("[Main] Submitting 6 jobs...")
        print()

        for i in range(6):
            await r.send(Job(id=i, payload=f"task-{i}"))

        await asyncio.sleep(1.0)

        print()
        stats: RouterStats = await r.ask(GetStats())
        print("=" * 60)
        print(f"Final Stats:")
        print(f"  Workers: {stats.workers}")
        print(f"  Jobs dispatched: {stats.jobs_dispatched}")
        print(f"  Jobs completed: {stats.jobs_completed}")
        print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
