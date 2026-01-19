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

from casty import Actor, ActorSystem, Context, LocalActorRef


@dataclass
class Job:
    """A job to be processed by a worker."""
    id: int
    payload: str


@dataclass
class JobResult:
    """Result from a completed job."""
    job_id: int
    worker_id: int
    result: str


@dataclass
class GetStats:
    """Request router statistics."""
    pass


@dataclass
class RouterStats:
    """Router statistics response."""
    jobs_dispatched: int
    jobs_completed: int
    workers: int


class Worker(Actor[Job]):
    """Worker that processes jobs and reports back to the router."""

    def __init__(self, worker_id: int, router: LocalActorRef):
        self.worker_id = worker_id
        self.router = router

    async def receive(self, msg: Job, ctx: Context) -> None:
        print(f"  [Worker-{self.worker_id}] Processing job #{msg.id}: {msg.payload}")

        await asyncio.sleep(0.1)

        result = f"Processed '{msg.payload}' by worker {self.worker_id}"
        await self.router.send(JobResult(msg.id, self.worker_id, result))


class Router(Actor[Job | JobResult | GetStats]):
    """Router that distributes jobs across a pool of workers using round-robin."""

    def __init__(self, num_workers: int = 3):
        self.num_workers = num_workers
        self.workers: list[LocalActorRef[Job]] = []
        self.current_index = 0
        self.jobs_dispatched = 0
        self.jobs_completed = 0
        self.results: list[JobResult] = []

    async def on_start(self) -> None:
        print(f"[Router] Starting with {self.num_workers} workers")
        for i in range(self.num_workers):
            worker = await self._ctx.actor(
                Worker,
                name=f"worker-{i}",
                worker_id=i,
                router=self._ctx.self_ref,
            )
            self.workers.append(worker)
        print(f"[Router] All {self.num_workers} workers spawned")

    async def receive(self, msg: Job | JobResult | GetStats, ctx: Context) -> None:
        match msg:
            case Job() as job:
                worker = self.workers[self.current_index]
                self.current_index = (self.current_index + 1) % len(self.workers)
                self.jobs_dispatched += 1

                print(f"[Router] Dispatching job #{job.id} to Worker-{self.current_index}")
                await worker.send(job)

            case JobResult() as result:
                self.jobs_completed += 1
                self.results.append(result)
                print(f"[Router] Job #{result.job_id} completed: {result.result}")

            case GetStats():
                await ctx.reply(RouterStats(
                    jobs_dispatched=self.jobs_dispatched,
                    jobs_completed=self.jobs_completed,
                    workers=len(self.workers),
                ))


async def main():
    print("=" * 60)
    print("Router Pattern Example")
    print("=" * 60)
    print()

    async with ActorSystem() as system:
        router = await system.actor(Router, name="router", num_workers=3)

        await asyncio.sleep(0.1)

        print()
        print("[Main] Submitting 6 jobs...")
        print()

        for i in range(6):
            await router.send(Job(id=i, payload=f"task-{i}"))

        await asyncio.sleep(1.0)

        print()
        stats: RouterStats = await router.ask(GetStats())
        print("=" * 60)
        print(f"Final Stats:")
        print(f"  Workers: {stats.workers}")
        print(f"  Jobs dispatched: {stats.jobs_dispatched}")
        print(f"  Jobs completed: {stats.jobs_completed}")
        print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
