"""Job Queue Example - Task Processing with Workers.

Demonstrates:
- Job queue actor managing a work queue
- Pool of worker actors consuming jobs
- Retry with exponential backoff on failure
- Dead-letter queue after max retries
- Job completion acknowledgment

Run with:
    uv run python examples/practical/02-job-queue.py
"""

import asyncio
import random
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any
from uuid import uuid4

from casty import actor, ActorSystem, Mailbox, LocalActorRef


class JobStatus(Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    DEAD = "dead"


@dataclass
class Job:
    id: str
    payload: Any
    max_retries: int = 3
    attempt: int = 0
    status: JobStatus = JobStatus.PENDING
    created_at: datetime = field(default_factory=datetime.now)

    @staticmethod
    def create(payload: Any, max_retries: int = 3) -> "Job":
        return Job(id=str(uuid4())[:8], payload=payload, max_retries=max_retries)


@dataclass
class Enqueue:
    job: Job


@dataclass
class RequestWork:
    worker_ref: LocalActorRef


@dataclass
class JobAssignment:
    job: Job


@dataclass
class JobCompleted:
    job_id: str
    result: Any


@dataclass
class JobFailed:
    job_id: str
    error: str


@dataclass
class GetStats:
    pass


@dataclass
class QueueStats:
    pending: int
    processing: int
    completed: int
    failed: int
    dead_letter: int


JobQueueMsg = Enqueue | RequestWork | JobCompleted | JobFailed | GetStats


@actor
async def job_queue(queue_name: str = "default", *, mailbox: Mailbox[JobQueueMsg]):
    pending: list[Job] = []
    processing: dict[str, Job] = {}
    completed: list[Job] = []
    dead_letter: list[Job] = []
    waiting_workers: list[LocalActorRef] = []

    async def assign_job(worker: LocalActorRef):
        if not pending:
            return
        job = pending.pop(0)
        job.status = JobStatus.PROCESSING
        processing[job.id] = job
        await worker.send(JobAssignment(job))

    async def dispatch_work():
        while pending and waiting_workers:
            worker = waiting_workers.pop(0)
            await assign_job(worker)

    async for msg, ctx in mailbox:
        match msg:
            case Enqueue(job):
                print(f"[Queue:{queue_name}] Job {job.id} enqueued (payload: {job.payload})")
                pending.append(job)
                await dispatch_work()

            case RequestWork(worker_ref):
                if pending:
                    await assign_job(worker_ref)
                else:
                    waiting_workers.append(worker_ref)

            case JobCompleted(job_id, result):
                job = processing.pop(job_id, None)
                if job:
                    job.status = JobStatus.COMPLETED
                    completed.append(job)
                    print(f"[Queue:{queue_name}] Job {job_id} completed (result: {result})")

            case JobFailed(job_id, error):
                job = processing.pop(job_id, None)
                if not job:
                    continue

                job.attempt += 1
                print(f"[Queue:{queue_name}] Job {job_id} failed (attempt {job.attempt}/{job.max_retries}): {error}")

                if job.attempt >= job.max_retries:
                    job.status = JobStatus.DEAD
                    dead_letter.append(job)
                    print(f"[Queue:{queue_name}] Job {job_id} moved to dead-letter queue")
                else:
                    backoff = 0.1 * (2 ** job.attempt)
                    print(f"[Queue:{queue_name}] Job {job_id} will retry in {backoff:.1f}s")
                    job.status = JobStatus.PENDING
                    await ctx.schedule(Enqueue(job), delay=backoff)

            case GetStats():
                stats = QueueStats(
                    pending=len(pending),
                    processing=len(processing),
                    completed=len(completed),
                    failed=sum(1 for j in completed if j.status == JobStatus.FAILED),
                    dead_letter=len(dead_letter),
                )
                await ctx.reply(stats)


@actor
async def worker(worker_id: int, queue_ref: LocalActorRef[JobQueueMsg], fail_rate: float = 0.3, *, mailbox: Mailbox[JobAssignment]):
    await queue_ref.send(RequestWork(mailbox._self_ref))

    async for msg, ctx in mailbox:
        match msg:
            case JobAssignment(job):
                print(f"  [Worker-{worker_id}] Processing job {job.id}...")
                await asyncio.sleep(random.uniform(0.05, 0.15))

                if random.random() < fail_rate:
                    await queue_ref.send(JobFailed(job.id, "Random processing error"))
                else:
                    result = f"processed:{job.payload}"
                    await queue_ref.send(JobCompleted(job.id, result))

                await queue_ref.send(RequestWork(ctx._self_ref))


async def main():
    print("=" * 60)
    print("Casty Job Queue Example")
    print("=" * 60)
    print()

    async with ActorSystem() as system:
        queue = await system.actor(job_queue(queue_name="tasks"), name="job-queue-tasks")
        print("Job queue 'tasks' created")
        print()

        print("Spawning 3 workers...")
        for i in range(3):
            await system.actor(
                worker(worker_id=i, queue_ref=queue, fail_rate=0.3),
                name=f"worker-{i}",
            )
        await asyncio.sleep(0.1)
        print()

        print("--- Enqueueing jobs ---")
        for i in range(8):
            job = Job.create(payload=f"task-{i}", max_retries=3)
            await queue.send(Enqueue(job))
        print()

        print("--- Processing jobs ---")
        await asyncio.sleep(2.0)
        print()

        print("--- Final Statistics ---")
        stats: QueueStats = await queue.ask(GetStats())
        print(f"Pending:     {stats.pending}")
        print(f"Processing:  {stats.processing}")
        print(f"Completed:   {stats.completed}")
        print(f"Dead-letter: {stats.dead_letter}")

    print()
    print("=" * 60)
    print("Job queue shut down")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
