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

from casty import Actor, ActorSystem, Context, LocalActorRef, on


# --- Messages ---

class JobStatus(Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    DEAD = "dead"


@dataclass
class Job:
    """A job to be processed."""
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
    """Enqueue a new job."""
    job: Job


@dataclass
class RequestWork:
    """Worker requests work from the queue."""
    worker_ref: LocalActorRef


@dataclass
class JobAssignment:
    """Job assigned to a worker."""
    job: Job


@dataclass
class JobCompleted:
    """Worker reports job completion."""
    job_id: str
    result: Any


@dataclass
class JobFailed:
    """Worker reports job failure."""
    job_id: str
    error: str


@dataclass
class GetStats:
    """Request queue statistics."""
    pass


@dataclass
class QueueStats:
    """Queue statistics response."""
    pending: int
    processing: int
    completed: int
    failed: int
    dead_letter: int


@dataclass
class ProcessJob:
    """Internal message to process a job."""
    job: Job
    queue_ref: LocalActorRef


# --- Job Queue Actor ---

type JobQueueMessage = Enqueue | RequestWork | JobCompleted | JobFailed | GetStats


class JobQueue(Actor[JobQueueMessage]):
    """Job queue that manages work distribution to workers.

    Features:
    - FIFO job queue
    - Tracks jobs in processing
    - Retries failed jobs with backoff
    - Dead-letter queue for jobs exceeding max retries
    """

    def __init__(self, name: str = "default"):
        self.name = name
        self.pending: list[Job] = []
        self.processing: dict[str, Job] = {}
        self.completed: list[Job] = []
        self.dead_letter: list[Job] = []
        self.waiting_workers: list[LocalActorRef] = []

    @on(Enqueue)
    async def handle_enqueue(self, msg: Enqueue, ctx: Context) -> None:
        job = msg.job
        print(f"[Queue:{self.name}] Job {job.id} enqueued (payload: {job.payload})")
        self.pending.append(job)
        await self._dispatch_work(ctx)

    @on(RequestWork)
    async def handle_request(self, msg: RequestWork, ctx: Context) -> None:
        if self.pending:
            await self._assign_job(msg.worker_ref)
        else:
            self.waiting_workers.append(msg.worker_ref)

    @on(JobCompleted)
    async def handle_completed(self, msg: JobCompleted, ctx: Context) -> None:
        job = self.processing.pop(msg.job_id, None)
        if job:
            job.status = JobStatus.COMPLETED
            self.completed.append(job)
            print(f"[Queue:{self.name}] Job {msg.job_id} completed (result: {msg.result})")

    @on(JobFailed)
    async def handle_failed(self, msg: JobFailed, ctx: Context) -> None:
        job = self.processing.pop(msg.job_id, None)
        if not job:
            return

        job.attempt += 1
        print(f"[Queue:{self.name}] Job {msg.job_id} failed (attempt {job.attempt}/{job.max_retries}): {msg.error}")

        if job.attempt >= job.max_retries:
            job.status = JobStatus.DEAD
            self.dead_letter.append(job)
            print(f"[Queue:{self.name}] Job {msg.job_id} moved to dead-letter queue")
        else:
            # Retry with exponential backoff
            backoff = 0.1 * (2 ** job.attempt)
            print(f"[Queue:{self.name}] Job {msg.job_id} will retry in {backoff:.1f}s")
            job.status = JobStatus.PENDING
            await ctx.schedule(backoff, Enqueue(job))

    @on(GetStats)
    async def handle_stats(self, msg: GetStats, ctx: Context) -> None:
        stats = QueueStats(
            pending=len(self.pending),
            processing=len(self.processing),
            completed=len(self.completed),
            failed=sum(1 for j in self.completed if j.status == JobStatus.FAILED),
            dead_letter=len(self.dead_letter),
        )
        await ctx.reply(stats)

    async def _dispatch_work(self, ctx: Context) -> None:
        """Dispatch pending jobs to waiting workers."""
        while self.pending and self.waiting_workers:
            worker = self.waiting_workers.pop(0)
            await self._assign_job(worker)

    async def _assign_job(self, worker: LocalActorRef) -> None:
        """Assign a pending job to a worker."""
        if not self.pending:
            return

        job = self.pending.pop(0)
        job.status = JobStatus.PROCESSING
        self.processing[job.id] = job
        await worker.send(JobAssignment(job))


# --- Worker Actor ---

type WorkerMessage = JobAssignment


class Worker(Actor[WorkerMessage]):
    """Worker that processes jobs from the queue.

    Simulates random failures to demonstrate retry logic.
    """

    def __init__(self, worker_id: int, queue_ref: LocalActorRef, fail_rate: float = 0.3):
        self.worker_id = worker_id
        self.queue_ref = queue_ref
        self.fail_rate = fail_rate
        self.jobs_processed = 0

    async def on_start(self) -> None:
        """Request work when starting."""
        await self.queue_ref.send(RequestWork(self._ctx.self_ref))

    @on(JobAssignment)
    async def handle_assignment(self, msg: JobAssignment, ctx: Context) -> None:
        job = msg.job
        print(f"  [Worker-{self.worker_id}] Processing job {job.id}...")

        # Simulate work
        await asyncio.sleep(random.uniform(0.05, 0.15))

        # Simulate random failures
        if random.random() < self.fail_rate:
            await self.queue_ref.send(JobFailed(job.id, "Random processing error"))
        else:
            result = f"processed:{job.payload}"
            await self.queue_ref.send(JobCompleted(job.id, result))
            self.jobs_processed += 1

        # Request more work
        await self.queue_ref.send(RequestWork(ctx.self_ref))


# --- Main ---

async def main():
    print("=" * 60)
    print("Casty Job Queue Example")
    print("=" * 60)
    print()

    async with ActorSystem() as system:
        # Create job queue
        queue = await system.spawn(JobQueue, name="tasks")
        print("Job queue 'tasks' created")
        print()

        # Create workers
        print("Spawning 3 workers...")
        workers = []
        for i in range(3):
            worker = await system.spawn(
                Worker,
                worker_id=i,
                queue_ref=queue,
                fail_rate=0.3,  # 30% failure rate
            )
            workers.append(worker)
        await asyncio.sleep(0.1)
        print()

        # Enqueue jobs
        print("--- Enqueueing jobs ---")
        for i in range(8):
            job = Job.create(payload=f"task-{i}", max_retries=3)
            await queue.send(Enqueue(job))
        print()

        # Wait for processing
        print("--- Processing jobs ---")
        await asyncio.sleep(2.0)
        print()

        # Get final stats
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
