"""Supervision — retry strategies, lifecycle hooks, and supervision trees."""

import asyncio
from dataclasses import dataclass

from casty import (
    ActorContext,
    ActorRef,
    ActorSystem,
    Behavior,
    Behaviors,
    Directive,
    OneForOneStrategy,
)


# ── Messages ─────────────────────────────────────────────────────────


@dataclass(frozen=True)
class Process:
    task: str


@dataclass(frozen=True)
class GetProcessed:
    reply_to: ActorRef[tuple[str, ...]]


@dataclass(frozen=True)
class CrashOnPurpose:
    pass


type WorkerMsg = Process | GetProcessed | CrashOnPurpose


# ── Worker behavior ──────────────────────────────────────────────────


def worker(processed: tuple[str, ...] = ()) -> Behavior[WorkerMsg]:
    async def receive(
        ctx: ActorContext[WorkerMsg], msg: WorkerMsg
    ) -> Behavior[WorkerMsg]:
        match msg:
            case Process(task) if task == "bad":
                raise ValueError(f"Cannot process: {task}")
            case Process(task):
                print(f"  [{ctx.self.address.path}] Processed: {task}")
                return worker((*processed, task))
            case GetProcessed(reply_to):
                reply_to.tell(processed)
                return Behaviors.same()
            case CrashOnPurpose():
                raise RuntimeError("Intentional crash!")

    return Behaviors.receive(receive)


# ── Supervision strategies ───────────────────────────────────────────


def always_restart() -> OneForOneStrategy:
    return OneForOneStrategy(
        max_restarts=3,
        within=60.0,
        decider=lambda _exc: Directive.restart,
    )


def selective_strategy() -> OneForOneStrategy:
    def decide(exc: Exception) -> Directive:
        match exc:
            case ValueError():
                print(f"  ValueError — restarting: {exc}")
                return Directive.restart
            case RuntimeError():
                print(f"  RuntimeError — stopping: {exc}")
                return Directive.stop
            case _:
                return Directive.escalate

    return OneForOneStrategy(max_restarts=5, within=60.0, decider=decide)


# ── Lifecycle hooks ──────────────────────────────────────────────────


def resilient_worker() -> Behavior[WorkerMsg]:
    async def on_start(ctx: ActorContext[WorkerMsg]) -> None:
        print(f"  [{ctx.self.address.path}] Started")

    async def on_stop(ctx: ActorContext[WorkerMsg]) -> None:
        print(f"  [{ctx.self.address.path}] Stopped")

    async def on_restart(ctx: ActorContext[WorkerMsg], exc: Exception) -> None:
        print(f"  [{ctx.self.address.path}] Restarting after: {exc}")

    return Behaviors.with_lifecycle(
        Behaviors.supervise(worker(), selective_strategy()),
        pre_start=on_start,
        post_stop=on_stop,
        pre_restart=on_restart,
    )


# ── Supervision tree ─────────────────────────────────────────────────


@dataclass(frozen=True)
class DelegateWork:
    task: str
    worker_index: int


@dataclass(frozen=True)
class GetReport:
    reply_to: ActorRef[str]


type SupervisorMsg = DelegateWork | GetReport


def team_supervisor(num_workers: int = 3) -> Behavior[SupervisorMsg]:
    async def setup(ctx: ActorContext[SupervisorMsg]) -> Behavior[SupervisorMsg]:
        workers = tuple(
            ctx.spawn(
                Behaviors.supervise(worker(), always_restart()),
                f"worker-{i}",
            )
            for i in range(num_workers)
        )
        return supervising(workers)

    return Behaviors.setup(setup)


def supervising(
    workers: tuple[ActorRef[WorkerMsg], ...],
) -> Behavior[SupervisorMsg]:
    async def receive(
        ctx: ActorContext[SupervisorMsg], msg: SupervisorMsg
    ) -> Behavior[SupervisorMsg]:
        match msg:
            case DelegateWork(task, worker_index):
                idx = worker_index % len(workers)
                workers[idx].tell(Process(task))
                return Behaviors.same()
            case GetReport(reply_to):
                reply_to.tell(f"Team has {len(workers)} workers")
                return Behaviors.same()

    return Behaviors.receive(receive)


# ── Main ─────────────────────────────────────────────────────────────


async def main() -> None:
    async with ActorSystem() as system:
        # 1. Basic supervision: restart on failure
        print("── Basic supervision ──")
        ref: ActorRef[WorkerMsg] = system.spawn(
            Behaviors.supervise(worker(), always_restart()),
            "basic-worker",
        )
        ref.tell(Process("hello"))
        ref.tell(Process("bad"))  # crashes → restarts
        ref.tell(Process("world"))  # continues after restart
        await asyncio.sleep(0.2)

        # 2. Selective strategy + lifecycle hooks
        print("\n── Selective strategy + lifecycle hooks ──")
        ref2: ActorRef[WorkerMsg] = system.spawn(
            resilient_worker(), "selective-worker"
        )
        ref2.tell(Process("good"))
        await asyncio.sleep(0.1)
        ref2.tell(Process("bad"))  # ValueError → restart
        await asyncio.sleep(0.1)
        ref2.tell(CrashOnPurpose())  # RuntimeError → stop
        await asyncio.sleep(0.2)

        # 3. Supervision tree: parent manages children
        print("\n── Supervision tree ──")
        supervisor: ActorRef[SupervisorMsg] = system.spawn(
            team_supervisor(3), "team"
        )
        supervisor.tell(DelegateWork("task-a", 0))
        supervisor.tell(DelegateWork("task-b", 1))
        supervisor.tell(DelegateWork("bad", 2))  # worker-2 crashes, restarts
        supervisor.tell(DelegateWork("task-c", 2))  # worker-2 handles after restart
        await asyncio.sleep(0.3)

        report: str = await system.ask(
            supervisor, lambda r: GetReport(reply_to=r), timeout=5.0
        )
        print(f"  {report}")


asyncio.run(main())
