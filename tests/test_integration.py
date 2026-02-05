# tests/test_integration.py
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

import pytest

from casty import (
    ActorContext,
    ActorRef,
    ActorSystem,
    Behavior,
    Behaviors,
    DeadLetter,
    Directive,
    Mailbox,
    MailboxOverflowStrategy,
    OneForOneStrategy,
    Terminated,
)


# --- Messages ---

@dataclass(frozen=True)
class Greet:
    name: str


@dataclass(frozen=True)
class GetCount:
    reply_to: ActorRef[int]


type GreeterMsg = Greet | GetCount


@dataclass(frozen=True)
class WorkItem:
    data: str


@dataclass(frozen=True)
class GetResults:
    reply_to: ActorRef[list[str]]


type WorkerMsg = WorkItem | GetResults


# --- Behaviors ---

def greeter(count: int = 0) -> Behavior[GreeterMsg]:
    async def receive(ctx: Any, msg: GreeterMsg) -> Any:
        match msg:
            case Greet(name):
                return greeter(count + 1)
            case GetCount(reply_to):
                reply_to.tell(count)
                return Behaviors.same()
            case _:
                return Behaviors.unhandled()

    return Behaviors.receive(receive)


# --- Integration Tests ---

async def test_full_lifecycle_greeter() -> None:
    """Spawn a greeter, send messages, ask for count, shutdown."""
    async with ActorSystem(name="test") as system:
        ref = system.spawn(greeter(), "greeter")

        ref.tell(Greet("Alice"))
        ref.tell(Greet("Bob"))
        ref.tell(Greet("Charlie"))
        await asyncio.sleep(0.1)

        count = await system.ask(ref, lambda r: GetCount(reply_to=r), timeout=5.0)
        assert count == 3


async def test_parent_child_hierarchy() -> None:
    """Parent spawns children, children process messages independently."""
    child_results: list[str] = []

    def worker() -> Behavior[WorkItem]:
        async def receive(ctx: Any, msg: WorkItem) -> Any:
            child_results.append(msg.data)
            return Behaviors.same()
        return Behaviors.receive(receive)

    @dataclass(frozen=True)
    class SpawnWorker:
        name: str

    @dataclass(frozen=True)
    class DispatchWork:
        worker_name: str
        data: str

    type BossMsg = SpawnWorker | DispatchWork

    def boss() -> Behavior[BossMsg]:
        workers: dict[str, ActorRef[WorkItem]] = {}

        async def setup(ctx: Any) -> Any:
            async def receive(ctx: Any, msg: BossMsg) -> Any:
                match msg:
                    case SpawnWorker(name):
                        workers[name] = ctx.spawn(worker(), name)
                        return Behaviors.same()
                    case DispatchWork(worker_name, data):
                        if worker_name in workers:
                            workers[worker_name].tell(WorkItem(data))
                        return Behaviors.same()
                    case _:
                        return Behaviors.unhandled()
            return Behaviors.receive(receive)
        return Behaviors.setup(setup)

    async with ActorSystem() as system:
        ref = system.spawn(boss(), "boss")
        ref.tell(SpawnWorker("w1"))
        ref.tell(SpawnWorker("w2"))
        await asyncio.sleep(0.1)

        ref.tell(DispatchWork("w1", "task-a"))
        ref.tell(DispatchWork("w2", "task-b"))
        await asyncio.sleep(0.1)

    assert sorted(child_results) == ["task-a", "task-b"]


async def test_supervision_restart_on_failure() -> None:
    """Actor fails, gets restarted by supervision, continues processing."""
    processed: list[str] = []
    fail_once = True

    def fragile_worker() -> Behavior[str]:
        async def receive(ctx: Any, msg: str) -> Any:
            nonlocal fail_once
            if msg == "fail" and fail_once:
                fail_once = False
                raise RuntimeError("boom")
            processed.append(msg)
            return Behaviors.same()
        return Behaviors.receive(receive)

    strategy = OneForOneStrategy(
        max_restarts=3,
        within=60.0,
        decider=lambda exc: Directive.restart,
    )

    async with ActorSystem() as system:
        ref = system.spawn(
            Behaviors.supervise(fragile_worker(), strategy),
            "worker",
        )
        ref.tell("fail")
        await asyncio.sleep(0.2)
        ref.tell("after-restart")
        await asyncio.sleep(0.1)

    assert "after-restart" in processed


async def test_lifecycle_hooks_called() -> None:
    """Lifecycle hooks fire in correct order."""
    hooks: list[str] = []

    async def pre_start(ctx: Any) -> None:
        hooks.append("pre_start")

    async def post_stop(ctx: Any) -> None:
        hooks.append("post_stop")

    behavior = Behaviors.with_lifecycle(
        Behaviors.receive(lambda ctx, msg: Behaviors.same()),
        pre_start=pre_start,
        post_stop=post_stop,
    )

    async with ActorSystem() as system:
        system.spawn(behavior, "hooked")
        await asyncio.sleep(0.1)

    assert hooks == ["pre_start", "post_stop"]


async def test_dead_letters() -> None:
    """Messages to stopped actors go to dead letters."""
    dead: list[DeadLetter] = []

    async with ActorSystem() as system:
        system.event_stream.subscribe(DeadLetter, lambda e: dead.append(e))  # type: ignore[arg-type,return-value]

        behavior = Behaviors.receive(lambda ctx, msg: Behaviors.stopped())
        ref = system.spawn(behavior, "short-lived")
        ref.tell("trigger-stop")
        await asyncio.sleep(0.1)

        ref.tell("after-death")
        await asyncio.sleep(0.1)

    assert len(dead) >= 1
    assert any(d.message == "after-death" for d in dead)


async def test_ask_timeout_raises() -> None:
    """Ask with no reply raises TimeoutError."""
    async def silent(ctx: Any, msg: Any) -> Any:
        return Behaviors.same()

    async with ActorSystem() as system:
        ref = system.spawn(Behaviors.receive(silent), "silent")
        with pytest.raises(TimeoutError):
            await system.ask(ref, lambda r: "hello", timeout=0.1)


async def test_bounded_mailbox_integration() -> None:
    """Bounded mailbox drops messages per strategy."""
    received: list[int] = []

    async def collector(ctx: Any, msg: int) -> Any:
        await asyncio.sleep(0.05)  # slow consumer
        received.append(msg)
        return Behaviors.same()

    mailbox: Mailbox[int] = Mailbox(
        capacity=2, overflow=MailboxOverflowStrategy.drop_new,
    )

    async with ActorSystem() as system:
        ref = system.spawn(Behaviors.receive(collector), "slow", mailbox=mailbox)
        for i in range(10):
            ref.tell(i)
        await asyncio.sleep(1.0)

    # Not all 10 should arrive due to bounded mailbox
    assert len(received) < 10


async def test_lookup_nested_child() -> None:
    """Lookup finds nested children by path."""

    async def leaf_setup(ctx: Any) -> Any:
        return Behaviors.receive(lambda ctx, msg: Behaviors.same())

    async def mid_setup(ctx: Any) -> Any:
        ctx.spawn(Behaviors.setup(leaf_setup), "leaf")
        return Behaviors.receive(lambda ctx, msg: Behaviors.same())

    async def root_setup(ctx: Any) -> Any:
        ctx.spawn(Behaviors.setup(mid_setup), "mid")
        return Behaviors.receive(lambda ctx, msg: Behaviors.same())

    async with ActorSystem() as system:
        system.spawn(Behaviors.setup(root_setup), "root")
        await asyncio.sleep(0.2)

        assert system.lookup("/root") is not None
        assert system.lookup("/root/mid") is not None
        assert system.lookup("/root/mid/leaf") is not None
        assert system.lookup("/root/nonexistent") is None
