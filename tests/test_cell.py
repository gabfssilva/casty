from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

from casty.actor import Behaviors
from casty.core.actor import ActorCell
from casty.core.behavior import Behavior
from casty.core.events import ActorStarted, ActorStopped, DeadLetter
from casty.core.event_stream import Subscribe as ESSubscribe
from casty.core.messages import Terminated
from casty.core.ref import LocalActorRef
from casty.core.supervision import OneForOneStrategy
from casty.core.task_runner import TaskRunnerMsg, task_runner
from casty.core.system import ActorSystem


async def make_task_runner(
    event_stream_ref: Any = None,
) -> ActorCell[TaskRunnerMsg]:
    cell: ActorCell[TaskRunnerMsg] = ActorCell(
        behavior=task_runner(),
        id="_task_runner",
        event_stream=event_stream_ref,
    )
    await cell.start()
    return cell


@dataclass(frozen=True)
class Ping:
    text: str


@dataclass(frozen=True)
class GetState:
    reply_to: LocalActorRef[str]


async def test_cell_processes_messages() -> None:
    received: list[str] = []

    async def handler(ctx: Any, msg: Ping) -> Behavior[Any]:
        received.append(msg.text)
        return Behaviors.same()

    cell: ActorCell[Ping] = ActorCell(
        behavior=Behaviors.receive(handler), id="test",
    )
    await cell.start()

    cell.ref.tell(Ping("hello"))
    cell.ref.tell(Ping("world"))
    await asyncio.sleep(0.1)

    assert received == ["hello", "world"]
    await cell.stop()


async def test_cell_behavior_state_transition() -> None:
    results: list[int] = []

    def counter(count: int = 0) -> Behavior[Ping]:
        async def handler(ctx: Any, msg: Ping) -> Behavior[Any]:
            new_count = count + 1
            results.append(new_count)
            return counter(new_count)
        return Behaviors.receive(handler)

    cell: ActorCell[Ping] = ActorCell(
        behavior=counter(), id="counter",
    )
    await cell.start()

    cell.ref.tell(Ping("a"))
    cell.ref.tell(Ping("b"))
    cell.ref.tell(Ping("c"))
    await asyncio.sleep(0.1)

    assert results == [1, 2, 3]
    await cell.stop()


async def test_cell_setup_behavior() -> None:
    started = False

    async def setup(ctx: Any) -> Behavior[Any]:
        nonlocal started
        started = True
        return Behaviors.receive(lambda ctx, msg: Behaviors.same())

    cell: ActorCell[str] = ActorCell(
        behavior=Behaviors.setup(setup), id="setup-test",
    )
    await cell.start()
    await asyncio.sleep(0.05)

    assert started
    await cell.stop()


async def test_cell_stopped_behavior_stops_actor() -> None:
    async def handler(ctx: Any, msg: str) -> Behavior[Any]:
        return Behaviors.stopped()

    cell: ActorCell[str] = ActorCell(
        behavior=Behaviors.receive(handler), id="stopper",
    )
    await cell.start()
    cell.ref.tell("stop")
    await asyncio.sleep(0.1)

    assert cell.is_stopped


async def test_cell_lifecycle_hooks() -> None:
    hooks_called: list[str] = []

    @dataclass(frozen=True)
    class Stop:
        pass

    async def pre_start(ctx: Any) -> None:
        hooks_called.append("pre_start")

    async def post_stop(ctx: Any) -> None:
        hooks_called.append("post_stop")

    async def handler(ctx: Any, msg: Any) -> Behavior[Any]:
        match msg:
            case Stop():
                return Behaviors.stopped()
            case _:
                return Behaviors.same()

    behavior = Behaviors.with_lifecycle(
        Behaviors.receive(handler),
        pre_start=pre_start,
        post_stop=post_stop,
    )
    cell: ActorCell[Any] = ActorCell(
        behavior=behavior, id="lifecycle",
    )
    await cell.start()
    await asyncio.sleep(0.05)
    assert "pre_start" in hooks_called

    cell.ref.tell(Stop())
    await asyncio.sleep(0.05)
    assert "post_stop" in hooks_called


async def test_cell_spawns_children() -> None:
    child_received: list[str] = []

    async def child_handler(ctx: Any, msg: str) -> Behavior[Any]:
        child_received.append(msg)
        return Behaviors.same()

    child_ref_holder: list[Any] = []

    async def parent_setup(ctx: Any) -> Behavior[Any]:
        ref = ctx.spawn(Behaviors.receive(child_handler), "child")
        child_ref_holder.append(ref)
        return Behaviors.receive(lambda ctx, msg: Behaviors.same())

    tr = await make_task_runner()
    cell: ActorCell[str] = ActorCell(
        behavior=Behaviors.setup(parent_setup),
        id="parent",
        task_runner=tr.ref,
    )
    await cell.start()
    await asyncio.sleep(0.05)

    assert len(child_ref_holder) == 1
    child_ref_holder[0].tell("hi from parent")
    await asyncio.sleep(0.05)
    assert child_received == ["hi from parent"]

    await cell.stop()
    await tr.stop()


async def test_cell_parent_stop_stops_children() -> None:
    async def parent_setup(ctx: Any) -> Behavior[Any]:
        ctx.spawn(Behaviors.receive(lambda ctx, msg: Behaviors.same()), "child")
        return Behaviors.receive(lambda ctx, msg: Behaviors.same())

    tr = await make_task_runner()
    parent: ActorCell[str] = ActorCell(
        behavior=Behaviors.setup(parent_setup),
        id="parent",
        task_runner=tr.ref,
    )
    await parent.start()
    await asyncio.sleep(0.05)

    assert len(parent.children) == 1

    await parent.stop()
    await asyncio.sleep(0.05)

    for child in parent.children.values():
        assert child.is_stopped
    await tr.stop()


async def test_cell_watch_receives_terminated() -> None:
    terminated_received: list[Terminated] = []

    async def watcher_handler(ctx: Any, msg: Any) -> Behavior[Any]:
        match msg:
            case Terminated():
                terminated_received.append(msg)
        return Behaviors.same()

    async def watched_handler(ctx: Any, msg: str) -> Behavior[Any]:
        return Behaviors.stopped()

    watcher: ActorCell[Any] = ActorCell(
        behavior=Behaviors.receive(watcher_handler), id="watcher",
    )
    watched: ActorCell[str] = ActorCell(
        behavior=Behaviors.receive(watched_handler), id="watched",
    )
    await watcher.start()
    await watched.start()

    watched.watchers.add(watcher)
    watched.ref.tell("die")
    await asyncio.sleep(0.1)

    assert len(terminated_received) == 1
    assert terminated_received[0].ref is watched.ref


async def test_cell_supervision_restarts_on_failure() -> None:
    call_count = 0

    async def failing_handler(ctx: Any, msg: str) -> Behavior[Any]:
        nonlocal call_count
        call_count += 1
        if call_count <= 1:
            raise RuntimeError("boom")
        return Behaviors.same()

    strategy = OneForOneStrategy(max_restarts=3, within=60.0)
    behavior = Behaviors.supervise(Behaviors.receive(failing_handler), strategy)

    cell: ActorCell[str] = ActorCell(
        behavior=behavior, id="restartable",
    )
    await cell.start()

    cell.ref.tell("trigger")
    await asyncio.sleep(0.2)

    assert not cell.is_stopped
    await cell.stop()


async def test_cell_publishes_events_to_stream() -> None:
    events: list[Any] = []

    def collector() -> Behavior[Any]:
        async def handler(ctx: Any, msg: Any) -> Behavior[Any]:
            events.append(msg)
            return Behaviors.same()
        return Behaviors.receive(handler)

    async with ActorSystem("test") as system:
        observer = system.spawn(collector(), "observer")
        system.event_stream.tell(ESSubscribe(event_type=ActorStarted, handler=observer))
        system.event_stream.tell(ESSubscribe(event_type=ActorStopped, handler=observer))
        await asyncio.sleep(0.05)

        ref = system.spawn(
            Behaviors.receive(lambda ctx, msg: Behaviors.stopped()),
            "observable",
        )
        await asyncio.sleep(0.05)

        ref.tell("trigger-stop")
        await asyncio.sleep(0.1)

    types = [type(e) for e in events]
    assert ActorStarted in types
    assert ActorStopped in types


async def test_cell_dead_letter_on_tell_after_stop() -> None:
    dead: list[DeadLetter] = []

    def collector() -> Behavior[DeadLetter]:
        async def handler(ctx: Any, msg: DeadLetter) -> Behavior[Any]:
            dead.append(msg)
            return Behaviors.same()
        return Behaviors.receive(handler)

    async with ActorSystem("test") as system:
        observer = system.spawn(collector(), "dead-observer")
        system.event_stream.tell(ESSubscribe(event_type=DeadLetter, handler=observer))
        await asyncio.sleep(0.05)

        ref = system.spawn(
            Behaviors.receive(lambda ctx, msg: Behaviors.stopped()),
            "dead",
        )
        ref.tell("trigger-stop")
        await asyncio.sleep(0.1)

        ref.tell("after-death")
        await asyncio.sleep(0.1)

    assert len(dead) >= 1
    assert any(d.message == "after-death" for d in dead)
