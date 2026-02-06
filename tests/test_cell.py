from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any


from casty.actor import Behaviors, ReceiveBehavior
from casty.events import EventStream, ActorStarted, ActorStopped, DeadLetter
from casty.messages import Terminated
from casty.ref import ActorRef
from casty.supervision import OneForOneStrategy
from casty.transport import LocalTransport
from casty._cell import ActorCell


@dataclass(frozen=True)
class Ping:
    text: str


@dataclass(frozen=True)
class GetState:
    reply_to: ActorRef[str]


async def test_cell_processes_messages() -> None:
    received: list[str] = []

    async def handler(ctx: Any, msg: Ping) -> Any:
        received.append(msg.text)
        return Behaviors.same()

    behavior = Behaviors.receive(handler)
    event_stream = EventStream()
    transport = LocalTransport()
    cell: ActorCell[Ping] = ActorCell(
        behavior=behavior, name="test", parent=None, event_stream=event_stream,
        system_name="test", local_transport=transport,
    )
    await cell.start()

    cell.ref.tell(Ping("hello"))
    cell.ref.tell(Ping("world"))
    await asyncio.sleep(0.1)

    assert received == ["hello", "world"]
    await cell.stop()


async def test_cell_behavior_state_transition() -> None:
    results: list[int] = []

    def counter(count: int = 0) -> ReceiveBehavior[Ping]:
        async def handler(ctx: Any, msg: Ping) -> Any:
            new_count = count + 1
            results.append(new_count)
            return counter(new_count)
        return Behaviors.receive(handler)

    event_stream = EventStream()
    transport = LocalTransport()
    cell: ActorCell[Ping] = ActorCell(
        behavior=counter(), name="counter", parent=None, event_stream=event_stream,
        system_name="test", local_transport=transport,
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

    async def setup(ctx: Any) -> Any:
        nonlocal started
        started = True
        return Behaviors.receive(lambda ctx, msg: Behaviors.same())

    behavior = Behaviors.setup(setup)
    event_stream = EventStream()
    transport = LocalTransport()
    cell: ActorCell[str] = ActorCell(
        behavior=behavior, name="setup-test", parent=None, event_stream=event_stream,
        system_name="test", local_transport=transport,
    )
    await cell.start()
    await asyncio.sleep(0.05)

    assert started
    await cell.stop()


async def test_cell_stopped_behavior_stops_actor() -> None:
    async def handler(ctx: Any, msg: str) -> Any:
        return Behaviors.stopped()

    event_stream = EventStream()
    transport = LocalTransport()
    cell: ActorCell[str] = ActorCell(
        behavior=Behaviors.receive(handler),
        name="stopper",
        parent=None,
        event_stream=event_stream,
        system_name="test",
        local_transport=transport,
    )
    await cell.start()
    cell.ref.tell("stop")
    await asyncio.sleep(0.1)

    assert cell.is_stopped


async def test_cell_lifecycle_hooks() -> None:
    hooks_called: list[str] = []

    async def pre_start(ctx: Any) -> None:
        hooks_called.append("pre_start")

    async def post_stop(ctx: Any) -> None:
        hooks_called.append("post_stop")

    behavior = Behaviors.with_lifecycle(
        Behaviors.receive(lambda ctx, msg: Behaviors.same()),
        pre_start=pre_start,
        post_stop=post_stop,
    )
    event_stream = EventStream()
    transport = LocalTransport()
    cell: ActorCell[str] = ActorCell(
        behavior=behavior, name="lifecycle", parent=None, event_stream=event_stream,
        system_name="test", local_transport=transport,
    )
    await cell.start()
    await asyncio.sleep(0.05)
    assert "pre_start" in hooks_called

    await cell.stop()
    await asyncio.sleep(0.05)
    assert "post_stop" in hooks_called


async def test_cell_spawns_children() -> None:
    child_received: list[str] = []

    async def child_handler(ctx: Any, msg: str) -> Any:
        child_received.append(msg)
        return Behaviors.same()

    child_ref: ActorRef[str] | None = None

    async def parent_setup(ctx: Any) -> Any:
        nonlocal child_ref
        child_ref = ctx.spawn(Behaviors.receive(child_handler), "child")
        return Behaviors.receive(lambda ctx, msg: Behaviors.same())

    event_stream = EventStream()
    transport = LocalTransport()
    cell: ActorCell[str] = ActorCell(
        behavior=Behaviors.setup(parent_setup),
        name="parent",
        parent=None,
        event_stream=event_stream,
        system_name="test",
        local_transport=transport,
    )
    await cell.start()
    await asyncio.sleep(0.05)

    assert child_ref is not None
    child_ref.tell("hi from parent")
    await asyncio.sleep(0.05)
    assert child_received == ["hi from parent"]

    await cell.stop()


async def test_cell_parent_stop_stops_children() -> None:
    async def parent_setup(ctx: Any) -> Any:
        ctx.spawn(Behaviors.receive(lambda ctx, msg: Behaviors.same()), "child")
        return Behaviors.receive(lambda ctx, msg: Behaviors.same())

    event_stream = EventStream()
    transport = LocalTransport()
    parent = ActorCell(
        behavior=Behaviors.setup(parent_setup),
        name="parent",
        parent=None,
        event_stream=event_stream,
        system_name="test",
        local_transport=transport,
    )
    await parent.start()
    await asyncio.sleep(0.05)

    assert len(parent._children) == 1

    await parent.stop()
    await asyncio.sleep(0.05)

    for child in parent._children.values():
        assert child.is_stopped


async def test_cell_watch_receives_terminated() -> None:
    terminated_received: list[Terminated] = []

    async def watcher_handler(ctx: Any, msg: Any) -> Any:
        match msg:
            case Terminated():
                terminated_received.append(msg)
        return Behaviors.same()

    async def watched_handler(ctx: Any, msg: str) -> Any:
        return Behaviors.stopped()

    event_stream = EventStream()
    transport = LocalTransport()
    watcher = ActorCell(
        behavior=Behaviors.receive(watcher_handler),
        name="watcher",
        parent=None,
        event_stream=event_stream,
        system_name="test",
        local_transport=transport,
    )
    watched = ActorCell(
        behavior=Behaviors.receive(watched_handler),
        name="watched",
        parent=None,
        event_stream=event_stream,
        system_name="test",
        local_transport=transport,
    )
    await watcher.start()
    await watched.start()

    watcher.watch(watched)
    watched.ref.tell("die")
    await asyncio.sleep(0.1)

    assert len(terminated_received) == 1
    assert terminated_received[0].ref is watched.ref


async def test_cell_supervision_restarts_on_failure() -> None:
    call_count = 0

    async def failing_handler(ctx: Any, msg: str) -> Any:
        nonlocal call_count
        call_count += 1
        if call_count <= 1:
            raise RuntimeError("boom")
        return Behaviors.same()

    strategy = OneForOneStrategy(max_restarts=3, within=60.0)
    behavior = Behaviors.supervise(Behaviors.receive(failing_handler), strategy)

    event_stream = EventStream()
    transport = LocalTransport()
    cell: ActorCell[str] = ActorCell(
        behavior=behavior, name="restartable", parent=None, event_stream=event_stream,
        system_name="test", local_transport=transport,
    )
    await cell.start()

    cell.ref.tell("trigger")
    await asyncio.sleep(0.2)

    assert not cell.is_stopped
    await cell.stop()


async def test_cell_publishes_events_to_stream() -> None:
    events: list[Any] = []
    event_stream = EventStream()
    event_stream.subscribe(ActorStarted, lambda e: events.append(e))
    event_stream.subscribe(ActorStopped, lambda e: events.append(e))

    transport = LocalTransport()
    cell: ActorCell[str] = ActorCell(
        behavior=Behaviors.receive(lambda ctx, msg: Behaviors.same()),
        name="observable",
        parent=None,
        event_stream=event_stream,
        system_name="test",
        local_transport=transport,
    )
    await cell.start()
    await asyncio.sleep(0.05)
    await cell.stop()
    await asyncio.sleep(0.05)

    types = [type(e) for e in events]
    assert ActorStarted in types
    assert ActorStopped in types


async def test_cell_dead_letter_on_tell_after_stop() -> None:
    dead: list[DeadLetter] = []
    event_stream = EventStream()
    event_stream.subscribe(DeadLetter, lambda e: dead.append(e))

    transport = LocalTransport()
    cell: ActorCell[str] = ActorCell(
        behavior=Behaviors.receive(lambda ctx, msg: Behaviors.same()),
        name="dead",
        parent=None,
        event_stream=event_stream,
        system_name="test",
        local_transport=transport,
    )
    await cell.start()
    await asyncio.sleep(0.05)

    await cell.stop()
    await asyncio.sleep(0.05)

    cell.ref.tell("after-death")
    await asyncio.sleep(0.05)

    assert len(dead) == 1
    assert dead[0].message == "after-death"
