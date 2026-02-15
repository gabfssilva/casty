from __future__ import annotations

import asyncio
from dataclasses import dataclass

from typing import Any

from casty import (
    ActorContext,
    ActorRef,
    Behavior,
    Behaviors,
    ActorSystem,
    OneForOneStrategy,
    SpyEvent,
    Terminated,
)


@dataclass(frozen=True)
class Ping:
    reply_to: ActorRef[str]


@dataclass(frozen=True)
class Stop:
    pass


type EchoMsg = Ping | Stop


def echo_behavior() -> Behavior[EchoMsg]:
    async def receive(ctx: ActorContext[EchoMsg], msg: EchoMsg) -> Behavior[EchoMsg]:
        match msg:
            case Ping(reply_to=reply_to):
                reply_to.tell("pong")
                return Behaviors.same()
            case Stop():
                return Behaviors.stopped()
            case _:
                return Behaviors.unhandled()

    return Behaviors.receive(receive)


def collector_behavior(
    collected: tuple[SpyEvent[EchoMsg], ...] = (),
) -> Behavior[SpyEvent[EchoMsg]]:
    async def receive(
        ctx: ActorContext[SpyEvent[EchoMsg]], msg: SpyEvent[EchoMsg]
    ) -> Behavior[SpyEvent[EchoMsg]]:
        return collector_behavior((*collected, msg))

    return Behaviors.receive(receive)


@dataclass(frozen=True)
class GetCollected:
    reply_to: ActorRef[tuple[SpyEvent[EchoMsg], ...]]


type CollectorMsg = SpyEvent[EchoMsg] | GetCollected


def queryable_collector(
    collected: tuple[SpyEvent[EchoMsg], ...] = (),
) -> Behavior[CollectorMsg]:
    async def receive(
        ctx: ActorContext[CollectorMsg], msg: CollectorMsg
    ) -> Behavior[CollectorMsg]:
        match msg:
            case GetCollected(reply_to=reply_to):
                reply_to.tell(collected)
                return Behaviors.same()
            case SpyEvent() as event:
                return queryable_collector((*collected, event))
            case _:
                return Behaviors.unhandled()

    return Behaviors.receive(receive)


async def test_spy_observes_messages() -> None:
    async with ActorSystem("spy-test") as system:
        observer: ActorRef[CollectorMsg] = system.spawn(
            queryable_collector(), "observer"
        )

        spied = system.spawn(
            Behaviors.spy(echo_behavior(), observer),  # type: ignore[arg-type]
            "echo",
        )

        spied.tell(Ping(reply_to=observer))  # type: ignore[arg-type]
        spied.tell(Ping(reply_to=observer))  # type: ignore[arg-type]

        await asyncio.sleep(0.1)

        events: tuple[SpyEvent[EchoMsg], ...] = await system.ask(
            observer, lambda r: GetCollected(reply_to=r), timeout=2.0
        )

        assert len(events) == 2
        assert isinstance(events[0].event, Ping)
        assert isinstance(events[1].event, Ping)
        assert events[0].timestamp <= events[1].timestamp
        assert "echo" in events[0].actor_path


async def test_spy_reports_stop_message() -> None:
    async with ActorSystem("spy-term") as system:
        observer: ActorRef[CollectorMsg] = system.spawn(
            queryable_collector(), "observer"
        )

        spied = system.spawn(
            Behaviors.spy(echo_behavior(), observer),  # type: ignore[arg-type]
            "echo",
        )

        spied.tell(Stop())

        await asyncio.sleep(0.1)

        events: tuple[SpyEvent[EchoMsg], ...] = await system.ask(
            observer, lambda r: GetCollected(reply_to=r), timeout=2.0
        )

        spy_events = [e for e in events if isinstance(e.event, Stop)]
        assert len(spy_events) == 1


async def test_spy_is_transparent_to_replies() -> None:
    async with ActorSystem("spy-reply") as system:
        replies: list[str] = []

        async def reply_collector(ctx: ActorContext[str], msg: str) -> Behavior[str]:
            replies.append(msg)
            return Behaviors.same()

        reply_ref: ActorRef[str] = system.spawn(
            Behaviors.receive(reply_collector), "replies"
        )

        observer: ActorRef[CollectorMsg] = system.spawn(
            queryable_collector(), "observer"
        )

        spied = system.spawn(
            Behaviors.spy(echo_behavior(), observer),  # type: ignore[arg-type]
            "echo",
        )

        spied.tell(Ping(reply_to=reply_ref))  # type: ignore[arg-type]

        await asyncio.sleep(0.1)

        assert replies == ["pong"]


@dataclass(frozen=True)
class SelfTell:
    value: int


@dataclass(frozen=True)
class Trigger:
    pass


type SelfTellMsg = SelfTell | Trigger


def self_tell_behavior() -> Behavior[SelfTellMsg]:
    async def receive(
        ctx: ActorContext[SelfTellMsg], msg: SelfTellMsg
    ) -> Behavior[SelfTellMsg]:
        match msg:
            case Trigger():
                ctx.self.tell(SelfTell(value=42))
                return Behaviors.same()
            case SelfTell():
                return Behaviors.same()
            case _:
                return Behaviors.unhandled()

    return Behaviors.receive(receive)


@dataclass(frozen=True)
class GetSelfTellCollected:
    reply_to: ActorRef[tuple[SpyEvent[SelfTellMsg], ...]]


type SelfTellCollectorMsg = SpyEvent[SelfTellMsg] | GetSelfTellCollected


def self_tell_collector(
    collected: tuple[SpyEvent[SelfTellMsg], ...] = (),
) -> Behavior[SelfTellCollectorMsg]:
    async def receive(
        ctx: ActorContext[SelfTellCollectorMsg], msg: SelfTellCollectorMsg
    ) -> Behavior[SelfTellCollectorMsg]:
        match msg:
            case GetSelfTellCollected(reply_to=reply_to):
                reply_to.tell(collected)
                return Behaviors.same()
            case SpyEvent() as event:
                return self_tell_collector((*collected, event))
            case _:
                return Behaviors.unhandled()

    return Behaviors.receive(receive)


async def test_spy_captures_self_tell() -> None:
    async with ActorSystem("spy-self") as system:
        observer: ActorRef[SelfTellCollectorMsg] = system.spawn(
            self_tell_collector(), "observer"
        )

        spied = system.spawn(
            Behaviors.spy(self_tell_behavior(), observer),  # type: ignore[arg-type]
            "self-teller",
        )

        spied.tell(Trigger())

        await asyncio.sleep(0.1)

        events: tuple[SpyEvent[SelfTellMsg], ...] = await system.ask(
            observer, lambda r: GetSelfTellCollected(reply_to=r), timeout=2.0
        )

        assert len(events) == 2
        assert isinstance(events[0].event, Trigger)
        assert isinstance(events[1].event, SelfTell)
        assert events[1].event.value == 42


async def test_spy_with_supervision() -> None:
    call_count = 0

    def flaky_behavior() -> Behavior[str]:
        async def receive(ctx: ActorContext[str], msg: str) -> Behavior[str]:
            nonlocal call_count
            call_count += 1
            if msg == "fail":
                raise RuntimeError("boom")
            return Behaviors.same()

        return Behaviors.receive(receive)

    async with ActorSystem("spy-sup") as system:
        observer: ActorRef[SpyEvent[str]] = system.spawn(
            collector_behavior(), "observer"
        )

        supervised = Behaviors.supervise(
            flaky_behavior(),
            OneForOneStrategy(max_restarts=3, within=60.0),
        )

        spied = system.spawn(
            Behaviors.spy(supervised, observer),
            "flaky",
        )

        spied.tell("hello")
        spied.tell("fail")
        spied.tell("world")

        await asyncio.sleep(0.3)

        assert call_count >= 2


@dataclass(frozen=True)
class SpawnChild:
    pass


@dataclass(frozen=True)
class ChildMsg:
    value: int


@dataclass(frozen=True)
class GetChildRef:
    reply_to: ActorRef[ActorRef[ChildMsg]]


type ParentMsg = SpawnChild | GetChildRef


def child_behavior() -> Behavior[ChildMsg]:
    async def receive(ctx: ActorContext[ChildMsg], msg: ChildMsg) -> Behavior[ChildMsg]:
        return Behaviors.same()

    return Behaviors.receive(receive)


def parent_behavior() -> Behavior[ParentMsg]:
    def active(child_ref: ActorRef[ChildMsg] | None = None) -> Behavior[ParentMsg]:
        async def receive(
            ctx: ActorContext[ParentMsg], msg: ParentMsg
        ) -> Behavior[ParentMsg]:
            match msg:
                case SpawnChild():
                    ref = ctx.spawn(child_behavior(), "child")
                    return active(ref)
                case GetChildRef(reply_to=reply_to):
                    if child_ref is not None:
                        reply_to.tell(child_ref)
                    return Behaviors.same()
                case _:
                    return Behaviors.unhandled()

        return Behaviors.receive(receive)

    return active()


@dataclass(frozen=True)
class GetAnyCollected:
    reply_to: ActorRef[tuple[SpyEvent[Any], ...]]


type AnyCollectorMsg = SpyEvent[Any] | GetAnyCollected


def any_collector(
    collected: tuple[SpyEvent[Any], ...] = (),
) -> Behavior[AnyCollectorMsg]:
    async def receive(
        ctx: ActorContext[AnyCollectorMsg], msg: AnyCollectorMsg
    ) -> Behavior[AnyCollectorMsg]:
        match msg:
            case GetAnyCollected(reply_to=reply_to):
                reply_to.tell(collected)
                return Behaviors.same()
            case SpyEvent() as event:
                return any_collector((*collected, event))
            case _:
                return Behaviors.unhandled()

    return Behaviors.receive(receive)


async def test_spy_children_observes_child_messages() -> None:
    async with ActorSystem("spy-children") as system:
        observer: ActorRef[AnyCollectorMsg] = system.spawn(any_collector(), "observer")

        spied = system.spawn(
            Behaviors.spy(parent_behavior(), observer, spy_children=True),  # type: ignore[arg-type]
            "parent",
        )

        spied.tell(SpawnChild())
        await asyncio.sleep(0.1)

        child_ref: ActorRef[ChildMsg] = await system.ask(
            spied,
            lambda r: GetChildRef(reply_to=r),
            timeout=2.0,  # type: ignore[arg-type]
        )

        child_ref.tell(ChildMsg(value=42))
        await asyncio.sleep(0.1)

        events: tuple[SpyEvent[Any], ...] = await system.ask(
            observer, lambda r: GetAnyCollected(reply_to=r), timeout=2.0
        )

        parent_events = [e for e in events if "parent" == e.actor_path.split("/")[0]]
        child_events = [e for e in events if "parent/child" in e.actor_path]

        assert len(parent_events) >= 1
        assert len(child_events) >= 1
        assert any(
            isinstance(e.event, ChildMsg) and e.event.value == 42 for e in child_events
        )


async def test_spy_children_false_does_not_spy_children() -> None:
    async with ActorSystem("spy-no-children") as system:
        observer: ActorRef[AnyCollectorMsg] = system.spawn(any_collector(), "observer")

        spied = system.spawn(
            Behaviors.spy(parent_behavior(), observer),  # type: ignore[arg-type]
            "parent",
        )

        spied.tell(SpawnChild())
        await asyncio.sleep(0.1)

        child_ref: ActorRef[ChildMsg] = await system.ask(
            spied,
            lambda r: GetChildRef(reply_to=r),
            timeout=2.0,  # type: ignore[arg-type]
        )

        child_ref.tell(ChildMsg(value=99))
        await asyncio.sleep(0.1)

        events: tuple[SpyEvent[Any], ...] = await system.ask(
            observer, lambda r: GetAnyCollected(reply_to=r), timeout=2.0
        )

        child_events = [e for e in events if "parent/child" in e.actor_path]
        assert len(child_events) == 0


@dataclass(frozen=True)
class Metric:
    instance_id: str
    name: str
    value: float


@dataclass(frozen=True)
class StartMonitor:
    pass


@dataclass(frozen=True)
class _Tick:
    metric: Metric


def lifecycle_child(parent_ref: ActorRef[Any]) -> Behavior[_Tick]:
    """Child with with_lifecycle — mirrors Skyward's instance_monitor."""

    async def _setup(ctx: ActorContext[_Tick]) -> Behavior[_Tick]:
        return Behaviors.with_lifecycle(
            streaming(count=0),
            post_stop=lambda _: asyncio.sleep(0),
        )

    def streaming(count: int) -> Behavior[_Tick]:
        async def receive(ctx: ActorContext[_Tick], msg: _Tick) -> Behavior[_Tick]:
            match msg:
                case _Tick(metric=metric):
                    parent_ref.tell(metric)
                    return streaming(count + 1)
            return Behaviors.same()

        return Behaviors.receive(receive)

    return Behaviors.setup(_setup)


def lifecycle_parent() -> Behavior[Any]:
    """Parent that spawns lifecycle child — mirrors Skyward's pool + provider."""

    def idle() -> Behavior[Any]:
        async def receive(ctx: ActorContext[Any], msg: Any) -> Behavior[Any]:
            match msg:
                case StartMonitor():
                    child = ctx.spawn(lifecycle_child(ctx.self), "monitor")
                    return active(child)
                case _:
                    return Behaviors.same()

        return Behaviors.receive(receive)

    def active(monitor_ref: ActorRef[_Tick]) -> Behavior[Any]:
        async def receive(ctx: ActorContext[Any], msg: Any) -> Behavior[Any]:
            return Behaviors.same()

        return Behaviors.receive(receive)

    return idle()


async def test_spy_children_survives_with_lifecycle() -> None:
    """Spy must survive when a child uses Behaviors.with_lifecycle().

    Replicates the Skyward scenario where instance_monitor (child with
    lifecycle) sends Metric events to pool_ref (spied parent). Both the
    parent spy and the child spy must remain active.
    """
    async with ActorSystem("spy-lifecycle") as system:
        observer: ActorRef[AnyCollectorMsg] = system.spawn(any_collector(), "observer")

        parent_ref = system.spawn(
            Behaviors.spy(lifecycle_parent(), observer, spy_children=True),  # type: ignore[arg-type]
            "parent",
        )

        parent_ref.tell(StartMonitor())
        await asyncio.sleep(0.1)

        events: tuple[SpyEvent[Any], ...] = await system.ask(
            observer, lambda r: GetAnyCollected(reply_to=r), timeout=2.0
        )
        parent_start_events = [e for e in events if e.actor_path == "parent"]
        assert len(parent_start_events) >= 1, "Parent spy must capture StartMonitor"

        # Look up the monitor ref via system
        monitor_ref = system.lookup("/parent/monitor")
        assert monitor_ref is not None, "Monitor child must exist"

        for i in range(5):
            monitor_ref.tell(_Tick(Metric("inst-1", "cpu", float(i * 20))))
        await asyncio.sleep(0.3)

        events = await system.ask(
            observer, lambda r: GetAnyCollected(reply_to=r), timeout=2.0
        )

        # Child spy: _Tick messages processed by the monitor
        child_tick_events = [
            e
            for e in events
            if "parent/monitor" in e.actor_path and isinstance(e.event, _Tick)
        ]

        # Parent spy: Metric messages forwarded by monitor to parent
        parent_metric_events = [
            e
            for e in events
            if e.actor_path == "parent" and isinstance(e.event, Metric)
        ]

        assert len(child_tick_events) == 5, (
            f"Child spy must capture all 5 _Tick messages, got {len(child_tick_events)}"
        )
        assert len(parent_metric_events) == 5, (
            f"Parent spy must capture all 5 Metric messages, got {len(parent_metric_events)}"
        )


@dataclass(frozen=True)
class KillChild:
    pass


type WatcherMsg = SpawnChild | KillChild | Terminated


def watcher_parent() -> Behavior[WatcherMsg]:
    def active(child_ref: ActorRef[ChildMsg] | None = None) -> Behavior[WatcherMsg]:
        async def receive(
            ctx: ActorContext[WatcherMsg], msg: WatcherMsg
        ) -> Behavior[WatcherMsg]:
            match msg:
                case SpawnChild():
                    ref = ctx.spawn(child_behavior(), "watched-child")
                    ctx.watch(ref)
                    return active(ref)
                case KillChild():
                    if child_ref is not None:
                        ctx.stop(child_ref)
                    return Behaviors.same()
                case Terminated():
                    return Behaviors.same()
                case _:
                    return Behaviors.unhandled()

        return Behaviors.receive(receive)

    return active()


async def test_spy_captures_terminated_from_watched_child() -> None:
    async with ActorSystem("spy-watch") as system:
        observer: ActorRef[AnyCollectorMsg] = system.spawn(any_collector(), "observer")

        spied = system.spawn(
            Behaviors.spy(watcher_parent(), observer),  # type: ignore[arg-type]
            "watcher",
        )

        spied.tell(SpawnChild())
        await asyncio.sleep(0.1)

        spied.tell(KillChild())
        await asyncio.sleep(0.1)

        events: tuple[SpyEvent[Any], ...] = await system.ask(
            observer, lambda r: GetAnyCollected(reply_to=r), timeout=2.0
        )

        watcher_events = [e for e in events if e.actor_path == "watcher"]
        terminated_events = [
            e for e in watcher_events if isinstance(e.event, Terminated)
        ]

        assert len(terminated_events) == 1
