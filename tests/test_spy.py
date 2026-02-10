from __future__ import annotations

import asyncio
from dataclasses import dataclass

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
        assert "/echo" in events[0].actor_path


async def test_spy_reports_terminated() -> None:
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
        terminated_events = [e for e in events if isinstance(e.event, Terminated)]

        assert len(spy_events) == 1
        assert len(terminated_events) == 1


async def test_spy_is_transparent_to_replies() -> None:
    async with ActorSystem("spy-reply") as system:
        replies: list[str] = []

        async def reply_collector(
            ctx: ActorContext[str], msg: str
        ) -> Behavior[str]:
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
