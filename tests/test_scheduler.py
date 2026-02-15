from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

from casty import ActorRef, Behaviors, ActorSystem, Behavior
from casty.core.scheduler import (
    CancelSchedule,
    ScheduleOnce,
    SchedulerMsg,
    ScheduleTick,
    scheduler,
)


@dataclass(frozen=True)
class Ping:
    seq: int


def collector(results: list[Any]) -> Behavior[Any]:
    async def receive(ctx: Any, msg: Any) -> Any:
        results.append(msg)
        return Behaviors.same()

    return Behaviors.receive(receive)


async def test_tick_delivers_periodically() -> None:
    results: list[Any] = []
    async with ActorSystem() as system:
        target = system.spawn(collector(results), "target")
        sched = system.spawn(scheduler(), "scheduler")
        sched.tell(
            ScheduleTick(key="ping", target=target, message=Ping(1), interval=0.05)
        )
        await asyncio.sleep(0.3)

    assert len(results) >= 3
    assert all(isinstance(r, Ping) for r in results)


async def test_once_delivers_after_delay() -> None:
    results: list[Any] = []
    async with ActorSystem() as system:
        target = system.spawn(collector(results), "target")
        sched = system.spawn(scheduler(), "scheduler")
        sched.tell(ScheduleOnce(key="ping", target=target, message=Ping(42), delay=0.1))
        await asyncio.sleep(0.05)
        assert len(results) == 0
        await asyncio.sleep(0.15)

    assert results == [Ping(42)]


async def test_cancel_stops_delivery() -> None:
    results: list[Any] = []
    async with ActorSystem() as system:
        target = system.spawn(collector(results), "target")
        sched = system.spawn(scheduler(), "scheduler")
        sched.tell(
            ScheduleTick(key="ping", target=target, message=Ping(1), interval=0.05)
        )
        await asyncio.sleep(0.15)
        count_before = len(results)
        assert count_before >= 1

        sched.tell(CancelSchedule(key="ping"))
        await asyncio.sleep(0.05)
        count_after = len(results)
        await asyncio.sleep(0.15)
        assert len(results) == count_after


async def test_same_key_replaces_schedule() -> None:
    results: list[Any] = []
    async with ActorSystem() as system:
        target = system.spawn(collector(results), "target")
        sched = system.spawn(scheduler(), "scheduler")
        sched.tell(ScheduleTick(key="a", target=target, message=Ping(1), interval=0.05))
        await asyncio.sleep(0.15)
        sched.tell(ScheduleTick(key="a", target=target, message=Ping(2), interval=0.05))
        await asyncio.sleep(0.01)
        results.clear()
        await asyncio.sleep(0.15)

    assert all(r == Ping(2) for r in results)
    assert len(results) >= 1


async def test_post_stop_cancels_all() -> None:
    results: list[Any] = []
    async with ActorSystem() as system:
        target = system.spawn(collector(results), "target")
        sched = system.spawn(scheduler(), "scheduler")
        sched.tell(ScheduleTick(key="a", target=target, message=Ping(1), interval=0.05))
        sched.tell(ScheduleTick(key="b", target=target, message=Ping(2), interval=0.05))
        await asyncio.sleep(0.15)

    count_at_stop = len(results)
    await asyncio.sleep(0.15)
    assert len(results) == count_at_stop


async def test_reschedule_pattern() -> None:
    attempts: list[int] = []

    def retry_actor(sched: ActorRef[SchedulerMsg]) -> Behavior[Any]:
        async def receive(ctx: Any, msg: Any) -> Any:
            match msg:
                case Ping(seq=seq):
                    attempts.append(seq)
                    if seq < 3:
                        sched.tell(
                            ScheduleOnce(
                                key="retry",
                                target=ctx.self,
                                message=Ping(seq + 1),
                                delay=0.05,
                            )
                        )
            return Behaviors.same()

        return Behaviors.receive(receive)

    async with ActorSystem() as system:
        sched = system.spawn(scheduler(), "scheduler")
        retrier = system.spawn(retry_actor(sched), "retrier")
        sched.tell(
            ScheduleOnce(key="retry", target=retrier, message=Ping(1), delay=0.05)
        )
        await asyncio.sleep(0.5)

    assert attempts == [1, 2, 3]
