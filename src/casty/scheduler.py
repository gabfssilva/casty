from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from casty.actor import Behavior, Behaviors

if TYPE_CHECKING:
    from casty.context import ActorContext
    from casty.ref import ActorRef


@dataclass(frozen=True)
class ScheduleTick:
    key: str
    target: ActorRef[Any]
    message: Any
    interval: float


@dataclass(frozen=True)
class ScheduleOnce:
    key: str
    target: ActorRef[Any]
    message: Any
    delay: float


@dataclass(frozen=True)
class CancelSchedule:
    key: str


type SchedulerMsg = ScheduleTick | ScheduleOnce | CancelSchedule


def scheduler() -> Behavior[SchedulerMsg]:
    async def setup(ctx: ActorContext[SchedulerMsg]) -> Behavior[SchedulerMsg]:
        running: dict[str, asyncio.Task[None]] = {}

        async def receive(ctx: ActorContext[SchedulerMsg], msg: SchedulerMsg) -> Behavior[SchedulerMsg]:
            match msg:
                case ScheduleTick(key=key, target=target, message=message, interval=interval):
                    if key in running:
                        running[key].cancel()

                    async def tick(t: ActorRef[Any] = target, m: Any = message, i: float = interval) -> None:
                        try:
                            while True:
                                await asyncio.sleep(i)
                                t.tell(m)
                        except asyncio.CancelledError:
                            pass

                    running[key] = asyncio.get_running_loop().create_task(tick())
                    return Behaviors.same()

                case ScheduleOnce(key=key, target=target, message=message, delay=delay):
                    if key in running:
                        running[key].cancel()

                    async def once(t: ActorRef[Any] = target, m: Any = message, d: float = delay) -> None:
                        try:
                            await asyncio.sleep(d)
                            t.tell(m)
                        except asyncio.CancelledError:
                            pass

                    running[key] = asyncio.get_running_loop().create_task(once())
                    return Behaviors.same()

                case CancelSchedule(key=key):
                    task = running.pop(key, None)
                    if task is not None:
                        task.cancel()
                    return Behaviors.same()

        async def cancel_all(ctx: ActorContext[SchedulerMsg]) -> None:
            for task in running.values():
                task.cancel()

        return Behaviors.with_lifecycle(
            Behaviors.receive(receive),
            post_stop=cancel_all,
        )

    return Behaviors.setup(setup)
