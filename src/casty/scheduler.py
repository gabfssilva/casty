"""Scheduler actor for periodic and one-shot timed messages.

Provides ``ScheduleTick`` (repeating), ``ScheduleOnce`` (delayed), and
``CancelSchedule`` messages, plus the ``scheduler()`` behavior factory.
"""

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
    """Schedule a message to be sent repeatedly at a fixed interval.

    If a schedule with the same ``key`` already exists, it is cancelled
    and replaced.

    Parameters
    ----------
    key : str
        Unique identifier for this schedule (used to cancel later).
    target : ActorRef[Any]
        The actor that will receive the message.
    message : Any
        The message to send on each tick.
    interval : float
        Seconds between each delivery.

    Examples
    --------
    >>> from casty import ScheduleTick
    >>> tick = ScheduleTick(key="heartbeat", target=ref, message="ping", interval=1.0)
    >>> scheduler_ref.tell(tick)
    """

    key: str
    target: ActorRef[Any]
    message: Any
    interval: float


@dataclass(frozen=True)
class ScheduleOnce:
    """Schedule a message to be sent once after a delay.

    If a schedule with the same ``key`` already exists, it is cancelled
    and replaced.

    Parameters
    ----------
    key : str
        Unique identifier for this schedule (used to cancel later).
    target : ActorRef[Any]
        The actor that will receive the message.
    message : Any
        The message to send after the delay.
    delay : float
        Seconds to wait before delivery.

    Examples
    --------
    >>> from casty import ScheduleOnce
    >>> once = ScheduleOnce(key="timeout", target=ref, message="expired", delay=5.0)
    >>> scheduler_ref.tell(once)
    """

    key: str
    target: ActorRef[Any]
    message: Any
    delay: float


@dataclass(frozen=True)
class CancelSchedule:
    """Cancel a previously registered schedule by key.

    No-op if the key does not exist.

    Parameters
    ----------
    key : str
        The key of the schedule to cancel.

    Examples
    --------
    >>> from casty import CancelSchedule
    >>> scheduler_ref.tell(CancelSchedule(key="heartbeat"))
    """

    key: str


type SchedulerMsg = ScheduleTick | ScheduleOnce | CancelSchedule
"""Union of all messages accepted by the scheduler actor."""


def scheduler() -> Behavior[SchedulerMsg]:
    """Create a scheduler actor behavior.

    The scheduler manages named timers that send messages to target actors.
    All running timers are automatically cancelled when the actor stops.

    Returns
    -------
    Behavior[SchedulerMsg]
        A behavior ready to be spawned.

    Examples
    --------
    >>> from casty import ActorSystem
    >>> from casty.scheduler import scheduler, ScheduleTick
    >>> ref = system.spawn(scheduler(), "scheduler")
    >>> ref.tell(ScheduleTick(key="tick", target=worker, message="go", interval=1.0))
    """

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
