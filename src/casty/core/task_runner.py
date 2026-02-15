"""Centralized fire-and-forget task runner actor.

Tracks asyncio.Task objects spawned via RunTask messages,
automatically cancelling all remaining tasks when the actor stops.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any, TYPE_CHECKING

from casty.core.behavior import Behavior
from casty.behaviors import Behaviors

if TYPE_CHECKING:
    from casty.core.context import ActorContext
    from casty.core.ref import ActorRef


@dataclass(frozen=True)
class TaskCompleted:
    key: str


@dataclass(frozen=True)
class TaskFailed:
    key: str
    exception: Exception


@dataclass(frozen=True)
class TaskCancelled:
    key: str


type TaskResult = TaskCompleted | TaskFailed | TaskCancelled


@dataclass(frozen=True)
class RunTask:
    fn: Callable[..., Awaitable[Any]]
    args: tuple[Any, ...] = ()
    kwargs: tuple[tuple[str, Any], ...] = ()
    reply_to: ActorRef[TaskResult] | None = field(default=None)
    key: str = field(default="")


type TaskRunnerMsg = RunTask


def task_runner() -> Behavior[TaskRunnerMsg]:
    """Create a task runner behavior that tracks fire-and-forget tasks."""

    async def setup(ctx: ActorContext[TaskRunnerMsg]) -> Behavior[TaskRunnerMsg]:
        tasks: set[asyncio.Task[None]] = set()

        async def receive(
            ctx: ActorContext[TaskRunnerMsg], msg: TaskRunnerMsg,
        ) -> Behavior[TaskRunnerMsg]:
            match msg:
                case RunTask(fn=fn, args=args, kwargs=kwargs, reply_to=reply_to, key=key):

                    async def tracked() -> None:
                        try:
                            await fn(*args, **dict(kwargs))
                            if reply_to is not None:
                                reply_to.tell(TaskCompleted(key=key))
                        except asyncio.CancelledError:
                            if reply_to is not None:
                                reply_to.tell(TaskCancelled(key=key))
                            raise
                        except Exception as exc:
                            if reply_to is not None:
                                reply_to.tell(TaskFailed(key=key, exception=exc))

                    task = asyncio.get_running_loop().create_task(tracked())
                    tasks.add(task)
                    task.add_done_callback(tasks.discard)
                    return Behaviors.same()

        async def cancel_all(ctx: ActorContext[TaskRunnerMsg]) -> None:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

        return Behaviors.with_lifecycle(
            Behaviors.receive(receive),
            post_stop=cancel_all,
        )

    return Behaviors.setup(setup)
