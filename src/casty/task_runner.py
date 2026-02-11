"""Centralized fire-and-forget task runner actor.

Tracks ``asyncio.Task`` objects spawned via ``RunTask`` messages,
automatically cleaning up on completion and cancelling all remaining
tasks when the actor stops.  Spawned by ``ActorSystem`` as
``_task_runner`` so every cell can offload coroutines without orphaning
tasks.
"""

from __future__ import annotations

import asyncio
from collections.abc import Coroutine
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from casty.actor import Behavior, Behaviors

if TYPE_CHECKING:
    from casty.context import ActorContext
    from casty.ref import ActorRef


@dataclass(frozen=True)
class TaskCompleted:
    """Notification that a tracked task finished successfully."""

    key: str


@dataclass(frozen=True)
class TaskFailed:
    """Notification that a tracked task raised an exception."""

    key: str
    exception: Exception


@dataclass(frozen=True)
class TaskCancelled:
    """Notification that a tracked task was cancelled."""

    key: str


type TaskResult = TaskCompleted | TaskFailed | TaskCancelled


@dataclass(frozen=True)
class RunTask:
    """Request the task runner to execute a coroutine as a tracked task.

    Parameters
    ----------
    coro : Coroutine[Any, Any, None]
        The coroutine to run.
    reply_to : ActorRef[TaskResult] | None
        Optional ref to notify on completion, failure, or cancellation.
    key : str
        Caller-chosen identifier echoed back in the result message.

    Examples
    --------
    >>> task_runner.tell(RunTask(some_async_work()))
    >>> task_runner.tell(RunTask(work(), reply_to=ctx.self, key="job-1"))
    """

    coro: Coroutine[Any, Any, None]
    reply_to: ActorRef[TaskResult] | None = field(default=None)
    key: str = field(default="")


type TaskRunnerMsg = RunTask


def task_runner() -> Behavior[TaskRunnerMsg]:
    """Create a task runner behavior that tracks fire-and-forget tasks.

    All running tasks are automatically cancelled when the actor stops.

    Returns
    -------
    Behavior[TaskRunnerMsg]
        A behavior ready to be spawned by the actor system.
    """

    async def setup(ctx: ActorContext[TaskRunnerMsg]) -> Behavior[TaskRunnerMsg]:
        tasks: set[asyncio.Task[None]] = set()

        async def receive(
            ctx: ActorContext[TaskRunnerMsg], msg: TaskRunnerMsg
        ) -> Behavior[TaskRunnerMsg]:
            match msg:
                case RunTask(coro=coro, reply_to=reply_to, key=key):

                    async def tracked() -> None:
                        try:
                            await coro
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
