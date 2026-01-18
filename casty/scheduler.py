import asyncio
import uuid
from asyncio import Task
from dataclasses import dataclass
from typing import Any

from casty import Actor, LocalActorRef, Context


@dataclass(slots=True, frozen=True)
class Schedule:
    """Schedule a message to be sent after a timeout.

    Use with ask() to get the task_id for cancellation:
        task_id = await scheduler.ask(Schedule(1.0, ref, MyMsg()))
        await scheduler.send(Cancel(task_id))  # Cancel if needed
    """
    timeout: float
    listener: LocalActorRef
    message: Any


@dataclass(slots=True, frozen=True)
class Cancel:
    """Cancel a scheduled message by task_id."""
    task_id: str


@dataclass(slots=True, frozen=True)
class _Wake:
    task_id: str
    listener: LocalActorRef
    message: Any


type SchedulerMessage = Schedule | Cancel | _Wake

class Scheduler(Actor[SchedulerMessage]):
    pending: dict[str, Task]

    def __init__(self):
        self.pending = {}

    async def receive(self, msg: SchedulerMessage, ctx: Context[SchedulerMessage]) -> None:
        match msg:
            case _Wake() as wake:
                await (wake.listener >> wake.message)
                self.pending.pop(wake.task_id, None)
            case Schedule():
                task_id = f"{uuid.uuid4()}"
                detached = ctx.detach(self._schedule_task(task_id, msg))
                self.pending[task_id] = detached.task
                await ctx.reply(task_id)
            case Cancel(task_id):
                task = self.pending.pop(task_id, None)
                if task and not task.done():
                    task.cancel()

    @staticmethod
    async def _schedule_task(task_id: str, msg: Schedule) -> _Wake:
        await asyncio.sleep(msg.timeout)
        return _Wake(task_id, msg.listener, msg.message)

    async def on_stop(self) -> None:
        for task in self.pending.values():
            task.cancel()
