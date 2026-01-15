import asyncio
import uuid
from asyncio import Task
from dataclasses import dataclass
from typing import Any

from casty import Actor, LocalRef, Context


@dataclass
class Schedule:
    timeout: float
    listener: LocalRef
    message: Any


@dataclass
class _Wake:
    task_id: str
    listener: LocalRef
    message: Any


class Scheduler(Actor[Schedule | _Wake]):
    pending: dict[str, Task]

    def __init__(self):
        self.pending = {}

    async def receive(self, msg: Schedule | _Wake, ctx: Context[Schedule | _Wake]) -> None:
        match msg:
            case _Wake():
                await (msg.listener >> msg.message)
                del self.pending[msg.task_id]
            case Schedule():
                task_id = f"{uuid.uuid4()}"
                self.pending[task_id] = asyncio.create_task(self._schedule_task(task_id, msg, ctx))

    @staticmethod
    async def _schedule_task(task_id: str, msg: Schedule, ctx: Context[Schedule | _Wake]) -> None:
        await asyncio.sleep(msg.timeout)
        await (ctx.self_ref >> _Wake(task_id, msg.listener, msg.message))

    async def on_stop(self) -> None:
        for task in self.pending.values():
            task.cancel()
