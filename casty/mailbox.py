from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, AsyncIterator

from .context import Context
from .envelope import Envelope


@dataclass
class Stop:
    pass


class Mailbox[M]:
    def __init__(
        self,
        queue: asyncio.Queue[Envelope[M | Stop]],
        self_id: str = "",
        node_id: str = "local",
        is_leader: bool = True,
        system: Any = None,
        self_ref: Any = None,
    ) -> None:
        self._queue = queue
        self._self_id = self_id
        self._node_id = node_id
        self._is_leader = is_leader
        self._system = system
        self._self_ref = self_ref

    def __aiter__(self) -> AsyncIterator[tuple[M, Context]]:
        return self

    async def __anext__(self) -> tuple[M, Context]:
        envelope = await self._queue.get()

        if isinstance(envelope.payload, Stop):
            raise StopAsyncIteration

        ctx = Context(
            self_id=self._self_id,
            sender=envelope.sender,
            node_id=self._node_id,
            is_leader=self._is_leader,
            reply_to=envelope.reply_to,
            _system=self._system,
            _self_ref=self._self_ref,
        )

        return envelope.payload, ctx

    async def put(self, envelope: Envelope[M | Stop]) -> None:
        await self._queue.put(envelope)

    def set_self_ref(self, ref: Any) -> None:
        self._self_ref = ref

    async def schedule[T](
        self,
        msg: T,
        *,
        delay: float | None = None,
        every: float | None = None,
    ) -> Any:
        if self._system is None:
            raise RuntimeError("Mailbox not bound to system")
        return await self._system.schedule(msg, to=self._self_ref, delay=delay, every=every)
