from __future__ import annotations

import asyncio
from enum import Enum, auto


class MailboxOverflowStrategy(Enum):
    drop_new = auto()
    drop_oldest = auto()
    backpressure = auto()


class Mailbox[M]:
    def __init__(
        self,
        capacity: int | None = None,
        overflow: MailboxOverflowStrategy = MailboxOverflowStrategy.drop_new,
    ) -> None:
        self._overflow = overflow
        if capacity is None:
            self._queue: asyncio.Queue[M] = asyncio.Queue()
        else:
            self._queue = asyncio.Queue(maxsize=capacity)
        self._capacity = capacity

    def put(self, msg: M) -> None:
        if self._capacity is None:
            self._queue.put_nowait(msg)
            return

        match self._overflow:
            case MailboxOverflowStrategy.drop_new:
                if not self._queue.full():
                    self._queue.put_nowait(msg)
            case MailboxOverflowStrategy.drop_oldest:
                if self._queue.full():
                    try:
                        self._queue.get_nowait()
                    except asyncio.QueueEmpty:
                        pass
                self._queue.put_nowait(msg)
            case MailboxOverflowStrategy.backpressure:
                if not self._queue.full():
                    self._queue.put_nowait(msg)
                else:
                    raise asyncio.QueueFull()

    async def put_async(self, msg: M) -> None:
        await self._queue.put(msg)

    async def get(self) -> M:
        return await self._queue.get()

    def size(self) -> int:
        return self._queue.qsize()

    def empty(self) -> bool:
        return self._queue.empty()
