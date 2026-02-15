"""Actor mailbox with configurable overflow strategies.

Provides a bounded or unbounded async message queue with three
overflow policies: drop newest, drop oldest, or backpressure.
"""

from __future__ import annotations

import asyncio
from enum import Enum, auto


class MailboxOverflowStrategy(Enum):
    """Policy applied when a bounded mailbox is full.

    Examples
    --------
    >>> from casty import MailboxOverflowStrategy
    >>> MailboxOverflowStrategy.drop_new
    <MailboxOverflowStrategy.drop_new: 1>
    """

    drop_new = auto()
    drop_oldest = auto()
    backpressure = auto()


class Mailbox[M]:
    """Async message queue for actor message delivery.

    Wraps an ``asyncio.Queue`` with configurable capacity and overflow
    handling. When no capacity is set the mailbox is unbounded.

    Parameters
    ----------
    capacity : int | None
        Maximum number of messages. ``None`` for unbounded.
    overflow : MailboxOverflowStrategy
        Policy when the mailbox is full.

    Examples
    --------
    >>> from casty import Mailbox, MailboxOverflowStrategy
    >>> mb = Mailbox[str](capacity=10, overflow=MailboxOverflowStrategy.drop_new)
    """

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
        """Enqueue a message, applying the overflow strategy if full.

        Parameters
        ----------
        msg : M
            The message to enqueue.

        Raises
        ------
        asyncio.QueueFull
            When the overflow strategy is ``backpressure`` and the
            mailbox is at capacity.

        Examples
        --------
        >>> mb = Mailbox[int](capacity=2)
        >>> mb.put(1)
        >>> mb.put(2)
        """
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
        """Enqueue a message, waiting if the mailbox is full.

        Parameters
        ----------
        msg : M
            The message to enqueue.

        Examples
        --------
        >>> mb = Mailbox[str](capacity=5)
        >>> await mb.put_async("hi")
        """
        await self._queue.put(msg)

    async def get(self) -> M:
        """Dequeue the next message, waiting if the mailbox is empty.

        Returns
        -------
        M
            The next message in the queue.

        Examples
        --------
        >>> mb = Mailbox[str]()
        >>> mb.put("hello")
        >>> msg = await mb.get()
        """
        return await self._queue.get()

    def size(self) -> int:
        """Return the number of messages currently in the mailbox.

        Returns
        -------
        int
            Current queue depth.

        Examples
        --------
        >>> mb = Mailbox[int]()
        >>> mb.put(42)
        >>> mb.size()
        1
        """
        return self._queue.qsize()

    def empty(self) -> bool:
        """Return whether the mailbox has no messages.

        Returns
        -------
        bool
            ``True`` if the mailbox is empty.

        Examples
        --------
        >>> mb = Mailbox[int]()
        >>> mb.empty()
        True
        """
        return self._queue.empty()
