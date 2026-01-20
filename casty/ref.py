from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable, TYPE_CHECKING, Awaitable

from .envelope import Envelope

if TYPE_CHECKING:
    from .mailbox import Mailbox


@runtime_checkable
class ActorRef[M](Protocol):
    actor_id: str

    async def send(self, msg: M) -> None: ...

    async def send_envelope(self, envelope: "Envelope[M]") -> None: ...

    async def ask(self, msg: M, timeout: float | None = None) -> Any: ...

    def __rshift__(self, msg: M) -> Awaitable[None]: ...

    def __lshift__[R](self, msg: M) -> Awaitable[R]: ...


@dataclass
class LocalActorRef[M](ActorRef[M]):
    actor_id: str
    mailbox: "Mailbox[M]"
    default_timeout: float = 30.0

    async def send(self, msg: M, *, sender: str | None = None) -> None:
        envelope = Envelope(payload=msg, sender=sender)
        await self.mailbox.put(envelope)

    async def send_envelope(self, envelope: Envelope[M]) -> None:
        await self.mailbox.put(envelope)

    async def ask[R](self, msg: M, timeout: float | None = None) -> R:
        future: asyncio.Future[Any] = asyncio.Future()
        envelope = Envelope(payload=msg, reply_to=future)
        await self.mailbox.put(envelope)

        actual_timeout = timeout if timeout is not None else self.default_timeout
        return await asyncio.wait_for(future, actual_timeout)

    def __rshift__(self, msg: M) -> Awaitable[None]:
        return self.send(msg)

    def __lshift__[R](self, msg: M) -> Awaitable[R]:
        return self.ask(msg)
