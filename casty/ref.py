from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable, TYPE_CHECKING, Awaitable

from .envelope import Envelope
from .serializable import serializable
from .logger import debug as log_debug

if TYPE_CHECKING:
    from .mailbox import Mailbox
    from .protocols import System


@runtime_checkable
class ActorRef[M](Protocol):
    actor_id: str

    async def send(self, msg: M, *, sender: ActorRef[Any] | None = None) -> None: ...

    async def send_envelope(self, envelope: "Envelope[M]") -> None: ...

    async def ask(self, msg: M, timeout: float | None = None) -> Any: ...

    def __rshift__(self, msg: M) -> Awaitable[None]: ...

    def __lshift__[R](self, msg: M) -> Awaitable[R]: ...


@serializable
@dataclass
class UnresolvedActorRef:
    actor_id: str
    node_id: str

    async def resolve[M](self, system: "System") -> ActorRef[M] | None:
        return await system.actor(name=self.actor_id, node_id=self.node_id)


@dataclass
class LocalActorRef[M](ActorRef[M]):
    actor_id: str
    mailbox: "Mailbox[M]"
    _system: "System | None" = field(default=None, repr=False)
    default_timeout: float = 30.0

    async def send(self, msg: M, *, sender: "ActorRef[Any] | None" = None) -> None:
        log_debug("send", self.actor_id, msg_type=type(msg).__name__, sender=sender.actor_id if sender else None)
        envelope = Envelope(payload=msg, sender=sender)
        await self.mailbox.put(envelope)

    async def send_envelope(self, envelope: Envelope[M]) -> None:
        await self.mailbox.put(envelope)

    async def ask[R](self, msg: M, timeout: float | None = None) -> R:
        log_debug("ask", self.actor_id, msg_type=type(msg).__name__, timeout=timeout)
        if self._system is None:
            raise RuntimeError("ActorRef not bound to system")
        return await self._system.ask(self, msg, timeout or self.default_timeout)

    def __rshift__(self, msg: M) -> Awaitable[None]:
        return self.send(msg)

    def __lshift__[R](self, msg: M) -> Awaitable[R]:
        return self.ask(msg)
