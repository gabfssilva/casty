from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from typing import Any, Awaitable, TYPE_CHECKING

if TYPE_CHECKING:
    from casty.ref import ActorRef
    from casty.envelope import Envelope
    from .serializer import Serializer


@dataclass
class SendDeliver:
    target: str
    payload: bytes
    sender_name: str | None = None


@dataclass
class SendAsk:
    target: str
    payload: bytes
    correlation_id: str


@dataclass
class RemoteRef[M]:
    actor_id: str
    name: str
    _session: "ActorRef" = field(repr=False)
    _serializer: "Serializer" = field(repr=False)
    default_timeout: float = 30.0

    async def send(self, msg: M, *, sender: "ActorRef[Any] | None" = None) -> None:
        payload = self._serializer.encode(msg)
        sender_name = sender.actor_id if sender else None
        await self._session.send(SendDeliver(
            target=self.name,
            payload=payload,
            sender_name=sender_name,
        ))

    async def send_envelope(self, envelope: "Envelope[M]") -> None:
        sender = envelope.sender
        await self.send(envelope.payload, sender=sender)

    async def ask[R](self, msg: M, timeout: float | None = None) -> R:
        payload = self._serializer.encode(msg)
        correlation_id = uuid.uuid4().hex
        response = await self._session.ask(
            SendAsk(target=self.name, payload=payload, correlation_id=correlation_id),
            timeout=timeout or self.default_timeout,
        )
        return self._serializer.decode(response)

    def __rshift__(self, msg: M) -> Awaitable[None]:
        return self.send(msg)

    def __lshift__[R](self, msg: M) -> Awaitable[R]:
        return self.ask(msg)
