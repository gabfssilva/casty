from __future__ import annotations

import asyncio
import uuid
from dataclasses import dataclass, field
from typing import Any, Awaitable, TYPE_CHECKING

from casty.envelope import Envelope
from casty.ref import ActorRef
from casty.serializable import serialize
from .transport_messages import Transmit, Connect
from .messages import GetAliveMembers

if TYPE_CHECKING:
    from casty.protocols import System


@dataclass
class RemoteActorRef[M](ActorRef[M]):
    actor_id: str
    node_id: str
    _system: "System | None" = field(default=None, repr=False)
    outbound_ref: ActorRef | None = field(default=None, repr=False)
    membership_ref: ActorRef | None = field(default=None, repr=False)
    pending: dict[str, asyncio.Future[Any]] = field(default_factory=dict, repr=False)
    default_timeout: float = 30.0

    async def _get_connection(self) -> ActorRef | None:
        if self.outbound_ref is None or self.membership_ref is None:
            return None

        members = await self.membership_ref.ask(GetAliveMembers())
        if self.node_id not in members:
            return None

        address = members[self.node_id].address
        conn = await self.outbound_ref.ask(Connect(node_id=self.node_id, address=address))
        return conn

    async def send(self, msg: M, *, sender: ActorRef[Any] | None = None) -> None:
        conn = await self._get_connection()

        if conn is None:
            return

        envelope = Envelope(
            payload=msg,
            target=self.actor_id,
        )

        if sender is not None:
            corr_id = uuid.uuid4().hex
            future: asyncio.Future[Any] = asyncio.Future()
            self.pending[corr_id] = future
            envelope.correlation_id = corr_id

            async def forward_reply():
                from casty.reply import Reply
                try:
                    result = await future
                    await sender.send(Reply(result=result))
                except asyncio.CancelledError:
                    pass

            asyncio.create_task(forward_reply())

        data = serialize(envelope)

        await conn.send(Transmit(data=data))

    async def send_envelope(self, envelope: Envelope[M]) -> None:
        conn = await self._get_connection()
        if conn is None:
            return

        envelope.target = self.actor_id

        if envelope.reply_to is not None:
            corr_id = uuid.uuid4().hex
            self.pending[corr_id] = envelope.reply_to
            envelope.correlation_id = corr_id
            envelope.reply_to = None

        data = serialize(envelope)
        await conn.send(Transmit(data=data))

    async def ask[R](self, msg: M, timeout: float | None = None) -> R:
        if self._system is None:
            raise RuntimeError("ActorRef not bound to system")
        return await self._system.ask(self, msg, timeout or self.default_timeout)

    def __rshift__(self, msg: M) -> Awaitable[None]:
        return self.send(msg)

    def __lshift__[R](self, msg: M) -> Awaitable[R]:
        return self.ask(msg)
