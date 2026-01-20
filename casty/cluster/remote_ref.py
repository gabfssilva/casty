from __future__ import annotations

import asyncio
import uuid
from dataclasses import dataclass, field
from typing import Any, Awaitable

from casty.envelope import Envelope
from casty.ref import ActorRef
from casty.serializable import serialize
from .transport_messages import Transmit, Connect
from .messages import GetAliveMembers

@dataclass
class RemoteActorRef[M](ActorRef[M]):
    actor_id: str
    node_id: str
    connection: ActorRef | None = None
    outbound_ref: ActorRef | None = field(default=None, repr=False)
    membership_ref: ActorRef | None = field(default=None, repr=False)
    pending_asks: dict[str, asyncio.Future[Any]] | None = None
    default_timeout: float = 30.0

    async def _get_connection(self) -> ActorRef | None:
        if self.connection is not None:
            return self.connection

        if self.outbound_ref is None or self.membership_ref is None:
            return None

        members = await self.membership_ref.ask(GetAliveMembers())
        if self.node_id not in members:
            return None

        address = members[self.node_id].address
        conn = await self.outbound_ref.ask(Connect(node_id=self.node_id, address=address))
        self.connection = conn
        return conn

    async def send(self, msg: M, *, sender: str | None = None) -> None:
        conn = await self._get_connection()
        if conn is None:
            return

        envelope = Envelope(
            payload=msg,
            sender=sender,
            target=self.actor_id,
        )
        data = serialize(envelope)
        await conn.send(Transmit(data=data))

    async def send_envelope(self, envelope: Envelope[M]) -> None:
        conn = await self._get_connection()
        if conn is None:
            return

        envelope.target = self.actor_id
        data = serialize(envelope)
        await conn.send(Transmit(data=data))

    async def ask(self, msg: M, timeout: float | None = None) -> Any:
        conn = await self._get_connection()
        if conn is None:
            raise RuntimeError(f"Cannot connect to node {self.node_id}")

        if self.pending_asks is None:
            raise RuntimeError("pending_asks not configured for ask()")

        correlation_id = uuid.uuid4().hex
        future: asyncio.Future[Any] = asyncio.Future()
        self.pending_asks[correlation_id] = future

        envelope = Envelope(
            payload=msg,
            target=self.actor_id,
            correlation_id=correlation_id,
        )
        data = serialize(envelope)
        await conn.send(Transmit(data=data))

        try:
            actual_timeout = timeout if timeout is not None else self.default_timeout
            return await asyncio.wait_for(future, actual_timeout)
        finally:
            self.pending_asks.pop(correlation_id, None)

    def __rshift__(self, msg: M) -> Awaitable[None]:
        return self.send(msg)

    def __lshift__[R](self, msg: M) -> Awaitable[R]:
        return self.ask(msg)
