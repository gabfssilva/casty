"""Remote actor reference for distributed messaging."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from ..actor import ActorId, ActorRef
from .serialize import serialize
from .transport import MessageType, Transport, WireMessage

if TYPE_CHECKING:
    from asyncio import Future


class RemoteRef[M](ActorRef[M]):
    """ActorRef that sends messages over the network."""

    __slots__ = ("_name", "_transport", "_type_registry", "_pending_asks")

    def __init__(
        self,
        name: str,
        transport: Transport,
        type_registry: dict[str, type],
        pending_asks: dict[str, "Future[Any]"],
    ) -> None:
        self._name = name
        self._transport = transport
        self._type_registry = type_registry
        self._pending_asks = pending_asks

    @property
    def id(self) -> ActorId:
        return ActorId(uid=uuid4(), name=self._name)

    async def send(self, msg: M) -> None:
        """Serialize and send via TCP."""
        data = serialize(msg)
        wire_msg = WireMessage(
            msg_type=MessageType.ACTOR_MSG,
            target_name=self._name,
            payload=data,
        )
        await self._transport.send(wire_msg)

    async def ask[R](self, msg: M, *, timeout: float = 5.0) -> R:
        """Send message and await response over the network."""
        request_id = str(uuid4())

        loop = asyncio.get_running_loop()
        future: Future[R] = loop.create_future()
        self._pending_asks[request_id] = future

        # Payload: request_id (36 bytes) + serialized message
        data = request_id.encode("utf-8") + serialize(msg)
        wire_msg = WireMessage(
            msg_type=MessageType.ASK_REQ,
            target_name=self._name,
            payload=data,
        )

        try:
            await self._transport.send(wire_msg)
            return await asyncio.wait_for(future, timeout=timeout)
        except asyncio.TimeoutError:
            self._pending_asks.pop(request_id, None)
            raise
