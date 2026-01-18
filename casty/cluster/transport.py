from __future__ import annotations

from abc import ABC
from dataclasses import dataclass
from typing import Any

from casty import Actor, LocalActorRef

from .messages import (
    TransportConnected,
    TransportDisconnected,
    TransportEvent,
    TransportReceived,
    TransportSend,
)


@dataclass(frozen=True, slots=True)
class Connect:
    node_id: str
    address: tuple[str, int]


@dataclass(frozen=True, slots=True)
class Disconnect:
    node_id: str


type TransportMessage = TransportSend | Connect | Disconnect


class Transport[M](Actor[TransportMessage | M], ABC):

    def __init__(self, cluster: LocalActorRef[TransportEvent]):
        self._cluster = cluster

    async def _notify_received(self, node_id: str, payload: Any) -> None:
        await self._cluster.send(TransportReceived(node_id=node_id, payload=payload))

    async def _notify_connected(self, node_id: str, address: tuple[str, int]) -> None:
        await self._cluster.send(TransportConnected(node_id=node_id, address=address))

    async def _notify_disconnected(self, node_id: str) -> None:
        await self._cluster.send(TransportDisconnected(node_id=node_id))
