from __future__ import annotations

import dataclasses
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Awaitable, Literal

import msgpack

from .consistency import Routing, NoLocalReplicaError
from .messages import ClusteredSend, ClusteredAsk
from ..protocols import ActorRef

if TYPE_CHECKING:
    from casty import LocalActorRef

_counter: int = 0


def _next_request_id() -> str:
    global _counter
    _counter += 1
    return f"req-{_counter}"


@dataclass
class ClusteredActorRef[M](ActorRef[M]):
    actor_id: str
    cluster: LocalActorRef[Any]
    local_ref: LocalActorRef[M] | None

    async def send(self, msg: M, *, routing: Routing = 'leader') -> None:
        match routing:
            case 'local':
                if self.local_ref is None:
                    raise NoLocalReplicaError(f"No local replica for actor {self.actor_id}")
                await self.local_ref.send(msg)

            case 'fastest':
                if self.local_ref is not None:
                    await self.local_ref.send(msg)
                else:
                    await self._send_to_cluster(msg, routing='leader')

            case 'leader':
                await self._send_to_cluster(msg, routing='leader')

            case node_id:
                await self._send_to_cluster(msg, routing=node_id)

    async def _send_to_cluster(self, msg: M, routing: str) -> None:
        payload_type = f"{type(msg).__module__}.{type(msg).__qualname__}"
        msg_dict = dataclasses.asdict(msg) if dataclasses.is_dataclass(msg) else msg.__dict__
        payload_bytes = msgpack.packb(msg_dict, use_bin_type=True)

        request_id = _next_request_id()
        clustered_send = ClusteredSend(
            actor_id=self.actor_id,
            request_id=request_id,
            payload_type=payload_type,
            payload=payload_bytes,
            routing=routing,
        )

        await self.cluster.send(clustered_send)

    async def ask[R](
        self,
        msg: M,
        *,
        routing: Routing = 'leader',
        timeout: float = 5.0,
    ) -> R:
        match routing:
            case 'local':
                if self.local_ref is None:
                    raise NoLocalReplicaError(f"No local replica for actor {self.actor_id}")
                return await self.local_ref.ask(msg, timeout=timeout)

            case 'fastest':
                if self.local_ref is not None:
                    return await self.local_ref.ask(msg, timeout=timeout)
                else:
                    return await self._ask_cluster(msg, routing='leader', timeout=timeout)

            case 'leader':
                return await self._ask_cluster(msg, routing='leader', timeout=timeout)

            case node_id:
                return await self._ask_cluster(msg, routing=node_id, timeout=timeout)

    async def _ask_cluster[R](self, msg: M, routing: str, timeout: float) -> R:
        payload_type = f"{type(msg).__module__}.{type(msg).__qualname__}"
        msg_dict = dataclasses.asdict(msg) if dataclasses.is_dataclass(msg) else msg.__dict__
        payload_bytes = msgpack.packb(msg_dict, use_bin_type=True)

        return await self.cluster.ask(
            ClusteredAsk(
                actor_id=self.actor_id,
                request_id=_next_request_id(),
                payload_type=payload_type,
                payload=payload_bytes,
                routing=routing,
            ),
            timeout=timeout,
        )

    def __rshift__(self, msg: M) -> Awaitable[None]:
        return self.send(msg)

    def __lshift__[R](self, msg: M) -> Awaitable[R]:
        return self.ask(msg)

    def __repr__(self) -> str:
        return f"ClusteredRef({self.actor_id})"
