from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Awaitable
from uuid import uuid4

import msgpack

from .consistency import Consistency
from .messages import ClusteredSend, ClusteredAsk

if TYPE_CHECKING:
    from casty import LocalRef


_counter: int = 0


def _next_request_id() -> str:
    global _counter
    _counter += 1
    return f"req-{_counter}"


@dataclass
class ClusteredRef[M]:
    actor_id: str
    cluster: LocalRef[Any]
    local_ref: LocalRef[M] | None
    write_consistency: Consistency

    async def send(self, msg: M, *, consistency: Consistency | None = None) -> None:
        effective = consistency if consistency is not None else self.write_consistency
        resolved = self._resolve_consistency(effective)

        if resolved <= 1 and self.local_ref is not None:
            await self.local_ref.send(msg)
            return

        payload_type = f"{type(msg).__module__}.{type(msg).__qualname__}"
        payload_bytes = msgpack.packb(msg.__dict__, use_bin_type=True)

        request_id = _next_request_id()
        clustered_send = ClusteredSend(
            actor_id=self.actor_id,
            request_id=request_id,
            payload_type=payload_type,
            payload=payload_bytes,
            consistency=resolved,
        )

        if resolved <= 1:
            await self.cluster.send(clustered_send)
        else:
            await self.cluster.ask(clustered_send, timeout=5.0)

    async def ask[R](
        self,
        msg: M,
        *,
        timeout: float = 5.0,
    ) -> R:
        if self.local_ref is not None:
            return await self.local_ref.ask(msg, timeout=timeout)

        payload_type = f"{type(msg).__module__}.{type(msg).__qualname__}"
        payload_bytes = msgpack.packb(msg.__dict__, use_bin_type=True)

        return await self.cluster.ask(
            ClusteredAsk(
                actor_id=self.actor_id,
                request_id="",
                payload_type=payload_type,
                payload=payload_bytes,
                consistency=1,
            ),
            timeout=timeout,
        )

    def _resolve_consistency(self, consistency: Consistency) -> int:
        match consistency:
            case 'async':
                return 0
            case 'one':
                return 1
            case 'all':
                return -1
            case 'quorum':
                return -2
            case int(n):
                return n

    def __rshift__(self, msg: M) -> Awaitable[None]:
        return self.send(msg)

    def __lshift__[R](self, msg: M) -> Awaitable[R]:
        return self.ask(msg)

    def __repr__(self) -> str:
        return f"ClusteredRef({self.actor_id})"
