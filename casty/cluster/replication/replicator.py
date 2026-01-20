from __future__ import annotations

import asyncio
import logging
from typing import Any, Callable, Awaitable
from dataclasses import dataclass

from .config import ReplicationConfig
from .messages import Replicate

logger = logging.getLogger(__name__)


class Replicator:
    def __init__(
        self,
        actor_id: str,
        config: ReplicationConfig,
        replica_nodes: list[str],
        send_fn: Callable[[str, Any], Awaitable[None]],
        node_id: str,
        timeout: float = 5.0,
    ):
        self._actor_id = actor_id
        self._config = config
        self._replica_nodes = replica_nodes
        self._send_fn = send_fn
        self._node_id = node_id
        self._timeout = timeout
        self._pending: dict[int, asyncio.Future[int]] = {}
        self._ack_counts: dict[int, int] = {}

    async def replicate(self, before: bytes, after: bytes, version: int) -> None:
        if before == after:
            return

        if not self._replica_nodes:
            return

        future: asyncio.Future[int] = asyncio.Future()
        self._pending[version] = future
        self._ack_counts[version] = 0

        msg = Replicate(
            actor_id=self._actor_id,
            version=version,
            snapshot=after,
        )

        for node_id in self._replica_nodes:
            await self._send_fn(node_id, msg)

        needed = self._config.write_quorum - 1

        if needed <= 0:
            self._cleanup(version)
            return

        try:
            await asyncio.wait_for(future, timeout=self._timeout)
        except asyncio.TimeoutError:
            logger.warning(
                f"Replication timeout for {self._actor_id} v{version}, "
                f"got {self._ack_counts.get(version, 0)}/{needed} acks"
            )
        finally:
            self._cleanup(version)

    def on_ack_received(self, actor_id: str, version: int, from_node: str) -> None:
        if actor_id != self._actor_id:
            return

        future = self._pending.get(version)
        if future is None or future.done():
            return

        self._ack_counts[version] = self._ack_counts.get(version, 0) + 1
        count = self._ack_counts[version]

        needed = self._config.write_quorum - 1
        if count >= needed:
            future.set_result(count)

    def _cleanup(self, version: int) -> None:
        self._pending.pop(version, None)
        self._ack_counts.pop(version, None)
