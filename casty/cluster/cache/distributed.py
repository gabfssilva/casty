from __future__ import annotations

from pathlib import Path
from typing import Any, Literal, TYPE_CHECKING

import msgpack

from casty.actor import LocalActorRef
from casty.wal import FileStoreBackend, InMemoryStoreBackend, StoreBackend

from ..clustered_ref import ClusteredActorRef
from ..scope import ClusterScope

from .messages import Get, Set, Delete, CacheHit, CacheMiss
from .actor import CacheActor

if TYPE_CHECKING:
    from ..clustered_system import ClusteredActorSystem

type Consistency = Literal['async', 'one', 'quorum', 'all']


class DistributedCache:
    def __init__(
        self,
        ref: LocalActorRef | ClusteredActorRef,
        default_consistency: Consistency = 'quorum'
    ):
        self._ref = ref
        self._default_consistency = default_consistency

    @property
    def ref(self) -> LocalActorRef | ClusteredActorRef:
        """Expose underlying ref for advanced use cases."""
        return self._ref

    @classmethod
    async def create(
        cls,
        system: "ClusteredActorSystem",
        name: str = "cache",
        replication: int = 3,
        default_consistency: Consistency = 'quorum',
        wal_path: str | Path | None = None,
    ) -> "DistributedCache":
        backend: StoreBackend
        if wal_path is not None:
            backend = FileStoreBackend(Path(wal_path) if isinstance(wal_path, str) else wal_path)
        else:
            backend = InMemoryStoreBackend()

        ref = await system.actor(
            CacheActor,
            name=name,
            scope=ClusterScope(replication=replication),
            backend=backend,
        )

        return cls(ref, default_consistency)

    async def get(self, key: str) -> Any | None:
        result = await self._ref.ask(Get(key))
        match result:
            case CacheHit(value):
                return msgpack.unpackb(value)
            case CacheMiss():
                return None

    async def set(self, key: str, value: Any, ttl: float | None = None) -> None:
        packed = msgpack.packb(value)
        await self._ref.ask(Set(key, packed, ttl))

    async def delete(self, key: str) -> None:
        await self._ref.ask(Delete(key))
