from __future__ import annotations

from typing import Any
from uuid import uuid4

from casty import Actor, ActorSystem, LocalRef

from .cluster import Cluster
from .clustered_actor import ClusteredActor
from .clustered_ref import ClusteredRef
from .config import ClusterConfig
from .consistency import Consistency, Replication
from .messages import RegisterClusteredActor, GetClusteredActor


class ClusteredActorSystem(ActorSystem):

    def __init__(self, config: ClusterConfig | None = None):
        super().__init__()
        self._config = config or ClusterConfig.development()
        self._cluster: LocalRef[Any] | None = None

    @property
    def node_id(self) -> str:
        return self._config.node_id or "unknown"

    async def start(self) -> None:
        await super().start()
        self._cluster = await super().spawn(
            Cluster,
            config=self._config,
        )

    async def spawn[M](
        self,
        actor_cls: type[Actor[M]],
        *,
        name: str | None = None,
        clustered: bool = False,
        replication: Replication = 1,
        singleton: bool = False,
        write_consistency: Consistency = 'one',
        **kwargs: Any,
    ) -> LocalRef[M] | ClusteredRef[M]:
        if not clustered:
            return await super().spawn(actor_cls, name=name, **kwargs)

        actor_id = name or f"{actor_cls.__name__}-{uuid4().hex[:8]}"

        info = await self._cluster.ask(GetClusteredActor(actor_id=actor_id))
        if info is None:
            await self._cluster.send(
                RegisterClusteredActor(
                    actor_id=actor_id,
                    actor_cls=actor_cls,
                    replication=replication if isinstance(replication, int) else 1,
                    singleton=singleton,
                )
            )
            info = await self._cluster.ask(GetClusteredActor(actor_id=actor_id))

        local_ref = self._get_local_actor(actor_id) if info else None

        return ClusteredRef(
            actor_id=actor_id,
            cluster=self._cluster,
            local_ref=local_ref,
            write_consistency=write_consistency,
        )

    def _get_local_actor(self, actor_id: str) -> LocalRef[Any] | None:
        if self._cluster is None:
            return None
        node = self._supervision_tree.get_node(self._cluster.id)
        if node is None:
            return None
        cluster_actor = node.actor_instance
        local_actors = getattr(cluster_actor, '_local_actors', {})
        return local_actors.get(actor_id)

    async def __aenter__(self) -> "ClusteredActorSystem":
        await self.start()
        return self
