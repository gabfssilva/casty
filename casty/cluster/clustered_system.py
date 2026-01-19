from __future__ import annotations

from typing import Any
from uuid import uuid4

from casty import Actor, LocalActorRef
from casty.system import LocalSystem
from casty.supervision import SupervisorConfig

from .cluster import Cluster
from .clustered_ref import ClusteredActorRef
from .config import ClusterConfig
from .messages import RegisterClusteredActor, GetClusteredActor
from .scope import Scope, ClusterScope


class ClusteredSystem(LocalSystem):

    def __init__(self, config: ClusterConfig | None = None):
        super().__init__()
        self._config = config or ClusterConfig.development()
        self._cluster: LocalActorRef[Any] | None = None

    @property
    def node_id(self) -> str:
        return self._config.node_id or "unknown"

    async def start(self) -> None:
        await super().start()
        self._cluster = await self._actor_internal(
            Cluster,
            name="__cluster__",
            config=self._config,
        )

    async def actor[M](
        self,
        actor_cls: type[Actor[M]],
        *,
        name: str,
        scope: Scope = 'local',
        supervision: SupervisorConfig | None = None,
        durable: bool = False,
        **kwargs: Any,
    ) -> LocalActorRef[M] | ClusteredActorRef[M]:
        if scope == 'local':
            return await super().actor(
                actor_cls,
                name=name,
                scope=scope,
                supervision=supervision,
                durable=durable,
                **kwargs,
            )

        if isinstance(scope, ClusterScope):
            replication = scope.replication
            consistency = scope.consistency
        else:
            replication = 1
            consistency = 'one'

        actor_id = name

        info = await self._cluster.ask(GetClusteredActor(actor_id=actor_id))
        if info is None:
            await self._cluster.send(
                RegisterClusteredActor(
                    actor_id=actor_id,
                    actor_cls=actor_cls,
                    replication=replication if isinstance(replication, int) else 1,
                    singleton=False,
                    actor_kwargs=kwargs if kwargs else None,
                )
            )
            info = await self._cluster.ask(GetClusteredActor(actor_id=actor_id))

        local_ref = self._get_local_actor(actor_id) if info else None

        return ClusteredActorRef(
            actor_id=actor_id,
            cluster=self._cluster,
            local_ref=local_ref,
            write_consistency=consistency,
        )

    def _get_local_actor(self, actor_id: str) -> LocalActorRef[Any] | None:
        if self._cluster is None:
            return None
        node = self._supervision_tree.get_node(self._cluster.id)
        if node is None:
            return None
        cluster_actor = node.actor_instance
        local_actors = getattr(cluster_actor, '_local_actors', {})
        return local_actors.get(actor_id)

    async def __aenter__(self) -> "ClusteredSystem":
        await self.start()
        return self
