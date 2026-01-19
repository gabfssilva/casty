from __future__ import annotations

from typing import Any
from uuid import uuid4

from casty import Actor, LocalActorRef
from casty.actor import ActorId
from casty.system import LocalSystem
from casty.supervision import SupervisorConfig

from .cluster import Cluster
from .clustered_ref import ClusteredActorRef
from .config import ClusterConfig
from .messages import RegisterClusteredActor, GetClusteredActor, ActorUnregistered
from .scope import Scope, ClusterScope


class ClusteredSystem(LocalSystem):

    def __init__(self, config: ClusterConfig | None = None):
        super().__init__()
        self._config = config or ClusterConfig.development()
        self._cluster: LocalActorRef[Any] | None = None
        self._node_id: str | None = None

    @property
    def node_id(self) -> str:
        return self._node_id or self._config.node_id or "unknown"

    async def start(self) -> None:
        await super().start()
        self._cluster = await self._actor_internal(
            Cluster,
            name="__cluster__",
            config=self._config,
        )
        # Get the actual node_id from the Cluster
        node = self._supervision_tree.get_node(self._cluster.id)
        if node:
            self._node_id = node.actor_instance._node_id

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
        else:
            replication = 1

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

    async def _on_actor_stopped(self, actor_id: ActorId) -> None:
        """Notify cluster when a clustered actor stops."""
        if self._cluster is None:
            return

        node = self._supervision_tree.get_node(self._cluster.id)
        if node is None:
            return

        cluster_actor = node.actor_instance
        local_actors: dict[str, LocalActorRef[Any]] = getattr(cluster_actor, '_local_actors', {})
        clustered_actors: dict[str, Any] = getattr(cluster_actor, '_clustered_actors', {})

        # Find which clustered actor_id corresponds to this ActorId
        logical_id = None
        for aid, ref in list(local_actors.items()):
            if ref.id == actor_id:
                logical_id = aid
                break

        if logical_id:
            local_actors.pop(logical_id, None)
            clustered_actors.pop(logical_id, None)
            # Notify other nodes
            await self._cluster.send(ActorUnregistered(actor_id=logical_id))

    async def __aenter__(self) -> "ClusteredSystem":
        await self.start()
        return self
