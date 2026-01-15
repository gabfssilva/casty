"""Clustered Actor System - ActorSystem with automatic cluster integration.

This module provides ClusteredActorSystem, a subclass of ActorSystem that
automatically registers named actors in the cluster for remote discovery.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any

from casty.actor import Actor, LocalRef, ShardedRef
from casty.system import ActorSystem
from casty.supervision import SupervisorConfig

from .cluster import Cluster
from .config import ClusterConfig
from .consistency import ShardConsistency
from .replication_config import Replication
from .messages import (
    LocalRegister,
    LocalUnregister,
    GetRemoteRef,
    GetMembers,
    Subscribe,
    Unsubscribe,
    ClusterEvent,
    RegisterShardedType,
    RemoteEntitySend,
    RemoteEntityAsk,
    RegisterSingleton,
)
from .remote_ref import RemoteRef
from .singleton_ref import SingletonRef

if TYPE_CHECKING:
    pass


class ClusteredActorSystem(ActorSystem):
    """Actor system with automatic cluster integration.

    ClusteredActorSystem extends ActorSystem to provide transparent cluster
    functionality. Named actors are automatically registered in the cluster
    for remote discovery.

    Features:
    - Named actors are automatically registered in the cluster
    - Unnamed actors remain local-only
    - Convenient `get_ref()` method for remote actor lookup
    - Subscribe to cluster events (NodeJoined, NodeFailed, etc.)

    Example:
        async with ActorSystem.clustered(
            host="0.0.0.0",
            port=7946,
            seeds=["192.168.1.10:7946"],
        ) as system:
            # Named actor → automatically registered in cluster
            worker = await system.spawn(Worker, name="worker-1")

            # Unnamed actor → local only
            temp = await system.spawn(TempProcessor)

            # Get reference to remote actor
            remote = await system.get_ref("worker-2")
            result = await remote.ask(GetStatus())

            # Subscribe to cluster events
            await system.subscribe(my_handler)
    """

    def __init__(self, config: ClusterConfig) -> None:
        super().__init__()
        self._cluster_config = config
        self._cluster: LocalRef | None = None
        self._cluster_ready = asyncio.Event()

    @property
    def cluster(self) -> LocalRef | None:
        """Get the Cluster actor reference."""
        return self._cluster

    @property
    def node_id(self) -> str | None:
        """Get this node's ID."""
        return self._cluster_config.node_id

    async def start(self) -> None:
        """Start the clustered actor system."""
        await super().start()

        # Spawn the Cluster actor (using parent's spawn to avoid registration)
        self._cluster = await super().spawn(Cluster, config=self._cluster_config)

        # Wait for cluster to be ready (bound to port)
        await asyncio.sleep(0.1)  # Give cluster time to bind
        self._cluster_ready.set()

    async def spawn[M](
        self,
        actor_cls: type[Actor[M]],
        *,
        name: str | None = None,
        supervision: SupervisorConfig | None = None,
        cluster: bool | None = None,
        sharded: bool = False,
        singleton: bool = False,
        consistency: ShardConsistency | None = None,
        durable: bool = False,
        replication: Replication | None = None,
        **kwargs: Any,
    ) -> LocalRef[M] | ShardedRef[M] | SingletonRef[M]:
        """Spawn an actor, optionally registering it in the cluster.

        Args:
            actor_cls: The actor class to instantiate
            name: Optional name for the actor. Named actors are registered
                  in the cluster by default.
            supervision: Override supervision configuration
            cluster: Explicit cluster registration control:
                     - None (default): Register if name is provided
                     - True: Force registration (requires name)
                     - False: Don't register even if named
            sharded: If True, spawn as a sharded actor type. Entities are
                    created lazily and routed via consistent hashing.
            singleton: If True, spawn as a cluster-wide singleton. Only one
                      instance exists across all nodes. Requires a name.
            consistency: For sharded actors, determines how many nodes must
                        acknowledge the registration before returning:
                        - Eventual() (default): Fire-and-forget, return immediately
                        - Strong(): Wait for all nodes
                        - Quorum(): Wait for majority
                        - AtLeast(n): Wait for at least n nodes
            durable: If True, persist actor state with WAL
            replication: Configuration for replication (factor + consistency level)
            **kwargs: Constructor arguments for the actor

        Returns:
            LocalRef for regular actors, ShardedRef for sharded actors,
            SingletonRef for singleton actors
        """
        from casty.cluster.messages import RegisterReplicatedType

        # Default replication config (no replication)
        replication = replication or Replication()
        write_consistency = replication.resolve_consistency()

        # Handle singleton actors
        if singleton:
            if not name:
                raise ValueError("Singleton actors require a name")
            if sharded:
                raise ValueError("Actor cannot be both singleton and sharded")

            # Register as replicated type if factor > 1
            if replication.factor > 1 and self._cluster:
                await self._cluster.send(
                    RegisterReplicatedType(
                        type_name=name,
                        actor_cls=actor_cls,
                        actor_type="singleton",
                        replication_factor=replication.factor,
                        write_consistency=write_consistency,
                    )
                )

            return await self._spawn_singleton(actor_cls, name)

        # Handle sharded actors
        if sharded:
            if not name:
                raise ValueError("Sharded actors require a name (entity_type)")

            # Register as replicated type if factor > 1
            if replication.factor > 1 and self._cluster:
                await self._cluster.send(
                    RegisterReplicatedType(
                        type_name=name,
                        actor_cls=actor_cls,
                        actor_type="sharded",
                        replication_factor=replication.factor,
                        write_consistency=write_consistency,
                    )
                )

            return await self._spawn_sharded(actor_cls, name, write_consistency)

        # Spawn the actor locally first
        ref = await super().spawn(
            actor_cls,
            name=name,
            supervision=supervision,
            durable=durable,
            **kwargs,
        )

        # Determine if we should register in cluster
        should_register = cluster if cluster is not None else (name is not None)

        if should_register and name and self._cluster:
            await self._cluster.send(LocalRegister(name, ref))

        return ref

    async def _spawn_sharded[M](
        self,
        actor_cls: type[Actor[M]],
        entity_type: str,
        consistency: ShardConsistency | None = None,
    ) -> ShardedRef[M]:
        """Spawn a sharded actor type.

        This registers the actor class for on-demand entity spawning.
        Actual entity actors are created lazily when messages are sent.

        Args:
            actor_cls: The actor class to use for entities
            entity_type: Name for this sharded actor type
            consistency: Determines how many nodes must acknowledge
                        the registration before returning

        Returns:
            ShardedRef for accessing entities
        """
        if not self._cluster:
            raise RuntimeError("Cluster not initialized")

        # Register the sharded type in the cluster
        # Use ask to wait for completion (supports consistency levels)
        await self._cluster.ask(
            RegisterShardedType(entity_type, actor_cls, consistency),
            timeout=30.0,  # Allow enough time for consistency wait
        )

        # Create ShardedRef with routing callbacks
        # For non-Eventual consistency, we use ask to wait for replication
        should_wait = consistency.should_wait() if consistency else False

        async def send_callback(et: str, entity_id: str, msg: Any) -> None:
            if self._cluster:
                if should_wait:
                    # Wait for replication to complete
                    await self._cluster.ask(RemoteEntitySend(et, entity_id, msg), timeout=10.0)
                else:
                    await self._cluster.send(RemoteEntitySend(et, entity_id, msg))

        async def ask_callback(et: str, entity_id: str, msg: Any, timeout: float) -> Any:
            if self._cluster:
                return await self._cluster.ask(
                    RemoteEntityAsk(et, entity_id, msg),
                    timeout=timeout,
                )
            return None

        return ShardedRef(
            entity_type=entity_type,
            system=self,
            send_callback=send_callback,
            ask_callback=ask_callback,
        )

    async def _spawn_singleton[M](
        self,
        actor_cls: type[Actor[M]],
        singleton_name: str,
    ) -> SingletonRef[M]:
        """Spawn a singleton actor type.

        This registers the actor as a cluster-wide singleton. The cluster
        determines which node owns it based on consistent hashing. If this
        node owns it, the actor is spawned locally. If another node owns it,
        the metadata is published for them to spawn it.

        Args:
            actor_cls: The actor class for the singleton
            singleton_name: Unique name for this singleton

        Returns:
            SingletonRef for accessing the singleton
        """
        if not self._cluster:
            raise RuntimeError("Cluster not initialized")

        return await self._cluster.ask(
            RegisterSingleton(singleton_name, actor_cls),
            timeout=30.0,
        )

    async def unregister(self, name: str) -> None:
        """Unregister an actor from the cluster.

        Args:
            name: The actor name to unregister
        """
        if self._cluster:
            await self._cluster.send(LocalUnregister(name))

    async def lookup[M](
        self,
        actor_name: str,
        node_id: str | None = None,
        *,
        timeout: float = 5.0,
    ) -> RemoteRef[M] | LocalRef[M] | None:
        """Get a reference to an actor by name.

        Looks up the actor in the cluster. If the actor is local, returns
        the local ActorRef. If remote, returns a RemoteRef.

        Args:
            actor_name: Name of the actor to find
            node_id: Optional node ID hint (skip lookup if known)
            timeout: Lookup timeout in seconds

        Returns:
            ActorRef for local actors, RemoteRef for remote actors,
            or None if not found
        """
        if not self._cluster:
            return None

        try:
            result = await self._cluster.ask(
                GetRemoteRef(actor_name, node_id),
                timeout=timeout,
            )
            return result
        except asyncio.TimeoutError:
            return None

    async def get_members(self) -> dict[str, str]:
        """Get the actor registry (actor_name -> node_id).

        Returns:
            Dictionary mapping actor names to their hosting node IDs
        """
        if not self._cluster:
            return {}

        return await self._cluster.ask(GetMembers())

    async def subscribe(self, subscriber: LocalRef[ClusterEvent]) -> None:
        """Subscribe to cluster events.

        Events include:
        - NodeJoined: A new node joined the cluster
        - NodeLeft: A node left gracefully
        - NodeFailed: A node failed (detected by SWIM)
        - ActorRegistered: An actor was registered
        - ActorUnregistered: An actor was unregistered

        Args:
            subscriber: ActorRef to receive cluster events
        """
        if self._cluster:
            await self._cluster.send(Subscribe(subscriber))

    async def unsubscribe(self, subscriber: LocalRef[ClusterEvent]) -> None:
        """Unsubscribe from cluster events.

        Args:
            subscriber: ActorRef to stop receiving events
        """
        if self._cluster:
            await self._cluster.send(Unsubscribe(subscriber))

    async def wait_ready(self, timeout: float = 10.0) -> bool:
        """Wait for the cluster to be ready.

        Args:
            timeout: Maximum time to wait in seconds

        Returns:
            True if cluster is ready, False if timeout
        """
        try:
            await asyncio.wait_for(self._cluster_ready.wait(), timeout=timeout)
            return True
        except asyncio.TimeoutError:
            return False

    async def __aenter__(self) -> "ClusteredActorSystem":
        """Enter async context manager."""
        await self.start()
        return self
