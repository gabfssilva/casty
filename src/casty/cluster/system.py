"""Cluster-aware actor system with automatic shard routing.

Extends the core ``ActorSystem`` with transparent sharding, broadcast actors,
and remote ``ask()`` support over TCP.  ``ClusteredActorSystem`` is the main
entry point for multi-node deployments.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, TYPE_CHECKING, cast, overload
from uuid import uuid4

from casty.actor import (
    Behavior,
    Behaviors,
    BroadcastedBehavior,
    ShardedBehavior,
    SingletonBehavior,
)
from casty.cluster.cluster import (
    Cluster,
    ClusterConfig,
    WaitForMembers,
)
from casty.cluster.receptionist import Find, Listing, ReceptionistMsg, ServiceKey
from casty.cluster.state import ClusterState, MemberStatus, NodeAddress, NodeId
from casty.cluster.topology_actor import TopologyMsg
from casty.core.mailbox import Mailbox
from casty.core.system import ActorSystem
from casty.ref import ActorRef, BroadcastRef
from casty.remote.ref import RemoteActorRef
from casty.core.address import ActorAddress
from casty.remote.serialization import PickleSerializer
from casty.remote.tcp_transport import (
    GetPort,
    InboundMessageHandler,
    RemoteTransport,
    TcpTransportConfig,
    TcpTransportMsg,
    tcp_transport,
)
from casty.core.transport import LocalTransport
from casty.remote.tls import Config as TlsConfig
from casty.cluster.coordinator import (
    CoordinatorMsg,
    LeastShardStrategy,
    RegisterRegion,
    shard_coordinator_actor,
)
from casty.cluster.envelope import ShardEnvelope
from casty.cluster.proxy import broadcast_proxy_behavior, shard_proxy_behavior

if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine

    from casty.config import CastyConfig
    from casty.distributed import Distributed
    from casty.core.journal import EventJournal


class ClusteredActorSystem(ActorSystem):
    """Actor system with cluster membership, sharding, and remote messaging.

    Extends ``ActorSystem`` to transparently distribute ``ShardedBehavior``
    actors across cluster nodes.  Handles gossip-based membership, failure
    detection, shard coordination, and TCP transport.

    Use as an async context manager to start/stop the cluster lifecycle.

    Parameters
    ----------
    name : str
        Logical system name (shared across cluster nodes).
    host : str
        Advertised hostname for this node.
    port : int
        Advertised port for this node (use ``0`` for auto-assignment).
    seed_nodes : tuple[tuple[str, int], ...] | None
        Initial contact points for cluster join.
    roles : frozenset[str]
        Roles assigned to this node (for role-aware shard placement).
    bind_host : str | None
        Network interface to bind to (defaults to *host*).
    config : CastyConfig | None
        Full configuration object.
    tls : TlsConfig | None
        TLS configuration for inter-node communication.  Use
        ``TlsConfig.from_paths(...)`` for file-based setup or pass
        pre-built ``ssl.SSLContext`` via ``TlsConfig(server_context=...,
        client_context=...)``.
    required_quorum : int | None
        If set, ``__aenter__`` blocks until this many nodes are ``up``.

    Examples
    --------
    >>> async with ClusteredActorSystem(
    ...     name="my-app", host="127.0.0.1", port=25520,
    ...     seed_nodes=[("127.0.0.1", 25520)],
    ... ) as system:
    ...     ref = system.spawn(Behaviors.sharded(my_entity, num_shards=50), "things")
    ...     ref.tell(ShardEnvelope("abc", DoSomething()))
    """

    def __init__(
        self,
        *,
        name: str,
        host: str,
        port: int,
        node_id: NodeId,
        seed_nodes: tuple[tuple[str, int], ...] | None = None,
        roles: frozenset[str] = frozenset(),
        bind_host: str | None = None,
        config: CastyConfig | None = None,
        tls: TlsConfig | None = None,
        required_quorum: int | None = None,
    ) -> None:
        super().__init__(name=name, config=config)
        self._local_transport = LocalTransport()
        self._host = host
        self._port: int = port
        self._node_id = node_id
        self._bind_host = bind_host or host
        self._seed_nodes = seed_nodes or ()
        self._roles = roles
        self._self_node = NodeAddress(host=host, port=port)
        self._coordinators: dict[str, ActorRef[CoordinatorMsg]] = {}
        self._receptionist_ref: ActorRef[ReceptionistMsg] | None = None
        self._topology_ref: ActorRef[TopologyMsg] | None = None
        self._required_quorum = required_quorum
        self._logger = logging.getLogger(f"casty.cluster.{name}")
        self._tls = tls
        self._serializer = PickleSerializer()
        self._remote_transport: RemoteTransport | None = None
        self._tcp_ref: ActorRef[TcpTransportMsg] | None = None

    def __make_ref__[M](self, id: str, deliver: Callable[[Any], None]) -> ActorRef[M]:
        path = f"/{id}" if not id.startswith("/") else id
        self._local_transport.register(path, deliver)
        if self._remote_transport is not None:
            addr = ActorAddress(
                system=self._name,
                path=path,
                host=self._host,
                port=self._port,
            )
            return self._remote_transport.make_ref(addr)
        return super().__make_ref__(id, deliver)

    @property
    def receptionist(self) -> ActorRef[ReceptionistMsg]:
        """The cluster-wide receptionist for service discovery."""
        if self._receptionist_ref is None:
            ref = self.lookup("/_cluster/_receptionist")
            if ref is None:
                msg = "Receptionist not available yet"
                raise RuntimeError(msg)
            self._receptionist_ref = ref  # type: ignore[assignment]
        return self._receptionist_ref

    @property
    def self_node(self) -> NodeAddress:
        """The ``NodeAddress`` representing this cluster member."""
        return self._self_node

    @classmethod
    def from_config(
        cls,
        config: CastyConfig,
        *,
        host: str | None = None,
        port: int | None = None,
        node_id: NodeId | None = None,
        seed_nodes: tuple[tuple[str, int], ...] | None = None,
        bind_host: str | None = None,
        tls: TlsConfig | None = None,
        required_quorum: int | None = None,
    ) -> ClusteredActorSystem:
        """Create a ``ClusteredActorSystem`` from a ``CastyConfig``.

        Reads host, port, seed nodes, and roles from the ``[cluster]``
        section of the config.  Keyword arguments override config values.

        Parameters
        ----------
        config : CastyConfig
            Parsed configuration (typically from ``load_config``).

        Returns
        -------
        ClusteredActorSystem

        Raises
        ------
        ValueError
            If the config has no ``[cluster]`` section.

        Examples
        --------
        >>> config = load_config(Path("casty.toml"))
        >>> system = ClusteredActorSystem.from_config(config)
        """
        cluster = config.cluster
        if cluster is None:
            msg = "CastyConfig has no [cluster] section"
            raise ValueError(msg)

        return cls(
            name=config.system_name,
            host=host or cluster.host,
            port=port if port is not None else cluster.port,
            node_id=node_id or cluster.node_id,
            seed_nodes=seed_nodes if seed_nodes is not None else cluster.seed_nodes,
            roles=cluster.roles,
            bind_host=bind_host,
            config=config,
            tls=tls if tls is not None else config.tls,
            required_quorum=required_quorum,
        )

    async def __aenter__(self) -> ClusteredActorSystem:
        # 1. Start the actor system first — we need spawn() for the transport actor
        await super().__aenter__()

        try:
            # 2. Spawn TCP transport actor
            tcp_config = TcpTransportConfig(
                host=self._bind_host,
                port=self._port,
                self_address=(self._host, self._port),
                server_ssl=self._tls.server_context if self._tls else None,
                client_ssl=self._tls.client_context if self._tls else None,
            )
            self._serializer = PickleSerializer()
            inbound = InboundMessageHandler(
                local=self._local_transport,
                serializer=self._serializer,  # pyright: ignore[reportArgumentType]
                system_name=self._name,
            )
            self._tcp_ref = super().spawn(
                tcp_transport(
                    tcp_config,
                    inbound,
                    logger=logging.getLogger(f"casty.tcp.{self._name}"),
                ),
                "_tcp_transport",
            )

            # 3. Ask for the actual port (use super().ask to avoid accessing _remote)
            actual_port: int = await super().ask(
                self._tcp_ref,
                lambda r: GetPort(reply_to=r),
                timeout=5.0,
            )
            if actual_port != self._port:
                self._port = actual_port
                self._self_node = NodeAddress(host=self._host, port=actual_port)

            # 4. Create RemoteTransport with the actor ref
            self._remote_transport = RemoteTransport(
                local=self._local_transport,
                tcp=self._tcp_ref,
                serializer=self._serializer,  # pyright: ignore[reportArgumentType]
                local_host=self._host,
                local_port=self._port,
                system_name=self._name,
                task_runner=self._ensure_task_runner(),
                local_node_id=self._node_id,
            )
            inbound.ref_factory = self._remote_transport.make_ref

            # Start cluster membership (gossip, heartbeat, failure detection)
            cluster_config = ClusterConfig(
                host=self._host,
                port=self._port,
                seed_nodes=self._seed_nodes,
                node_id=self._node_id,
                roles=self._roles,
            )
            from casty.config import CastyConfig as _CastyConfig

            cfg = self._config or _CastyConfig()
            self._cluster = Cluster(
                self,
                cluster_config,
                remote_transport=self._remote_transport,
                local_transport=self._local_transport,
                system_name=self._name,
                gossip_interval=cfg.gossip.interval,
                gossip_fanout=cfg.gossip.fanout,
                heartbeat_interval=cfg.heartbeat.interval,
                availability_interval=cfg.heartbeat.availability_check_interval,
                failure_detector_config=cfg.failure_detector,
                event_stream=self.event_stream,
            )
            await self._cluster.start()
        except BaseException:
            await super().shutdown()
            raise

        self._remote_transport.update_node_index(
            {self._node_id: (self._host, self._port)}
        )

        await self._local_transport.wait_for_path("/_cluster/_topology")
        self._topology_ref = self.lookup("/_cluster/_topology")  # type: ignore[assignment]

        self._local_transport.register_path_factory(
            "/_coord-",
            self._lazy_spawn_coordinator,
        )

        from casty.distributed.barrier import BarrierMsg, barrier_entity

        self._barrier_proxy: ActorRef[ShardEnvelope[BarrierMsg]] = self.spawn(
            Behaviors.sharded(entity_factory=barrier_entity, num_shards=10),
            "_barrier",
        )

        self._logger.info("Started on %s:%d", self._host, self._port)

        if self._required_quorum is not None:
            self._logger.info("Waiting for %d nodes...", self._required_quorum)
            await self.wait_for(self._required_quorum)
            self._logger.info("Cluster ready (%d nodes up)", self._required_quorum)

        return self

    @property
    def _remote(self) -> RemoteTransport:
        if self._remote_transport is None:
            msg = "ClusteredActorSystem not started"
            raise RuntimeError(msg)
        return self._remote_transport

    @overload
    def spawn[M](
        self, behavior: BroadcastedBehavior[M], name: str
    ) -> BroadcastRef[M]: ...

    @overload
    def spawn[M](
        self, behavior: ShardedBehavior[M], name: str
    ) -> ActorRef[ShardEnvelope[M]]: ...

    @overload
    def spawn[M](self, behavior: SingletonBehavior[M], name: str) -> ActorRef[M]: ...

    @overload
    def spawn[M](
        self,
        behavior: Behavior[M],
        name: str,
        *,
        mailbox: Mailbox[M] | None = None,
    ) -> ActorRef[M]: ...

    def spawn[M](
        self,
        behavior: BroadcastedBehavior[M]
        | ShardedBehavior[M]
        | SingletonBehavior[M]
        | Behavior[M],
        name: str,
        *,
        mailbox: Mailbox[M] | None = None,
    ) -> BroadcastRef[M] | ActorRef[ShardEnvelope[M]] | ActorRef[M]:
        match behavior:
            case BroadcastedBehavior():
                return self._spawn_broadcasted(behavior, name)
            case ShardedBehavior():
                return self._spawn_sharded(behavior, name)
            case SingletonBehavior():
                return self._spawn_singleton(behavior, name)
            case _:
                return super().spawn(behavior, name, mailbox=mailbox)

    def _spawn_sharded[M](
        self, sharded: ShardedBehavior[M], name: str
    ) -> ActorRef[ShardEnvelope[M]]:
        from casty.cluster.region import shard_region_actor

        self._logger.info("Sharded entity: %s (shards=%d)", name, sharded.num_shards)

        # Build available nodes from self + seed_nodes
        available_nodes = frozenset(
            {self._self_node}
            | {NodeAddress(host=h, port=p) for h, p in self._seed_nodes}
        )

        # Coordinator: deterministic — lives on first seed node
        coordinator = self._get_or_create_coordinator(name, sharded, available_nodes)
        coordinator.tell(RegisterRegion(node=self._self_node))

        # Local shard region
        region_ref = super().spawn(
            shard_region_actor(
                entity_factory=sharded.entity_factory,
                logger=logging.getLogger(f"casty.region.{self._name}"),
            ),
            f"_region-{name}",
        )

        # Proxy — the ref users interact with
        proxy_ref: ActorRef[Any] = super().spawn(
            shard_proxy_behavior(
                coordinator=coordinator,
                local_region=region_ref,
                self_node=self._self_node,
                shard_name=name,
                num_shards=sharded.num_shards,
                remote_transport=self._remote_transport,
                system_name=self._name,
                logger=logging.getLogger(f"casty.shard_proxy.{self._name}"),
                topology_ref=self._topology_ref,  # type: ignore[arg-type]
            ),
            name,
        )

        return cast(ActorRef[ShardEnvelope[M]], proxy_ref)

    def _spawn_singleton[M](
        self, singleton: SingletonBehavior[M], name: str
    ) -> ActorRef[M]:
        from casty.cluster.singleton import singleton_manager_actor

        self._logger.info("Singleton: %s", name)

        manager_ref: ActorRef[Any] = super().spawn(
            singleton_manager_actor(
                factory=singleton.factory,
                name=name,
                remote_transport=self._remote_transport,
                system_name=self._name,
                logger=logging.getLogger(f"casty.singleton.{self._name}"),
                topology_ref=self._topology_ref,  # type: ignore[arg-type]
                self_node=self._self_node,
            ),
            f"_singleton-{name}",
        )

        return cast(ActorRef[M], manager_ref)

    def _spawn_broadcasted[M](
        self, broadcasted: BroadcastedBehavior[M], name: str
    ) -> BroadcastRef[M]:
        self._logger.info("Broadcasted actor: %s", name)

        # Spawn the real actor on this node at /_bcast-{name}
        local_ref: ActorRef[M] = super().spawn(broadcasted.behavior, f"_bcast-{name}")

        # Spawn the proxy at /{name} — fans out to all members
        proxy_ref: ActorRef[Any] = super().spawn(
            broadcast_proxy_behavior(
                local_ref=local_ref,
                self_node=self._self_node,
                bcast_name=name,
                remote_transport=self._remote_transport,
                system_name=self._name,
                topology_ref=self._topology_ref,  # type: ignore[arg-type]
            ),
            name,
        )

        if not isinstance(proxy_ref, RemoteActorRef):
            msg = "BroadcastRef requires a RemoteActorRef"
            raise TypeError(msg)
        return BroadcastRef[M](
            address=proxy_ref.address,
            _transport=proxy_ref._transport,  # pyright: ignore[reportPrivateUsage]
        )

    def _lazy_spawn_coordinator(self, path: str) -> None:
        """Lazily spawn a coordinator when a remote message arrives for an unknown shard type.

        Called by ``LocalTransport`` path factory when a message arrives at
        ``/_coord-{name}`` and no handler exists yet. This happens when a follower
        coordinator on another node forwards ``GetShardLocation`` or ``RegisterRegion``
        to the leader node, which hasn't spawned that shard type locally.
        """
        prefix = "/_coord-"
        name = path[len(prefix) :]
        if name in self._coordinators:
            return

        self._logger.info("Lazy-spawning coordinator for shard type: %s", name)

        available_nodes = frozenset(
            {self._self_node}
            | {NodeAddress(host=h, port=p) for h, p in self._seed_nodes}
        )

        coord_ref = super().spawn(
            shard_coordinator_actor(
                strategy=LeastShardStrategy(),
                available_nodes=available_nodes,
                shard_type=name,
                remote_transport=self._remote_transport,
                system_name=self._name,
                logger=logging.getLogger(f"casty.coordinator.{self._name}"),
                topology_ref=self._topology_ref,  # type: ignore[arg-type]
                self_node=self._self_node,
            ),
            f"_coord-{name}",
        )
        self._coordinators[name] = coord_ref

    def _get_or_create_coordinator(
        self,
        name: str,
        sharded: ShardedBehavior[Any],
        available_nodes: frozenset[NodeAddress],
    ) -> ActorRef[CoordinatorMsg]:
        if name in self._coordinators:
            return self._coordinators[name]

        coord_ref = super().spawn(
            shard_coordinator_actor(
                strategy=LeastShardStrategy(),
                available_nodes=available_nodes,
                replication=sharded.replication,
                shard_type=name,
                remote_transport=self._remote_transport,
                system_name=self._name,
                logger=logging.getLogger(f"casty.coordinator.{self._name}"),
                topology_ref=self._topology_ref,  # type: ignore[arg-type]
                self_node=self._self_node,
            ),
            f"_coord-{name}",
        )
        self._coordinators[name] = coord_ref

        return coord_ref

    @overload
    async def ask[M, R](
        self,
        ref: BroadcastRef[M],
        msg_factory: Callable[[ActorRef[R]], M],
        *,
        timeout: float,
    ) -> tuple[R, ...]: ...

    @overload
    async def ask[M, R](
        self,
        ref: ActorRef[M],
        msg_factory: Callable[[ActorRef[R]], M],
        *,
        timeout: float,
    ) -> R: ...

    async def ask[M, R](
        self,
        ref: ActorRef[M],
        msg_factory: Callable[[ActorRef[R]], M],
        *,
        timeout: float,
    ) -> R | tuple[R, ...]:
        """Ask with remote-addressable temp reply ref.

        When *ref* is a ``BroadcastRef``, the message is fanned out to all
        cluster members and responses are collected into a ``tuple[R, ...]``.
        """
        if isinstance(ref, BroadcastRef):
            bcast_result: tuple[R, ...] = await self._ask_broadcast(
                ref, msg_factory, timeout=timeout
            )  # type: ignore[assignment]
            return bcast_result

        future: asyncio.Future[R] = asyncio.get_running_loop().create_future()
        ask_id = uuid4().hex
        temp_path = f"/_ask/{ask_id}"

        def on_reply(msg: Any) -> None:
            if not future.done():
                future.set_result(msg)

        temp_ref: ActorRef[R] = self.__make_ref__(temp_path, on_reply)
        try:
            ref.tell(msg_factory(temp_ref))
            return await asyncio.wait_for(future, timeout=timeout)
        finally:
            self._local_transport.unregister(temp_path)

    async def _ask_broadcast[M, R](
        self,
        ref: BroadcastRef[M],
        msg_factory: Callable[[ActorRef[R]], M],
        *,
        timeout: float,
    ) -> tuple[R, ...]:
        """Broadcast ask — sends to all members, collects all responses."""
        state = await self.get_cluster_state(timeout=timeout)
        expected = sum(1 for m in state.members if m.status == MemberStatus.up)

        responses: list[R] = []
        done_event = asyncio.Event()
        ask_id = uuid4().hex
        temp_path = f"/_ask/{ask_id}"

        def on_reply(msg: Any) -> None:
            responses.append(msg)
            if len(responses) >= expected:
                done_event.set()

        temp_ref: ActorRef[R] = self.__make_ref__(temp_path, on_reply)
        try:
            ref.tell(msg_factory(temp_ref))
            await asyncio.wait_for(done_event.wait(), timeout=timeout)
            return tuple(responses)
        finally:
            self._local_transport.unregister(temp_path)

    async def get_cluster_state(self, *, timeout: float = 5.0) -> ClusterState:
        """Query the current cluster membership state.

        Returns
        -------
        ClusterState
            Snapshot of members, their statuses, and the vector clock.

        Examples
        --------
        >>> state = await system.get_cluster_state()
        >>> len(state.members)
        3
        """
        state = await self._cluster.get_state(timeout=timeout)
        self._update_node_index(state)
        return state

    def _update_node_index(self, state: ClusterState) -> None:
        self._remote.update_node_index(
            {m.id: (m.address.host, m.address.port) for m in state.members}
        )

    async def wait_for(self, n: int, *, timeout: float = 60.0) -> ClusterState:
        """Block until at least *n* members have status ``up``.

        Parameters
        ----------
        n : int
            Minimum number of ``up`` members required.
        timeout : float
            Seconds to wait before raising ``TimeoutError``.

        Returns
        -------
        ClusterState
            The cluster state once the quorum is reached.

        Examples
        --------
        >>> state = await system.wait_for(3, timeout=30.0)
        """
        state: ClusterState = await self.ask(
            self._cluster.ref,
            lambda r: WaitForMembers(n=n, reply_to=r),
            timeout=timeout,
        )
        self._update_node_index(state)
        return state

    async def barrier(self, name: str, n: int, *, timeout: float = 60.0) -> None:
        """Distributed barrier -- blocks until *n* nodes have reached this point.

        Parameters
        ----------
        name : str
            Barrier name (shared across nodes).
        n : int
            Number of nodes that must arrive before all are released.
        timeout : float
            Seconds to wait before raising ``TimeoutError``.

        Examples
        --------
        >>> await system.barrier("setup-done", n=3)
        """
        from casty.distributed.barrier import BarrierArrive

        node_id = f"{self._host}:{self._port}"
        await self.ask(
            self._barrier_proxy,
            lambda r: ShardEnvelope(
                name, BarrierArrive(node=node_id, expected=n, reply_to=r)
            ),
            timeout=timeout,
        )

    @overload
    def lookup[M](
        self,
        path: ServiceKey[M],
        *,
        timeout: float = 5.0,
    ) -> Coroutine[Any, Any, Listing[M]]: ...

    @overload
    def lookup(
        self,
        path: str,
        *,
        node: NodeId | NodeAddress | None = None,
    ) -> ActorRef[Any] | None: ...

    def lookup[M](  # type: ignore[reportIncompatibleMethodOverride]
        self,
        path: str | ServiceKey[M],
        *,
        node: NodeId | NodeAddress | None = None,
        timeout: float = 5.0,
    ) -> ActorRef[Any] | None | Coroutine[Any, Any, Listing[M]]:
        """Look up an actor by path or find services by key.

        When *path* is a ``str``, performs a path-based lookup
        (optionally on a remote *node*).  When it is a ``ServiceKey``,
        queries the cluster receptionist and returns an awaitable
        ``Listing``.

        Parameters
        ----------
        path : str | ServiceKey
            Actor path (e.g. ``"/my-actor"``) or a typed service key.
        node : NodeId | NodeAddress | None
            Target node for path-based lookup.  ``None`` means local.
        timeout : float
            Seconds to wait for the receptionist (only for ``ServiceKey``).

        Returns
        -------
        ActorRef[Any] | None | Coroutine[Any, Any, Listing]
            For path lookups, the actor reference or ``None``.
            For service key lookups, an awaitable ``Listing``.

        Examples
        --------
        >>> ref = system.lookup("/my-actor")
        >>> remote = system.lookup("/my-actor", node="worker-1")
        >>> listing = await system.lookup(ServiceKey[PaymentMsg]("payment"))
        """
        match path:
            case ServiceKey() as key:
                return self._lookup_service(key, timeout=timeout)
            case str() as actor_path:
                normalized = (
                    f"/{actor_path}" if not actor_path.startswith("/") else actor_path
                )
                match node:
                    case None:
                        local_ref = super().lookup(actor_path)
                        if local_ref is None:
                            return None
                        addr = ActorAddress(
                            system=self._name,
                            path=normalized,
                            host=self._host,
                            port=self._port,
                        )
                        return self._remote.make_ref(addr)
                    case NodeAddress(host=host, port=port):
                        addr = ActorAddress(
                            system=self._name,
                            path=normalized,
                            host=host,
                            port=port,
                        )
                    case str() as node_id:
                        addr = ActorAddress(
                            system=self._name,
                            path=normalized,
                            node_id=node_id,
                        )
                return self._remote.make_ref(addr)

    async def _lookup_service[M](
        self,
        key: ServiceKey[M],
        *,
        timeout: float = 5.0,
    ) -> Listing[M]:
        return await self.ask(
            self.receptionist,
            lambda r: Find(key=key, reply_to=r),
            timeout=timeout,
        )

    def distributed(self, *, journal: EventJournal | None = None) -> Distributed:
        """Create a ``Distributed`` facade for this system.

        Parameters
        ----------
        journal : EventJournal | None
            If provided, data structures use event sourcing for persistence.

        Returns
        -------
        Distributed

        Examples
        --------
        >>> d = system.distributed()
        >>> counter = d.counter("hits")
        """
        from casty.distributed import Distributed

        return Distributed(self, journal=journal)

    async def shutdown(self) -> None:
        """Shut down the cluster node, closing transport and all actors."""
        self._logger.info("Shutting down")
        self._coordinators.clear()
        tcp_cell = self._root_cells.pop("_tcp_transport", None)
        await super().shutdown()
        if tcp_cell is not None:
            await tcp_cell.stop()
