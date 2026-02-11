"""Cluster-aware actor system with automatic shard routing.

Extends the core ``ActorSystem`` with transparent sharding, broadcast actors,
and remote ``ask()`` support over TCP.  ``ClusteredActorSystem`` is the main
entry point for multi-node deployments.
"""
# src/casty/sharding.py
from __future__ import annotations

import asyncio
import hashlib
import logging
from dataclasses import dataclass
from typing import Any, TYPE_CHECKING, cast, overload
from uuid import uuid4

from casty.actor import (
    Behavior,
    Behaviors,
    BroadcastedBehavior,
    ShardedBehavior,
    SingletonBehavior,
)
from casty.address import ActorAddress
from casty.cluster_state import ClusterState, MemberStatus, NodeAddress, NodeId
from casty.mailbox import Mailbox
from casty.ref import ActorRef, BroadcastRef
from casty.cluster import (
    Cluster,
    ClusterConfig,
    RegisterBroadcast,
    RegisterCoordinator,
    RegisterSingletonManager,
    WaitForMembers,
)
from casty.remote_transport import RemoteTransport, TcpTransport
from casty.serialization import PickleSerializer
from casty.system import ActorSystem
from casty.transport import MessageTransport

if TYPE_CHECKING:
    from collections.abc import Callable

    from casty.config import CastyConfig
    from casty.distributed import Distributed
    from casty.journal import EventJournal

from casty.tls import Config as TlsConfig

from casty.shard_coordinator_actor import (
    CoordinatorMsg,
    GetShardLocation,
    LeastShardStrategy,
    PublishAllocations,
    RegisterRegion,
    ShardLocation,
    shard_coordinator_actor,
)


@dataclass(frozen=True)
class ShardEnvelope[M]:
    """Envelope that routes a message to a specific entity within a shard region.

    Wraps a message ``M`` together with an ``entity_id`` used for deterministic
    shard assignment.  The shard proxy computes the target shard from the
    ``entity_id`` and forwards the envelope to the owning node.

    Parameters
    ----------
    entity_id : str
        Logical identifier of the target entity.
    message : M
        The payload message delivered to the entity actor.

    Examples
    --------
    >>> ref.tell(ShardEnvelope("user-42", Deposit(amount=100)))
    """
    entity_id: str
    message: M


def entity_shard(entity_id: str, num_shards: int) -> int:
    """Deterministic shard assignment -- consistent across processes.

    Parameters
    ----------
    entity_id : str
        The entity identifier to hash.
    num_shards : int
        Total number of shards.

    Returns
    -------
    int
        Shard index in ``[0, num_shards)``.

    Examples
    --------
    >>> entity_shard("user-42", 100)
    72
    """
    digest = hashlib.md5(entity_id.encode(), usedforsecurity=False).digest()
    return int.from_bytes(digest[:4], "big") % num_shards


# ---------------------------------------------------------------------------
# Shard proxy behavior — routes ShardEnvelope to the right region
# ---------------------------------------------------------------------------


def shard_proxy_behavior(
    *,
    coordinator: ActorRef[CoordinatorMsg],
    local_region: ActorRef[Any],
    self_node: NodeAddress,
    shard_name: str,
    num_shards: int,
    remote_transport: RemoteTransport | None,
    system_name: str,
    logger: logging.Logger | None = None,
) -> Behavior[Any]:
    """Proxy actor that routes ShardEnvelopes to the correct region.

    - Computes shard_id from entity_id
    - Caches shard->node mappings
    - Buffers messages while waiting for coordinator response
    """
    log = logger or logging.getLogger(f"casty.shard_proxy.{system_name}")

    def active(
        shard_cache: dict[int, NodeAddress],
        buffer: dict[int, list[ShardEnvelope[Any]]],
    ) -> Behavior[Any]:
        async def receive(ctx: Any, msg: Any) -> Any:
            match msg:
                case ShardEnvelope():
                    envelope = cast(ShardEnvelope[Any], msg)
                    shard_id = entity_shard(envelope.entity_id, num_shards)

                    if shard_id in shard_cache:
                        node = shard_cache[shard_id]
                        if node == self_node:
                            local_region.tell(envelope)
                        elif remote_transport is not None:
                            remote_addr = ActorAddress(
                                system=system_name,
                                path=f"/_region-{shard_name}",
                                host=node.host,
                                port=node.port,
                            )
                            remote_ref: ActorRef[Any] = (
                                remote_transport.make_ref(remote_addr)
                            )
                            remote_ref.tell(envelope)
                        return Behaviors.same()

                    existing_buf = buffer.get(shard_id, [])
                    new_buf = [*existing_buf, envelope]
                    new_buffer = {**buffer, shard_id: new_buf}
                    if not existing_buf:
                        log.debug("Shard %d: requesting location (entity=%s)", shard_id, envelope.entity_id)
                        coordinator.tell(
                            GetShardLocation(
                                shard_id=shard_id, reply_to=ctx.self
                            )
                        )
                    return active(shard_cache, new_buffer)

                case ShardLocation(shard_id=sid, node=node):
                    new_cache = {**shard_cache, sid: node}
                    buffered = buffer.get(sid, [])
                    log.debug("Shard %d -> %s:%d (flushed %d)", sid, node.host, node.port, len(buffered))
                    new_buffer = {k: v for k, v in buffer.items() if k != sid}
                    for envelope in buffered:
                        if node == self_node:
                            local_region.tell(envelope)
                        elif remote_transport is not None:
                            remote_addr = ActorAddress(
                                system=system_name,
                                path=f"/_region-{shard_name}",
                                host=node.host,
                                port=node.port,
                            )
                            remote_ref: ActorRef[Any] = (
                                remote_transport.make_ref(remote_addr)
                            )
                            remote_ref.tell(envelope)
                    return active(new_cache, new_buffer)

                case _:
                    return Behaviors.same()

        return Behaviors.receive(receive)

    return active({}, {})


# ---------------------------------------------------------------------------
# Broadcast proxy behavior — fans out messages to all cluster members
# ---------------------------------------------------------------------------


def broadcast_proxy_behavior(
    *,
    local_ref: ActorRef[Any],
    self_node: NodeAddress,
    bcast_name: str,
    remote_transport: RemoteTransport,
    system_name: str,
) -> Behavior[Any]:
    """Proxy that fans out every message to all cluster members.

    Receives ``ClusterState`` from the cluster actor to track membership.
    Any other message is forwarded to every ``up`` member — locally via
    ``local_ref.tell()`` and remotely via ``remote_transport.make_ref()``.
    """

    def active(members: frozenset[NodeAddress]) -> Behavior[Any]:
        async def receive(ctx: Any, msg: Any) -> Behavior[Any]:
            match msg:
                case ClusterState() as state:
                    up = frozenset(
                        m.address for m in state.members if m.status == MemberStatus.up
                    )
                    return active(up)
                case _:
                    for node in members:
                        if node == self_node:
                            local_ref.tell(msg)
                        else:
                            addr = ActorAddress(
                                system=system_name,
                                path=f"/_bcast-{bcast_name}",
                                host=node.host,
                                port=node.port,
                            )
                            remote_ref: ActorRef[Any] = remote_transport.make_ref(addr)
                            remote_ref.tell(msg)
                    return Behaviors.same()

        return Behaviors.receive(receive)

    return active(frozenset({self_node}))


# ---------------------------------------------------------------------------
# ClusteredActorSystem
# ---------------------------------------------------------------------------


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
    seed_nodes : list[tuple[str, int]] | None
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
        seed_nodes: list[tuple[str, int]] | None = None,
        roles: frozenset[str] = frozenset(),
        bind_host: str | None = None,
        config: CastyConfig | None = None,
        tls: TlsConfig | None = None,
        required_quorum: int | None = None,
    ) -> None:
        super().__init__(name=name, config=config)
        self._host = host
        self._port: int = port
        self._node_id = node_id
        self._bind_host = bind_host or host
        self._seed_nodes = seed_nodes or []
        self._roles = roles
        self._self_node = NodeAddress(host=host, port=port)
        self._coordinators: dict[str, ActorRef[CoordinatorMsg]] = {}
        self._required_quorum = required_quorum
        self._logger = logging.getLogger(f"casty.sharding.{name}")

        # Serialization + transport
        self._tcp_transport = TcpTransport(
            self._bind_host,
            port,
            logger=logging.getLogger(f"casty.remote_transport.{name}"),
            server_ssl=tls.server_context if tls else None,
            client_ssl=tls.client_context if tls else None,
        )
        self._serializer = PickleSerializer()
        self._remote_transport = RemoteTransport(
            local=self._local_transport,
            tcp=self._tcp_transport,
            serializer=self._serializer,
            local_host=host,
            local_port=port,
            system_name=name,
        )

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
        seed_nodes: list[tuple[str, int]] | None = None,
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
        await self._remote_transport.start()
        # Update actual port (handles port=0 for tests)
        actual_port: int = self._tcp_transport.port
        if actual_port != self._port:
            self._port = actual_port
            self._self_node = NodeAddress(host=self._host, port=actual_port)
            self._remote_transport.set_local_port(actual_port)
        try:
            await super().__aenter__()

            # Wire task runner into remote transport now that the system is live
            self._remote_transport.set_task_runner(self._ensure_task_runner())

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
                system_name=self._name,
                gossip_interval=cfg.gossip.interval,
                gossip_fanout=cfg.gossip.fanout,
                heartbeat_interval=cfg.heartbeat.interval,
                availability_interval=cfg.heartbeat.availability_check_interval,
                failure_detector_config=cfg.failure_detector,
            )
            await self._cluster.start()
        except BaseException:
            await self._remote_transport.stop()
            raise

        self._remote_transport.set_local_node_id(self._node_id)
        self._remote_transport.update_node_index(
            {self._node_id: (self._host, self._port)}
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

    def _get_ref_transport(self) -> MessageTransport | None:
        return self._remote_transport

    def _get_ref_host(self) -> str | None:
        return self._host

    def _get_ref_port(self) -> int | None:
        return self._port

    def _get_ref_node_id(self) -> str | None:
        return self._node_id

    @overload
    def spawn[M](
        self, behavior: BroadcastedBehavior[M], name: str
    ) -> BroadcastRef[M]: ...

    @overload
    def spawn[M](
        self, behavior: ShardedBehavior[M], name: str
    ) -> ActorRef[ShardEnvelope[M]]: ...

    @overload
    def spawn[M](
        self, behavior: SingletonBehavior[M], name: str
    ) -> ActorRef[M]: ...

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
        behavior: BroadcastedBehavior[M] | ShardedBehavior[M] | SingletonBehavior[M] | Behavior[M],
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
        from casty.shard_region_actor import shard_region_actor

        self._logger.info("Sharded entity: %s (shards=%d)", name, sharded.num_shards)

        # Build available nodes from self + seed_nodes
        available_nodes = frozenset(
            {self._self_node}
            | {NodeAddress(host=h, port=p) for h, p in self._seed_nodes}
        )

        # Coordinator: deterministic — lives on first seed node
        coordinator = self._get_or_create_coordinator(
            name, sharded, available_nodes
        )
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
            ),
            name,
        )

        return cast(ActorRef[ShardEnvelope[M]], proxy_ref)

    def _spawn_singleton[M](
        self, singleton: SingletonBehavior[M], name: str
    ) -> ActorRef[M]:
        from casty.singleton import singleton_manager_actor

        self._logger.info("Singleton: %s", name)

        manager_ref: ActorRef[Any] = super().spawn(
            singleton_manager_actor(
                factory=singleton.factory,
                name=name,
                remote_transport=self._remote_transport,
                system_name=self._name,
                logger=logging.getLogger(f"casty.singleton.{self._name}"),
            ),
            f"_singleton-{name}",
        )

        self._cluster.ref.tell(RegisterSingletonManager(  # type: ignore[arg-type]
            name=name,
            manager_ref=manager_ref,
        ))

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
            ),
            name,
        )

        # Register proxy with cluster actor so it receives ClusterState updates
        self._cluster.ref.tell(RegisterBroadcast(  # type: ignore[arg-type]
            name=name,
            proxy_ref=proxy_ref,
        ))

        return BroadcastRef[M](
            address=proxy_ref.address,
            _transport=proxy_ref._transport,  # pyright: ignore[reportPrivateUsage]
        )

    def _get_or_create_coordinator(
        self,
        name: str,
        sharded: ShardedBehavior[Any],
        available_nodes: frozenset[NodeAddress],
    ) -> ActorRef[CoordinatorMsg]:
        if name in self._coordinators:
            return self._coordinators[name]

        publish_ref = cast(ActorRef[PublishAllocations], self._cluster.ref)

        coord_ref = super().spawn(
            shard_coordinator_actor(
                strategy=LeastShardStrategy(),
                available_nodes=available_nodes,
                replication=sharded.replication,
                shard_type=name,
                publish_ref=publish_ref,
                remote_transport=self._remote_transport,
                system_name=self._name,
                logger=logging.getLogger(f"casty.coordinator.{self._name}"),
            ),
            f"_coord-{name}",
        )
        self._coordinators[name] = coord_ref

        self._cluster.ref.tell(RegisterCoordinator(  # type: ignore[arg-type]
            shard_name=name,
            coord_ref=coord_ref,
        ))

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
            bcast_result: tuple[R, ...] = await self._ask_broadcast(ref, msg_factory, timeout=timeout)  # type: ignore[assignment]
            return bcast_result

        future: asyncio.Future[R] = asyncio.get_running_loop().create_future()
        ask_id = uuid4().hex
        temp_path = f"/_ask/{ask_id}"

        def on_reply(msg: Any) -> None:
            if not future.done():
                future.set_result(msg)

        self._local_transport.register(temp_path, on_reply)
        try:
            temp_ref: ActorRef[R] = ActorRef(
                address=ActorAddress(
                    system=self._name,
                    path=temp_path,
                    host=self._host,
                    port=self._port,
                ),
                _transport=self._remote_transport,
            )
            message = msg_factory(temp_ref)
            ref.tell(message)
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

        self._local_transport.register(temp_path, on_reply)
        try:
            temp_ref: ActorRef[R] = ActorRef(
                address=ActorAddress(
                    system=self._name,
                    path=temp_path,
                    host=self._host,
                    port=self._port,
                ),
                _transport=self._remote_transport,
            )
            message = msg_factory(temp_ref)
            ref.tell(message)
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
        self._remote_transport.update_node_index(
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
            lambda r: ShardEnvelope(name, BarrierArrive(node=node_id, expected=n, reply_to=r)),
            timeout=timeout,
        )

    def lookup(
        self, path: str, *, node: NodeId | NodeAddress | None = None,
    ) -> ActorRef[Any] | None:
        """Look up an actor by path, optionally on a remote node.

        When *node* is ``None``, performs a local lookup.
        When *node* is a ``NodeId``, constructs an address with ``node_id``
        and lets the transport resolve it.
        When *node* is a ``NodeAddress``, constructs a host/port address.

        Parameters
        ----------
        path : str
            Actor path (e.g. ``"/my-actor"``).
        node : NodeId | NodeAddress | None
            Target node.  ``None`` means local.

        Returns
        -------
        ActorRef[Any] | None

        Examples
        --------
        >>> ref = system.lookup("/my-actor")
        >>> remote = system.lookup("/my-actor", node="worker-1")
        >>> remote = system.lookup("/my-actor", node=NodeAddress("10.0.0.2", 25520))
        """
        if node is None:
            return super().lookup(path)
        if isinstance(node, NodeAddress):
            addr = ActorAddress(
                system=self._name, path=path,
                host=node.host, port=node.port,
            )
        else:
            addr = ActorAddress(
                system=self._name, path=path, node_id=node,
            )
        return self._remote_transport.make_ref(addr)

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
        await super().shutdown()
        await self._remote_transport.stop()
