# src/casty/sharding.py
from __future__ import annotations

import asyncio
import hashlib
import logging
import ssl
from dataclasses import dataclass
from typing import Any, TYPE_CHECKING, cast, overload
from uuid import uuid4

from casty.actor import Behavior, Behaviors, BroadcastedBehavior, ShardedBehavior
from casty.address import ActorAddress
from casty.cluster_state import ClusterState, MemberStatus, NodeAddress
from casty.mailbox import Mailbox
from casty.ref import ActorRef, BroadcastRef
from casty.cluster import (
    Cluster,
    ClusterConfig,
    RegisterBroadcast,
    RegisterCoordinator,
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
    entity_id: str
    message: M


def entity_shard(entity_id: str, num_shards: int) -> int:
    """Deterministic shard assignment — consistent across processes."""
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
    def __init__(
        self,
        *,
        name: str,
        host: str,
        port: int,
        seed_nodes: list[tuple[str, int]] | None = None,
        roles: frozenset[str] = frozenset(),
        bind_host: str | None = None,
        config: CastyConfig | None = None,
        server_ssl: ssl.SSLContext | None = None,
        client_ssl: ssl.SSLContext | None = None,
        required_quorum: int | None = None,
    ) -> None:
        super().__init__(name=name, config=config)
        self._host = host
        self._port: int = port
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
            server_ssl=server_ssl,
            client_ssl=client_ssl,
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
        return self._self_node

    @classmethod
    def from_config(
        cls,
        config: CastyConfig,
        *,
        host: str | None = None,
        port: int | None = None,
        seed_nodes: list[tuple[str, int]] | None = None,
        bind_host: str | None = None,
        server_ssl: ssl.SSLContext | None = None,
        client_ssl: ssl.SSLContext | None = None,
        required_quorum: int | None = None,
    ) -> ClusteredActorSystem:
        cluster = config.cluster
        if cluster is None:
            msg = "CastyConfig has no [cluster] section"
            raise ValueError(msg)

        return cls(
            name=config.system_name,
            host=host or cluster.host,
            port=port if port is not None else cluster.port,
            seed_nodes=seed_nodes if seed_nodes is not None else cluster.seed_nodes,
            roles=cluster.roles,
            bind_host=bind_host,
            config=config,
            server_ssl=server_ssl,
            client_ssl=client_ssl,
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

            # Start cluster membership (gossip, heartbeat, failure detection)
            cluster_config = ClusterConfig(
                host=self._host,
                port=self._port,
                seed_nodes=self._seed_nodes,
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
                heartbeat_interval=cfg.heartbeat.interval,
                availability_interval=cfg.heartbeat.availability_check_interval,
                failure_detector_config=cfg.failure_detector,
            )
            await self._cluster.start()
        except BaseException:
            await self._remote_transport.stop()
            raise

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
        self,
        behavior: Behavior[M],
        name: str,
        *,
        mailbox: Mailbox[M] | None = None,
    ) -> ActorRef[M]: ...

    def spawn[M](
        self,
        behavior: BroadcastedBehavior[M] | ShardedBehavior[M] | Behavior[M],
        name: str,
        *,
        mailbox: Mailbox[M] | None = None,
    ) -> BroadcastRef[M] | ActorRef[ShardEnvelope[M]] | ActorRef[M]:
        match behavior:
            case BroadcastedBehavior():
                return self._spawn_broadcasted(behavior, name)
            case ShardedBehavior():
                return self._spawn_sharded(behavior, name)
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
        """Query the current cluster membership state."""
        return await self._cluster.get_state(timeout=timeout)

    async def wait_for(self, n: int, *, timeout: float = 60.0) -> ClusterState:
        """Block until at least *n* members have status ``up``."""
        return await self.ask(
            self._cluster.ref,
            lambda r: WaitForMembers(n=n, reply_to=r),
            timeout=timeout,
        )

    async def barrier(self, name: str, n: int, *, timeout: float = 60.0) -> None:
        """Distributed barrier — blocks until *n* nodes have reached this point."""
        from casty.distributed.barrier import BarrierArrive

        node_id = f"{self._host}:{self._port}"
        await self.ask(
            self._barrier_proxy,
            lambda r: ShardEnvelope(name, BarrierArrive(node=node_id, expected=n, reply_to=r)),
            timeout=timeout,
        )

    def lookup(
        self, path: str, *, node: NodeAddress | None = None,
    ) -> ActorRef[Any] | None:
        """Look up an actor by path, optionally on a remote node.

        When *node* is ``None`` or points to this node, performs a local
        lookup (may return ``None`` if no actor exists at *path*).
        When *node* points to a different cluster member, constructs a
        remote ``ActorRef`` — the actor is assumed to exist.
        """
        if node is None or node == self._self_node:
            return super().lookup(path)
        addr = ActorAddress(
            system=self._name, path=path, host=node.host, port=node.port,
        )
        return self._remote_transport.make_ref(addr)

    def distributed(self, *, journal: EventJournal | None = None) -> Distributed:
        """Create a :class:`Distributed` facade for this system."""
        from casty.distributed import Distributed

        return Distributed(self, journal=journal)

    async def shutdown(self) -> None:
        self._logger.info("Shutting down")
        self._coordinators.clear()
        await super().shutdown()
        await self._remote_transport.stop()
