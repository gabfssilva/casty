# src/casty/sharding.py
from __future__ import annotations

import asyncio
import hashlib
import logging
from dataclasses import dataclass
from typing import Any, TYPE_CHECKING, cast, overload
from uuid import uuid4

from casty.actor import Behavior, Behaviors, ShardedBehavior
from casty.address import ActorAddress
from casty.cluster_state import ClusterState, NodeAddress
from casty.mailbox import Mailbox
from casty.ref import ActorRef
from casty.cluster import Cluster, ClusterConfig, RegisterCoordinator, WaitForMembers
from casty.remote_transport import RemoteTransport, TcpTransport
from casty.serialization import JsonSerializer, TypeRegistry
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
    ) -> None:
        super().__init__(name=name, config=config)
        self._host = host
        self._port: int = port
        self._bind_host = bind_host or host
        self._seed_nodes = seed_nodes or []
        self._roles = roles
        self._self_node = NodeAddress(host=host, port=port)
        self._coordinators: dict[str, ActorRef[CoordinatorMsg]] = {}
        self._logger = logging.getLogger(f"casty.sharding.{name}")

        # Serialization + transport
        self._type_registry = TypeRegistry()
        self._tcp_transport = TcpTransport(self._bind_host, port, logger=logging.getLogger(f"casty.remote_transport.{name}"))
        self._serializer = JsonSerializer(self._type_registry)
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

    @property
    def type_registry(self) -> TypeRegistry:
        return self._type_registry

    @classmethod
    def from_config(
        cls,
        config: CastyConfig,
        *,
        host: str | None = None,
        port: int | None = None,
        seed_nodes: list[tuple[str, int]] | None = None,
        bind_host: str | None = None,
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

        self._logger.info("Started on %s:%d", self._host, self._port)
        return self

    def _get_ref_transport(self) -> MessageTransport | None:
        return self._remote_transport

    def _get_ref_host(self) -> str | None:
        return self._host

    def _get_ref_port(self) -> int | None:
        return self._port

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
        behavior: ShardedBehavior[M] | Behavior[M],
        name: str,
        *,
        mailbox: Mailbox[M] | None = None,
    ) -> ActorRef[ShardEnvelope[M]] | ActorRef[M]:
        match behavior:
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

    async def ask[M, R](
        self,
        ref: ActorRef[M],
        msg_factory: Callable[[ActorRef[R]], M],
        *,
        timeout: float,
    ) -> R:
        """Ask with remote-addressable temp reply ref."""
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

    def distributed(self, *, journal: EventJournal | None = None) -> Distributed:
        """Create a :class:`Distributed` facade for this system."""
        from casty.distributed import Distributed

        return Distributed(self, journal=journal)

    async def shutdown(self) -> None:
        self._logger.info("Shutting down")
        self._coordinators.clear()
        await super().shutdown()
        await self._remote_transport.stop()
