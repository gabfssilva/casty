"""Cluster client for topology-aware routing without cluster membership.

``ClusterClient`` connects to a Casty cluster as an external observer,
receives topology updates (membership + shard allocations), and routes
``ShardEnvelope`` messages directly to the owning node — zero hops,
no proxy overhead, no cluster participation.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, cast
from uuid import uuid4

from casty.actor import Behavior, Behaviors
from casty.core.address import ActorAddress
from casty.cluster.state import NodeAddress, ServiceEntry
from casty.cluster.receptionist import Listing, ServiceInstance, ServiceKey
from casty.ref import ActorRef
from casty.remote.tcp_transport import (
    GetPort,
    InboundMessageHandler,
    RemoteTransport,
    TcpTransportConfig,
    TcpTransportMsg,
    tcp_transport,
)
from casty.core.scheduler import ScheduleOnce, scheduler
from casty.remote.serialization import PickleSerializer
from casty.cluster.coordinator import GetShardLocation, ShardLocation
from casty.cluster.envelope import ShardEnvelope, entity_shard
from casty.core.system import ActorSystem
from casty.cluster.topology import SubscribeTopology, TopologySnapshot
from casty.core.transport import LocalTransport

if TYPE_CHECKING:
    from casty.context import ActorContext

SUBSCRIPTION_TIMEOUT = 10.0


@dataclass(frozen=True)
class NodeFailed:
    """TCP send failure detected — evict allocations for this node."""

    host: str
    port: int


@dataclass(frozen=True)
class RetryShardQuery:
    """Delayed retry for a shard location query after a node failure."""

    shard_id: int


@dataclass(frozen=True)
class SubscriptionTimeout:
    """No topology update received within the liveness window."""


type ClientProxyMsg = (
    ShardEnvelope[Any] | ShardLocation | TopologySnapshot | NodeFailed | RetryShardQuery
)

type TopologySubscriberMsg = TopologySnapshot | RegisterProxy | SubscriptionTimeout


def client_proxy_behavior(
    *,
    shard_name: str,
    num_shards: int,
    remote_transport: RemoteTransport,
    system_name: str,
    logger: logging.Logger | None = None,
) -> Behavior[ClientProxyMsg]:
    """Shard proxy that always routes remotely.

    Uses allocation table from ``TopologySnapshot`` for known shards.
    On cache miss, queries the coordinator on the leader node via
    ``GetShardLocation`` (same protocol as the cluster's shard proxy).
    """

    log = logger or logging.getLogger(f"casty.client_proxy.{shard_name}")

    def _route(envelope: ShardEnvelope[Any], node: NodeAddress) -> None:
        remote_addr = ActorAddress(
            system=system_name,
            path=f"/_region-{shard_name}",
            host=node.host,
            port=node.port,
        )
        remote_transport.make_ref(remote_addr).tell(envelope)

    def _query_coordinator(
        shard_id: int,
        leader: NodeAddress,
        reply_to: ActorRef[ClientProxyMsg],
    ) -> None:
        coord_addr = ActorAddress(
            system=system_name,
            path=f"/_coord-{shard_name}",
            host=leader.host,
            port=leader.port,
        )
        coord_ref: ActorRef[GetShardLocation] = remote_transport.make_ref(coord_addr)
        coord_ref.tell(GetShardLocation(shard_id=shard_id, reply_to=reply_to))
        log.debug("Shard %d: requesting location from coordinator", shard_id)

    async def setup(ctx: ActorContext[ClientProxyMsg]) -> Behavior[ClientProxyMsg]:
        scheduler_ref = ctx.spawn(scheduler(), "_proxy_scheduler")

        def active(
            allocations: dict[int, NodeAddress],
            leader: NodeAddress | None,
            buffer: dict[int, tuple[ShardEnvelope[Any], ...]],
            failed_nodes: frozenset[NodeAddress],
        ) -> Behavior[ClientProxyMsg]:
            async def receive(
                ctx: ActorContext[ClientProxyMsg],
                msg: ClientProxyMsg,
            ) -> Behavior[ClientProxyMsg]:
                match msg:
                    case ShardEnvelope() as envelope:
                        shard_id = entity_shard(envelope.entity_id, num_shards)

                        if shard_id in allocations:
                            _route(envelope, allocations[shard_id])
                            return Behaviors.same()

                        existing = buffer.get(shard_id, ())
                        new_buf = {**buffer, shard_id: (*existing, envelope)}
                        if not existing and leader is not None:
                            _query_coordinator(shard_id, leader, ctx.self)
                        return active(allocations, leader, new_buf, failed_nodes)

                    case ShardLocation(shard_id=sid, node=node):
                        if node in failed_nodes:
                            log.debug(
                                "Shard %d -> %s:%d REJECTED (failed node)",
                                sid,
                                node.host,
                                node.port,
                            )
                            scheduler_ref.tell(
                                ScheduleOnce(
                                    key=f"retry-shard-{sid}",
                                    target=ctx.self,
                                    message=RetryShardQuery(shard_id=sid),
                                    delay=0.5,
                                )
                            )
                            return Behaviors.same()

                        new_allocs = {**allocations, sid: node}
                        buffered = buffer.get(sid, ())
                        log.debug(
                            "Shard %d -> %s:%d (flushed %d)",
                            sid,
                            node.host,
                            node.port,
                            len(buffered),
                        )
                        new_buf = {k: v for k, v in buffer.items() if k != sid}
                        for envelope in buffered:
                            _route(envelope, node)
                        return active(new_allocs, leader, new_buf, failed_nodes)

                    case TopologySnapshot() as snapshot:
                        allocs = snapshot.shard_allocations.get(shard_name, {})
                        merged = {
                            **allocations,
                            **{sid: alloc.primary for sid, alloc in allocs.items()},
                        }
                        unreachable = snapshot.unreachable
                        new_allocations = {
                            sid: node
                            for sid, node in merged.items()
                            if node not in unreachable
                        }
                        evicted = len(merged) - len(new_allocations)
                        if evicted:
                            log.info(
                                "Evicted %d shards routed to unreachable nodes",
                                evicted,
                            )
                        new_leader = snapshot.leader
                        still_buffered: dict[int, tuple[ShardEnvelope[Any], ...]] = {}
                        for sid, envelopes in buffer.items():
                            if sid in new_allocations:
                                for envelope in envelopes:
                                    _route(envelope, new_allocations[sid])
                            else:
                                still_buffered[sid] = envelopes
                        if new_leader is not None:
                            for sid in still_buffered:
                                if sid not in allocations:
                                    _query_coordinator(sid, new_leader, ctx.self)
                        healthy_addrs = {
                            m.address for m in snapshot.members
                        } - snapshot.unreachable
                        new_failed = failed_nodes - healthy_addrs
                        log.debug(
                            "Topology: %d shards, leader=%s (epoch=%d)",
                            len(new_allocations),
                            f"{new_leader.host}:{new_leader.port}"
                            if new_leader
                            else "none",
                            snapshot.allocation_epoch,
                        )
                        return active(
                            new_allocations, new_leader, still_buffered, new_failed
                        )

                    case NodeFailed(host=failed_host, port=failed_port):
                        failed = NodeAddress(host=failed_host, port=failed_port)
                        new_allocs = {
                            sid: node
                            for sid, node in allocations.items()
                            if node != failed
                        }
                        evicted_sids = [
                            sid for sid in allocations if sid not in new_allocs
                        ]
                        new_failed = failed_nodes | {failed}
                        if evicted_sids:
                            log.info(
                                "TCP failure %s:%d — evicted %d shards",
                                failed_host,
                                failed_port,
                                len(evicted_sids),
                            )
                        for sid in evicted_sids:
                            scheduler_ref.tell(
                                ScheduleOnce(
                                    key=f"retry-shard-{sid}",
                                    target=ctx.self,
                                    message=RetryShardQuery(shard_id=sid),
                                    delay=0.5,
                                )
                            )
                        new_leader = None if leader == failed else leader
                        return active(new_allocs, new_leader, buffer, new_failed)

                    case RetryShardQuery(shard_id=sid):
                        if sid in buffer and leader is not None:
                            _query_coordinator(sid, leader, ctx.self)
                        return Behaviors.same()

                    case _:
                        return Behaviors.same()

            return Behaviors.receive(receive)

        return active({}, None, {}, frozenset())

    return Behaviors.setup(setup)


@dataclass(frozen=True)
class RegisterProxy:
    """Register an additional proxy to receive topology updates."""

    proxy_ref: ActorRef[ClientProxyMsg]


def topology_subscriber(
    *,
    contact_points: list[tuple[str, int]],
    remote_transport: RemoteTransport,
    system_name: str,
    proxies: list[ActorRef[ClientProxyMsg]],
    logger: logging.Logger | None = None,
    on_snapshot: Callable[[TopologySnapshot], None] | None = None,
) -> Behavior[TopologySubscriberMsg]:
    """Actor that subscribes to topology updates from a cluster contact point.

    On startup, sends ``SubscribeTopology`` to the first reachable contact
    point's ``/_topology`` actor. Forwards received ``TopologySnapshot``
    to all registered proxies. Reconnects to the next contact point if
    no topology update is received within ``SUBSCRIPTION_TIMEOUT``.
    """

    if not contact_points:
        msg = "No contact points provided"
        raise ValueError(msg)

    log = logger or logging.getLogger("casty.topology_subscriber")

    static_contacts = tuple(contact_points)

    def _build_contacts(
        snapshot: TopologySnapshot | None,
    ) -> tuple[tuple[str, int], ...]:
        if snapshot is None:
            return static_contacts
        member_addrs = tuple(
            (m.address.host, m.address.port)
            for m in snapshot.members
            if (m.address.host, m.address.port) not in static_contacts
        )
        return (*static_contacts, *member_addrs)

    def _subscribe_to(
        idx: int,
        known: tuple[tuple[str, int], ...],
        ctx: ActorContext[TopologySubscriberMsg],
        sched: ActorRef[Any],
    ) -> None:
        host, port = known[idx % len(known)]
        log.info("Subscribing to topology from %s:%d", host, port)
        addr = ActorAddress(
            system=system_name,
            path="/_cluster/_topology",
            host=host,
            port=port,
        )
        ref: ActorRef[SubscribeTopology] = remote_transport.make_ref(addr)
        ref.tell(SubscribeTopology(reply_to=ctx.self))
        sched.tell(
            ScheduleOnce(
                key="liveness",
                target=ctx.self,
                message=SubscriptionTimeout(),
                delay=SUBSCRIPTION_TIMEOUT,
            )
        )

    def connected(
        registered_proxies: tuple[ActorRef[ClientProxyMsg], ...],
        last_snapshot: TopologySnapshot | None,
        contact_index: int,
        scheduler_ref: ActorRef[Any],
    ) -> Behavior[TopologySubscriberMsg]:
        async def receive(
            ctx: ActorContext[TopologySubscriberMsg],
            msg: TopologySubscriberMsg,
        ) -> Behavior[TopologySubscriberMsg]:
            match msg:
                case TopologySnapshot() as snapshot:
                    log.info(
                        "Topology: %d members, %d shard types, epoch=%d",
                        len(snapshot.members),
                        len(snapshot.shard_allocations),
                        snapshot.allocation_epoch,
                    )
                    if on_snapshot is not None:
                        on_snapshot(snapshot)
                    for proxy in registered_proxies:
                        proxy.tell(snapshot)
                    return connected(
                        registered_proxies, snapshot, contact_index, scheduler_ref
                    )

                case RegisterProxy(proxy_ref=proxy_ref):
                    if last_snapshot is not None:
                        proxy_ref.tell(last_snapshot)
                    return connected(
                        (*registered_proxies, proxy_ref),
                        last_snapshot,
                        contact_index,
                        scheduler_ref,
                    )

                case SubscriptionTimeout() if last_snapshot is None:
                    known = _build_contacts(last_snapshot)
                    next_idx = (contact_index + 1) % len(known)
                    host, port = known[next_idx]
                    log.warning(
                        "No topology in %.0fs, reconnecting to %s:%d (index=%d/%d)",
                        SUBSCRIPTION_TIMEOUT,
                        host,
                        port,
                        next_idx,
                        len(known),
                    )
                    _subscribe_to(next_idx, known, ctx, scheduler_ref)
                    return connected(
                        registered_proxies, last_snapshot, next_idx, scheduler_ref
                    )

                case SubscriptionTimeout():
                    return Behaviors.same()

                case _:
                    return Behaviors.same()

        return Behaviors.receive(receive)

    async def setup(
        ctx: ActorContext[TopologySubscriberMsg],
    ) -> Behavior[TopologySubscriberMsg]:
        sched = ctx.spawn(scheduler(), "_sub_scheduler")
        _subscribe_to(0, static_contacts, ctx, sched)
        return connected(tuple(proxies), None, 0, sched)

    return Behaviors.setup(setup)


class _RemoteActorSystem(ActorSystem):
    """ActorSystem that provides remote addressing for spawned actors."""

    def __init__(
        self,
        name: str,
        remote_transport: RemoteTransport | None,
        host: str,
        port: int,
    ) -> None:
        super().__init__(name=name)
        self._local_transport = LocalTransport()
        self._remote = remote_transport
        self._host = host
        self._port = port

    @property
    def local_transport(self) -> LocalTransport:
        return self._local_transport

    def __make_ref__[M](self, id: str, deliver: Callable[[Any], None]) -> ActorRef[M]:
        path = f"/{id}" if not id.startswith("/") else id
        self._local_transport.register(path, deliver)
        if self._remote is not None:
            addr = ActorAddress(
                system=self._name,
                path=path,
                host=self._host,
                port=self._port,
            )
            return self._remote.make_ref(addr)
        return super().__make_ref__(id, deliver)

    def set_remote(self, transport: RemoteTransport) -> None:
        self._remote = transport

    async def shutdown(self) -> None:
        tcp_cell = self._root_cells.pop("_tcp_transport", None)
        await super().shutdown()
        if tcp_cell is not None:
            await tcp_cell.stop()

    def set_port(self, port: int) -> None:
        self._port = port

    def set_host(self, host: str) -> None:
        self._host = host


class ClusterClient:
    """External client that routes messages to a Casty cluster.

    Connects to cluster contact points via TCP, subscribes to topology
    updates, and routes ``ShardEnvelope`` messages directly to the node
    owning each shard — zero hops, no cluster membership.

    Parameters
    ----------
    contact_points
        List of ``(host, port)`` for cluster nodes to connect to.
    system_name
        Logical name of the cluster's actor system (must match).
    client_host
        Advertised hostname for this client (for receiving responses).
    client_port
        Advertised port for this client (``0`` for auto-assignment).

    Examples
    --------
    >>> async with ClusterClient(
    ...     contact_points=[("10.0.1.10", 25520)],
    ...     system_name="my-cluster",
    ... ) as client:
    ...     counter = client.entity_ref("counter", num_shards=100)
    ...     counter.tell(ShardEnvelope("user-42", Increment(1)))
    ...     count = await client.ask(
    ...         counter,
    ...         lambda r: ShardEnvelope("user-42", GetCount(reply_to=r)),
    ...     )
    """

    def __init__(
        self,
        *,
        contact_points: list[tuple[str, int]],
        system_name: str,
        client_host: str = "127.0.0.1",
        client_port: int = 0,
        advertised_host: str | None = None,
        advertised_port: int | None = None,
        address_map: dict[tuple[str, int], tuple[str, int]] | None = None,
    ) -> None:
        self._contact_points = contact_points
        self._system_name = system_name
        self._client_host = client_host
        self._client_port = client_port
        self._advertised_host = advertised_host
        self._advertised_port = advertised_port
        self._address_map = address_map
        self._proxies: dict[str, ActorRef[ClientProxyMsg]] = {}
        self._subscriber_ref: ActorRef[TopologySubscriberMsg] | None = None
        self._last_snapshot: TopologySnapshot | None = None
        self._logger = logging.getLogger(f"casty.client.{system_name}")
        self._serializer = PickleSerializer()
        self._system: _RemoteActorSystem | None = None
        self._remote_transport: RemoteTransport | None = None

    async def __aenter__(self) -> ClusterClient:
        client_name = f"{self._system_name}-client"

        # 1. Start the actor system first
        self._system = _RemoteActorSystem(
            name=client_name,
            remote_transport=None,
            host=self._client_host,
            port=self._client_port,
        )
        await self._system.__aenter__()

        # 2. Spawn TCP transport actor (client_only — no server)
        effective_host = self._advertised_host or self._client_host
        effective_port = self._advertised_port or self._client_port
        tcp_config = TcpTransportConfig(
            host=self._client_host,
            port=self._client_port,
            self_address=(effective_host, effective_port),
            client_only=True,
            address_map=self._address_map or {},
        )
        inbound = InboundMessageHandler(
            local=self._system.local_transport,
            serializer=self._serializer,  # pyright: ignore[reportArgumentType]
            system_name=client_name,
        )
        tcp_ref: ActorRef[TcpTransportMsg] = self._system.spawn(
            tcp_transport(
                tcp_config,
                inbound,
                logger=logging.getLogger(f"casty.client.tcp.{self._system_name}"),
            ),
            "_tcp_transport",
        )

        # 3. Ask for actual port and update system address
        actual_port: int = await self._system.ask(
            tcp_ref,
            lambda r: GetPort(reply_to=r),
            timeout=5.0,
        )
        if actual_port != self._client_port:
            self._client_port = actual_port
            self._system.set_port(actual_port)
        if self._advertised_host is not None:
            self._system.set_host(self._advertised_host)

        # 4. Create RemoteTransport with the actor ref
        self._remote_transport = RemoteTransport(
            local=self._system.local_transport,
            tcp=tcp_ref,
            serializer=self._serializer,  # pyright: ignore[reportArgumentType]
            local_host=self._client_host,
            local_port=self._client_port,
            system_name=client_name,
            on_send_failure=self._on_send_failure,
            advertised_host=self._advertised_host,
            advertised_port=self._advertised_port,
            task_runner=self._system._ensure_task_runner(),  # pyright: ignore[reportPrivateUsage]
        )
        inbound.ref_factory = self._remote_transport.make_ref
        self._system.set_remote(self._remote_transport)

        self._subscriber_ref = self._system.spawn(
            topology_subscriber(
                contact_points=self._contact_points,
                remote_transport=self._remote_transport,
                system_name=self._system_name,
                proxies=[],
                logger=self._logger,
                on_snapshot=self._on_topology_update,
            ),
            "_topology_sub",
        )

        self._logger.info(
            "ClusterClient started on %s:%d → cluster %s",
            self._client_host,
            self._client_port,
            self._system_name,
        )
        return self

    async def __aexit__(self, *exc: object) -> None:
        if self._system is not None:
            await self._system.shutdown()

    def _on_send_failure(self, host: str, port: int) -> None:
        msg = NodeFailed(host=host, port=port)
        for proxy_ref in self._proxies.values():
            proxy_ref.tell(msg)

    def _on_topology_update(self, snapshot: TopologySnapshot) -> None:
        self._last_snapshot = snapshot

    def _require_started(self) -> tuple[_RemoteActorSystem, RemoteTransport]:
        if self._system is None or self._remote_transport is None:
            raise RuntimeError("ClusterClient is not started — use 'async with'")
        return self._system, self._remote_transport

    def lookup[M](self, key: ServiceKey[M]) -> Listing[M]:
        """Look up service instances registered in the cluster.

        Reads from the locally cached topology snapshot — no network
        round-trip required.  Returns an empty ``Listing`` if no
        topology has been received yet.

        Parameters
        ----------
        key
            Typed service key to search for.

        Returns
        -------
        Listing[M]
            Current set of instances matching the key.
        """
        _, remote_transport = self._require_started()
        entries: frozenset[ServiceEntry] = (
            self._last_snapshot.registry
            if self._last_snapshot is not None
            else frozenset()
        )
        instances = frozenset(
            ServiceInstance(
                ref=remote_transport.make_ref(
                    ActorAddress(
                        system=self._system_name,
                        path=entry.path,
                        host=entry.node.host,
                        port=entry.node.port,
                    )
                ),
                node=entry.node,
            )
            for entry in entries
            if entry.key == key.name
        )
        return Listing(key=key, instances=instances)

    def spawn[M](self, behavior: Behavior[M], name: str) -> ActorRef[M]:
        """Spawn a local actor on this client's internal actor system.

        The actor is remotely addressable — cluster nodes can send
        messages back to it via TCP.

        Parameters
        ----------
        behavior
            The behavior for the new actor.
        name
            Unique name for the actor.

        Returns
        -------
        ActorRef[M]
            A ref to the newly spawned actor.
        """
        system, _ = self._require_started()
        return system.spawn(behavior, name)

    def entity_ref(
        self, shard_type: str, *, num_shards: int
    ) -> ActorRef[ShardEnvelope[Any]]:
        """Get a ref that routes ``ShardEnvelope`` messages to the cluster.

        Creates a local proxy actor for the given shard type on first call.
        Subsequent calls with the same ``shard_type`` return the cached proxy.

        Parameters
        ----------
        shard_type
            Name of the sharded entity type (must match the cluster's name).
        num_shards
            Number of shards (must match the cluster's configuration).

        Returns
        -------
        ActorRef[ShardEnvelope[Any]]
            A ref that accepts ``ShardEnvelope`` messages and routes them
            to the correct cluster node.

        Examples
        --------
        >>> counter = client.entity_ref("counter", num_shards=100)
        >>> counter.tell(ShardEnvelope("entity-1", Increment(1)))
        """
        if shard_type in self._proxies:
            return cast(ActorRef[ShardEnvelope[Any]], self._proxies[shard_type])

        system, remote_transport = self._require_started()

        proxy_ref = system.spawn(
            client_proxy_behavior(
                shard_name=shard_type,
                num_shards=num_shards,
                remote_transport=remote_transport,
                system_name=self._system_name,
                logger=logging.getLogger(f"casty.client_proxy.{shard_type}"),
            ),
            f"_proxy-{shard_type}",
        )
        self._proxies[shard_type] = proxy_ref

        if self._subscriber_ref is not None:
            self._subscriber_ref.tell(RegisterProxy(proxy_ref=proxy_ref))

        return cast(ActorRef[ShardEnvelope[Any]], proxy_ref)

    async def ask[M, R](
        self,
        ref: ActorRef[M],
        msg_factory: Callable[[ActorRef[R]], M],
        *,
        timeout: float = 5.0,
    ) -> R:
        """Send a message and wait for a reply.

        Creates a temporary remotely-addressable ref so the cluster can
        respond directly to this client via TCP.

        Parameters
        ----------
        ref
            Target actor (typically from ``entity_ref``).
        msg_factory
            Factory that receives a reply-to ref and returns the message.
        timeout
            Maximum seconds to wait.

        Returns
        -------
        R
            The reply from the cluster actor.

        Raises
        ------
        asyncio.TimeoutError
            If no reply is received within ``timeout``.

        Examples
        --------
        >>> count = await client.ask(
        ...     counter_ref,
        ...     lambda r: ShardEnvelope("user-42", GetCount(reply_to=r)),
        ... )
        """
        system, _ = self._require_started()

        future: asyncio.Future[R] = asyncio.get_running_loop().create_future()
        ask_id = uuid4().hex
        temp_path = f"/_ask/{ask_id}"

        def on_reply(msg: Any) -> None:
            if not future.done():
                future.set_result(msg)

        temp_ref: ActorRef[R] = system.__make_ref__(temp_path, on_reply)
        try:
            ref.tell(msg_factory(temp_ref))
            return await asyncio.wait_for(future, timeout=timeout)
        finally:
            system.local_transport.unregister(temp_path)
