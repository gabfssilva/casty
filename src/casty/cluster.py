"""Cluster membership management and coordination.

Provides ``ClusterConfig`` for cluster setup and ``Cluster`` as a thin wrapper
around the internal cluster actor.  The cluster actor orchestrates gossip-based
membership, heartbeat failure detection, and leader-driven member promotion.
"""

# src/casty/cluster.py
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, TYPE_CHECKING

from casty.actor import Behavior, Behaviors
from casty.cluster_state import (
    ClusterState,
    Member,
    MemberStatus,
    NodeAddress,
    NodeId,
)
from casty.gossip_actor import (
    DownMember,
    GossipMsg,
    GossipTick,
    GetClusterState,
    JoinRequest,
    PromoteMember,
    UpdateShardAllocations,
    gossip_actor,
)
from casty.heartbeat_actor import (
    CheckAvailability,
    HeartbeatMsg,
    HeartbeatTick,
    NodeUnreachable,
    heartbeat_actor,
)
from casty.shard_coordinator_actor import (
    CoordinatorMsg,
    NodeDown,
    PublishAllocations,
    SetRole,
    SyncAllocations,
    UpdateTopology,
)
from casty.scheduler import (
    CancelSchedule,
    ScheduleOnce,
    SchedulerMsg,
    ScheduleTick,
)
from casty.address import ActorAddress
from casty.failure_detector import PhiAccrualFailureDetector
from casty.ref import ActorRef

if TYPE_CHECKING:
    from casty.config import FailureDetectorConfig
    from casty.context import ActorContext
    from casty.remote_transport import RemoteTransport
    from casty.system import ActorSystem


# --- ClusterCmd messages ---


@dataclass(frozen=True)
class GetState:
    reply_to: ActorRef[ClusterState]


@dataclass(frozen=True)
class RegisterCoordinator:
    """Registers a replicated coordinator with the cluster actor."""

    shard_name: str
    coord_ref: ActorRef[CoordinatorMsg]


@dataclass(frozen=True)
class WaitForMembers:
    n: int
    reply_to: ActorRef[ClusterState]


@dataclass(frozen=True)
class RegisterBroadcast:
    name: str
    proxy_ref: ActorRef[Any]


@dataclass(frozen=True)
class RegisterSingletonManager:
    """Registers a singleton manager with the cluster actor."""

    name: str
    manager_ref: ActorRef[Any]


@dataclass(frozen=True)
class JoinRetry:
    pass


type ClusterCmd = (
    GetState | WaitForMembers | NodeUnreachable | PublishAllocations
    | RegisterCoordinator | RegisterBroadcast | RegisterSingletonManager
    | JoinRetry | ClusterState
)


# --- ClusterConfig ---


@dataclass(frozen=True)
class ClusterConfig:
    """Configuration for joining or forming a cluster.

    Parameters
    ----------
    host : str
        Bind address for this node.
    port : int
        TCP port for this node.
    seed_nodes : list[tuple[str, int]]
        Initial contact points for cluster formation.  An empty list means
        this node forms a new single-node cluster.
    node_id : NodeId
        Human-readable identifier for this node (e.g. ``"worker-1"``).
    roles : frozenset[str]
        Roles to advertise for this node.

    Examples
    --------
    >>> cfg = ClusterConfig("127.0.0.1", 2551, [("127.0.0.1", 2552)], node_id="node-1")
    >>> cfg.host, cfg.port
    ('127.0.0.1', 2551)
    """

    host: str
    port: int
    seed_nodes: list[tuple[str, int]]
    node_id: NodeId
    roles: frozenset[str] = field(default_factory=lambda: frozenset[str]())


# --- cluster_actor behavior ---


def cluster_receive(
    *,
    gossip_ref: ActorRef[GossipMsg],
    heartbeat_ref: ActorRef[HeartbeatMsg],
    scheduler_ref: ActorRef[SchedulerMsg],
    self_node: NodeAddress,
    node_id: NodeId,
    roles: frozenset[str],
    seed_refs: tuple[ActorRef[GossipMsg], ...],
    logger: logging.Logger,
    coordinators: dict[str, ActorRef[CoordinatorMsg]],
    broadcast_proxies: dict[str, ActorRef[Any]],
    singleton_managers: dict[str, ActorRef[Any]],
    cluster_state: ClusterState | None,
    waiters: tuple[WaitForMembers, ...] = (),
) -> Behavior[ClusterCmd]:
    """Pure receive behavior — all state in parameters, returns new behavior."""

    def _up_count(state: ClusterState) -> int:
        return sum(1 for m in state.members if m.status == MemberStatus.up)

    def _notify_waiters(
        state: ClusterState, pending: tuple[WaitForMembers, ...],
    ) -> tuple[WaitForMembers, ...]:
        up = _up_count(state)
        remaining: list[WaitForMembers] = []
        for w in pending:
            if up >= w.n:
                w.reply_to.tell(state)
            else:
                remaining.append(w)
        return tuple(remaining)

    def _next(
        *,
        new_coordinators: dict[str, ActorRef[CoordinatorMsg]] | None = None,
        new_broadcast_proxies: dict[str, ActorRef[Any]] | None = None,
        new_singleton_managers: dict[str, ActorRef[Any]] | None = None,
        new_cluster_state: ClusterState | None = None,
        new_waiters: tuple[WaitForMembers, ...] | None = None,
    ) -> Behavior[ClusterCmd]:
        return cluster_receive(
            gossip_ref=gossip_ref,
            heartbeat_ref=heartbeat_ref,
            scheduler_ref=scheduler_ref,
            self_node=self_node,
            node_id=node_id,
            roles=roles,
            seed_refs=seed_refs,
            logger=logger,
            coordinators=new_coordinators
            if new_coordinators is not None
            else coordinators,
            broadcast_proxies=new_broadcast_proxies
            if new_broadcast_proxies is not None
            else broadcast_proxies,
            singleton_managers=new_singleton_managers
            if new_singleton_managers is not None
            else singleton_managers,
            cluster_state=new_cluster_state
            if new_cluster_state is not None
            else cluster_state,
            waiters=new_waiters
            if new_waiters is not None
            else waiters,
        )

    async def receive(
        ctx: ActorContext[ClusterCmd], msg: ClusterCmd
    ) -> Behavior[ClusterCmd]:
        match msg:
            case GetState(reply_to):
                gossip_ref.tell(GetClusterState(reply_to=reply_to))
                return Behaviors.same()

            case WaitForMembers(n, reply_to):
                if cluster_state is not None and _up_count(cluster_state) >= n:
                    reply_to.tell(cluster_state)
                    return Behaviors.same()
                return _next(new_waiters=(*waiters, msg))

            case NodeUnreachable(node):
                logger.warning("Node unreachable: %s:%d", node.host, node.port)
                gossip_ref.tell(DownMember(address=node))
                for coord_ref in coordinators.values():
                    coord_ref.tell(NodeDown(node=node))
                return Behaviors.same()

            case PublishAllocations(shard_type, allocations, epoch):
                gossip_ref.tell(
                    UpdateShardAllocations(
                        shard_type=shard_type,
                        allocations=allocations,
                        epoch=epoch,
                    )
                )
                return Behaviors.same()

            case RegisterCoordinator(shard_name=shard_name, coord_ref=coord_ref):
                logger.info("Registered coordinator: %s", shard_name)
                new_coords = {**coordinators, shard_name: coord_ref}
                if cluster_state is not None:
                    is_leader = cluster_state.leader == self_node
                    coord_ref.tell(
                        SetRole(
                            is_leader=is_leader,
                            leader_node=cluster_state.leader,
                        )
                    )
                    allocs = cluster_state.shard_allocations.get(shard_name, {})
                    coord_ref.tell(
                        SyncAllocations(
                            allocations=allocs,
                            epoch=cluster_state.allocation_epoch,
                        )
                    )
                return _next(new_coordinators=new_coords)

            case RegisterBroadcast(name=name, proxy_ref=proxy_ref):
                logger.info("Registered broadcast: %s", name)
                new_proxies = {**broadcast_proxies, name: proxy_ref}
                if cluster_state is not None:
                    proxy_ref.tell(cluster_state)
                return _next(new_broadcast_proxies=new_proxies)

            case RegisterSingletonManager(name=sm_name, manager_ref=manager_ref):
                logger.info("Registered singleton manager: %s", sm_name)
                new_managers = {**singleton_managers, sm_name: manager_ref}
                if cluster_state is not None:
                    is_leader = cluster_state.leader == self_node
                    manager_ref.tell(
                        SetRole(
                            is_leader=is_leader,
                            leader_node=cluster_state.leader,
                        )
                    )
                return _next(new_singleton_managers=new_managers)

            case ClusterState() as state:
                if state.diff(cluster_state):
                    logger.info("Cluster topology update: %s", state)

                # Leader promotes joining → up only when state is converged
                if state.leader == self_node and state.is_converged:
                    for m in state.members:
                        if m.status == MemberStatus.joining:
                            logger.info("Promoting member %s:%d", m.address.host, m.address.port)
                            gossip_ref.tell(PromoteMember(address=m.address))

                members = frozenset(
                    m.address
                    for m in state.members
                    if m.status in (MemberStatus.up, MemberStatus.joining)
                )
                heartbeat_ref.tell(HeartbeatTick(members=members))

                up_nodes = frozenset(
                    m.address for m in state.members if m.status == MemberStatus.up
                )
                for shard_name, coord_ref in coordinators.items():
                    is_leader = state.leader == self_node
                    coord_ref.tell(
                        SetRole(
                            is_leader=is_leader,
                            leader_node=state.leader,
                        )
                    )
                    allocs = state.shard_allocations.get(shard_name, {})
                    coord_ref.tell(
                        SyncAllocations(
                            allocations=allocs,
                            epoch=state.allocation_epoch,
                        )
                    )
                    coord_ref.tell(UpdateTopology(available_nodes=up_nodes))
                is_leader_role = SetRole(
                    is_leader=state.leader == self_node,
                    leader_node=state.leader,
                )
                for manager_ref in singleton_managers.values():
                    manager_ref.tell(is_leader_role)
                for proxy_ref in broadcast_proxies.values():
                    proxy_ref.tell(state)
                remaining = _notify_waiters(state, waiters)
                return _next(new_cluster_state=state, new_waiters=remaining)

            case JoinRetry():
                if cluster_state is None or len(cluster_state.members) <= 1:
                    logger.debug("Join retry (%d seeds)", len(seed_refs))
                    for seed_ref in seed_refs:
                        seed_ref.tell(
                            JoinRequest(
                                node=self_node,
                                roles=roles,
                                node_id=node_id,
                                reply_to=gossip_ref,  # type: ignore[arg-type]
                            )
                        )
                    scheduler_ref.tell(
                        ScheduleOnce(
                            key="join-retry",
                            target=ctx.self,
                            message=JoinRetry(),
                            delay=1.0,
                        )
                    )
                return Behaviors.same()

            case _:
                return Behaviors.same()

    return Behaviors.receive(receive)


def cluster_actor(
    *,
    config: ClusterConfig,
    scheduler_ref: ActorRef[SchedulerMsg],
    remote_transport: RemoteTransport | None = None,
    system_name: str = "",
    gossip_interval: float = 1.0,
    gossip_fanout: int = 3,
    heartbeat_interval: float = 0.5,
    availability_interval: float = 2.0,
    failure_detector_config: FailureDetectorConfig | None = None,
) -> Behavior[ClusterCmd]:
    async def setup(ctx: ActorContext[ClusterCmd]) -> Behavior[ClusterCmd]:
        self_node = NodeAddress(host=config.host, port=config.port)
        initial_member = Member(
            address=self_node, status=MemberStatus.up, roles=config.roles, id=config.node_id
        )
        initial_state = ClusterState().add_member(initial_member)

        logger = logging.getLogger(f"casty.cluster.{system_name}")
        gossip_logger = logging.getLogger(f"casty.gossip.{system_name}")
        heartbeat_logger = logging.getLogger(f"casty.heartbeat.{system_name}")

        gossip_ref: ActorRef[GossipMsg] = ctx.spawn(
            gossip_actor(
                self_node=self_node,
                initial_state=initial_state,
                remote_transport=remote_transport,
                system_name=system_name,
                logger=gossip_logger,
                fanout=gossip_fanout,
            ),
            "_gossip",
        )
        from casty.config import FailureDetectorConfig as _FDConfig

        fd = failure_detector_config or _FDConfig()
        detector = PhiAccrualFailureDetector(
            threshold=fd.threshold,
            max_sample_size=fd.max_sample_size,
            min_std_deviation_ms=fd.min_std_deviation_ms,
            acceptable_heartbeat_pause_ms=fd.acceptable_heartbeat_pause_ms,
            first_heartbeat_estimate_ms=fd.first_heartbeat_estimate_ms,
        )
        heartbeat_ref: ActorRef[HeartbeatMsg] = ctx.spawn(
            heartbeat_actor(
                self_node=self_node,
                detector=detector,
                remote_transport=remote_transport,
                system_name=system_name,
                logger=heartbeat_logger,
            ),
            "_heartbeat",
        )

        # Build seed refs for join retry
        seed_refs_list: list[ActorRef[GossipMsg]] = []
        if remote_transport is not None:
            for seed_host, seed_port in config.seed_nodes:
                seed_node = NodeAddress(host=seed_host, port=seed_port)
                if seed_node == self_node:
                    continue
                seed_gossip_addr = ActorAddress(
                    system=system_name,
                    path="/_cluster/_gossip",
                    host=seed_host,
                    port=seed_port,
                )
                seed_refs_list.append(
                    remote_transport.make_ref(seed_gossip_addr)  # type: ignore[arg-type]
                )
        seed_refs = tuple(seed_refs_list)

        # Schedule periodic ticks via scheduler
        scheduler_ref.tell(ScheduleTick(
            key="gossip", target=gossip_ref, message=GossipTick(), interval=gossip_interval,
        ))
        scheduler_ref.tell(ScheduleTick(
            key="heartbeat",
            target=gossip_ref,
            message=GetClusterState(reply_to=ctx.self),  # type: ignore[arg-type]
            interval=heartbeat_interval,
        ))
        scheduler_ref.tell(ScheduleTick(
            key="availability",
            target=heartbeat_ref,
            message=CheckAvailability(reply_to=ctx.self),  # type: ignore[arg-type]
            interval=availability_interval,
        ))

        # Schedule join retry for multi-node clusters
        if seed_refs:
            scheduler_ref.tell(ScheduleOnce(
                key="join-retry", target=ctx.self, message=JoinRetry(), delay=0.0,
            ))

        async def cancel_schedules(ctx: ActorContext[ClusterCmd]) -> None:
            scheduler_ref.tell(CancelSchedule(key="gossip"))
            scheduler_ref.tell(CancelSchedule(key="heartbeat"))
            scheduler_ref.tell(CancelSchedule(key="availability"))
            scheduler_ref.tell(CancelSchedule(key="join-retry"))

        logger.info("Cluster started %s:%d (seeds=%d)", self_node.host, self_node.port, len(config.seed_nodes))

        has_seeds = bool(config.seed_nodes)
        initial_cluster_state = None if has_seeds else initial_state

        return Behaviors.with_lifecycle(
            cluster_receive(
                gossip_ref=gossip_ref,
                heartbeat_ref=heartbeat_ref,
                scheduler_ref=scheduler_ref,
                self_node=self_node,
                node_id=config.node_id,
                roles=config.roles,
                seed_refs=seed_refs,
                logger=logger,
                coordinators={},
                broadcast_proxies={},
                singleton_managers={},
                cluster_state=initial_cluster_state,
            ),
            post_stop=cancel_schedules,
        )

    return Behaviors.setup(setup)


# --- Cluster thin wrapper ---


class Cluster:
    """High-level handle for starting and querying a cluster on an ``ActorSystem``.

    Wraps the internal cluster actor, providing ``start``, ``get_state``, and
    ``shutdown`` methods.

    Parameters
    ----------
    system : ActorSystem
        The actor system that will host the cluster actor.
    config : ClusterConfig
        Cluster network and seed configuration.
    remote_transport : RemoteTransport or None
        Transport for cross-node communication.  ``None`` for local-only.
    system_name : str
        Logical name of the actor system (used in actor addresses).
    gossip_interval : float
        Seconds between gossip rounds.
    gossip_fanout : int
        Number of peers contacted per gossip round.
    heartbeat_interval : float
        Seconds between heartbeat state polls.
    availability_interval : float
        Seconds between failure-detector availability checks.
    failure_detector_config : FailureDetectorConfig or None
        Tuning for the phi accrual failure detector.

    Examples
    --------
    >>> cluster = Cluster(system, ClusterConfig("127.0.0.1", 2551, []))
    >>> await cluster.start()
    >>> state = await cluster.get_state()
    >>> len(state.members)
    1
    """

    def __init__(
        self,
        system: ActorSystem,
        config: ClusterConfig,
        *,
        remote_transport: RemoteTransport | None = None,
        system_name: str = "",
        gossip_interval: float = 1.0,
        gossip_fanout: int = 3,
        heartbeat_interval: float = 0.5,
        availability_interval: float = 2.0,
        failure_detector_config: FailureDetectorConfig | None = None,
    ) -> None:
        self._system = system
        self._config = config
        self._remote_transport = remote_transport
        self._system_name = system_name
        self._gossip_interval = gossip_interval
        self._gossip_fanout = gossip_fanout
        self._heartbeat_interval = heartbeat_interval
        self._availability_interval = availability_interval
        self._failure_detector_config = failure_detector_config
        self._ref: ActorRef[ClusterCmd] | None = None

    @property
    def ref(self) -> ActorRef[ClusterCmd]:
        """The cluster actor's ref.

        Returns
        -------
        ActorRef[ClusterCmd]
            Reference to the running cluster actor.

        Raises
        ------
        RuntimeError
            If the cluster has not been started yet.
        """
        if self._ref is None:
            msg = "Cluster not started"
            raise RuntimeError(msg)
        return self._ref

    async def start(self) -> None:
        """Spawn the cluster actor and begin membership protocol.

        After this method returns the cluster is actively gossiping and
        monitoring heartbeats.
        """
        self._ref = self._system.spawn(
            cluster_actor(
                config=self._config,
                scheduler_ref=self._system.scheduler,
                remote_transport=self._remote_transport,
                system_name=self._system_name,
                gossip_interval=self._gossip_interval,
                gossip_fanout=self._gossip_fanout,
                heartbeat_interval=self._heartbeat_interval,
                availability_interval=self._availability_interval,
                failure_detector_config=self._failure_detector_config,
            ),
            "_cluster",
        )

    async def get_state(self, *, timeout: float = 5.0) -> ClusterState:
        """Request the current cluster state from the gossip actor.

        Parameters
        ----------
        timeout : float
            Maximum seconds to wait for a reply.

        Returns
        -------
        ClusterState
            The latest cluster state snapshot.
        """
        return await self._system.ask(
            self.ref,
            lambda r: GetState(reply_to=r),
            timeout=timeout,
        )

    async def shutdown(self) -> None:
        """Gracefully shut down the cluster actor."""
        pass
