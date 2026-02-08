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
)
from casty.gossip_actor import (
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
class JoinRetry:
    pass


type ClusterCmd = (
    GetState | WaitForMembers | NodeUnreachable | PublishAllocations
    | RegisterCoordinator | RegisterBroadcast | JoinRetry | ClusterState
)


# --- ClusterConfig ---


@dataclass(frozen=True)
class ClusterConfig:
    host: str
    port: int
    seed_nodes: list[tuple[str, int]]
    roles: frozenset[str] = field(default_factory=lambda: frozenset[str]())


# --- cluster_actor behavior ---


def cluster_receive(
    *,
    gossip_ref: ActorRef[GossipMsg],
    heartbeat_ref: ActorRef[HeartbeatMsg],
    scheduler_ref: ActorRef[SchedulerMsg],
    self_node: NodeAddress,
    roles: frozenset[str],
    seed_refs: tuple[ActorRef[GossipMsg], ...],
    logger: logging.Logger,
    coordinators: dict[str, ActorRef[CoordinatorMsg]],
    broadcast_proxies: dict[str, ActorRef[Any]],
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
        new_cluster_state: ClusterState | None = None,
        new_waiters: tuple[WaitForMembers, ...] | None = None,
    ) -> Behavior[ClusterCmd]:
        return cluster_receive(
            gossip_ref=gossip_ref,
            heartbeat_ref=heartbeat_ref,
            scheduler_ref=scheduler_ref,
            self_node=self_node,
            roles=roles,
            seed_refs=seed_refs,
            logger=logger,
            coordinators=new_coordinators
            if new_coordinators is not None
            else coordinators,
            broadcast_proxies=new_broadcast_proxies
            if new_broadcast_proxies is not None
            else broadcast_proxies,
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
    heartbeat_interval: float = 0.5,
    availability_interval: float = 2.0,
    failure_detector_config: FailureDetectorConfig | None = None,
) -> Behavior[ClusterCmd]:
    async def setup(ctx: ActorContext[ClusterCmd]) -> Behavior[ClusterCmd]:
        self_node = NodeAddress(host=config.host, port=config.port)
        initial_member = Member(
            address=self_node, status=MemberStatus.up, roles=config.roles
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
                roles=config.roles,
                seed_refs=seed_refs,
                logger=logger,
                coordinators={},
                broadcast_proxies={},
                cluster_state=initial_cluster_state,
            ),
            post_stop=cancel_schedules,
        )

    return Behaviors.setup(setup)


# --- Cluster thin wrapper ---


class Cluster:
    def __init__(
        self,
        system: ActorSystem,
        config: ClusterConfig,
        *,
        remote_transport: RemoteTransport | None = None,
        system_name: str = "",
        gossip_interval: float = 1.0,
        heartbeat_interval: float = 0.5,
        availability_interval: float = 2.0,
        failure_detector_config: FailureDetectorConfig | None = None,
    ) -> None:
        self._system = system
        self._config = config
        self._remote_transport = remote_transport
        self._system_name = system_name
        self._gossip_interval = gossip_interval
        self._heartbeat_interval = heartbeat_interval
        self._availability_interval = availability_interval
        self._failure_detector_config = failure_detector_config
        self._ref: ActorRef[ClusterCmd] | None = None

    @property
    def ref(self) -> ActorRef[ClusterCmd]:
        if self._ref is None:
            msg = "Cluster not started"
            raise RuntimeError(msg)
        return self._ref

    async def start(self) -> None:
        self._ref = self._system.spawn(
            cluster_actor(
                config=self._config,
                scheduler_ref=self._system.scheduler,
                remote_transport=self._remote_transport,
                system_name=self._system_name,
                gossip_interval=self._gossip_interval,
                heartbeat_interval=self._heartbeat_interval,
                availability_interval=self._availability_interval,
                failure_detector_config=self._failure_detector_config,
            ),
            "_cluster",
        )

    async def get_state(self, *, timeout: float = 5.0) -> ClusterState:
        return await self._system.ask(
            self.ref,
            lambda r: GetState(reply_to=r),
            timeout=timeout,
        )

    async def shutdown(self) -> None:
        pass
