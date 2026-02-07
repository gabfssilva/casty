# src/casty/cluster.py
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

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
class JoinRetry:
    pass


type ClusterCmd = (
    GetState | NodeUnreachable | PublishAllocations | RegisterCoordinator | JoinRetry | ClusterState
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
    cluster_state: ClusterState | None,
) -> Behavior[ClusterCmd]:
    """Pure receive behavior — all state in parameters, returns new behavior."""

    def _next(
        *,
        new_coordinators: dict[str, ActorRef[CoordinatorMsg]] | None = None,
        new_cluster_state: ClusterState | None = None,
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
            cluster_state=new_cluster_state
            if new_cluster_state is not None
            else cluster_state,
        )

    async def receive(
        ctx: ActorContext[ClusterCmd], msg: ClusterCmd
    ) -> Behavior[ClusterCmd]:
        match msg:
            case GetState(reply_to):
                gossip_ref.tell(GetClusterState(reply_to=reply_to))
                return Behaviors.same()

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

            case ClusterState() as state:
                if state.diff(cluster_state):
                    logger.info("Cluster topology update: %s", state)

                members = frozenset(
                    m.address
                    for m in state.members
                    if m.status in (MemberStatus.up, MemberStatus.joining)
                )
                heartbeat_ref.tell(HeartbeatTick(members=members))
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
                return _next(new_cluster_state=state)

            case JoinRetry():
                if cluster_state is None or len(cluster_state.members) <= 1:
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
) -> Behavior[ClusterCmd]:
    async def setup(ctx: ActorContext[ClusterCmd]) -> Behavior[ClusterCmd]:
        self_node = NodeAddress(host=config.host, port=config.port)
        initial_member = Member(
            address=self_node, status=MemberStatus.up, roles=config.roles
        )
        initial_state = ClusterState().add_member(initial_member)

        gossip_ref: ActorRef[GossipMsg] = ctx.spawn(
            gossip_actor(
                self_node=self_node,
                initial_state=initial_state,
                remote_transport=remote_transport,
                system_name=system_name,
            ),
            "_gossip",
        )
        detector = PhiAccrualFailureDetector()
        heartbeat_ref: ActorRef[HeartbeatMsg] = ctx.spawn(
            heartbeat_actor(
                self_node=self_node,
                detector=detector,
                remote_transport=remote_transport,
                system_name=system_name,
            ),
            "_heartbeat",
        )

        logger = logging.getLogger(f"casty.cluster.{system_name}")

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
            key="gossip", target=gossip_ref, message=GossipTick(), interval=1.0,
        ))
        scheduler_ref.tell(ScheduleTick(
            key="heartbeat",
            target=gossip_ref,
            message=GetClusterState(reply_to=ctx.self),  # type: ignore[arg-type]
            interval=0.5,
        ))
        scheduler_ref.tell(ScheduleTick(
            key="availability",
            target=heartbeat_ref,
            message=CheckAvailability(reply_to=ctx.self),  # type: ignore[arg-type]
            interval=2.0,
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
    ) -> None:
        self._system = system
        self._config = config
        self._remote_transport = remote_transport
        self._system_name = system_name
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
