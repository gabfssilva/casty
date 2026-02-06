# src/casty/cluster.py
from __future__ import annotations

import asyncio
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
from casty._gossip_actor import (
    GossipMsg,
    GossipTick,
    GetClusterState,
    JoinRequest,
    UpdateShardAllocations,
    gossip_actor,
)
from casty._heartbeat_actor import (
    CheckAvailability,
    HeartbeatMsg,
    HeartbeatTick,
    NodeUnreachable,
    heartbeat_actor,
)
from casty._shard_coordinator_actor import (
    CoordinatorMsg,
    NodeDown,
    PublishAllocations,
    SetRole,
    SyncAllocations,
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


type ClusterCmd = GetState | NodeUnreachable | PublishAllocations | RegisterCoordinator


# --- ClusterConfig ---


@dataclass(frozen=True)
class ClusterConfig:
    host: str
    port: int
    seed_nodes: list[tuple[str, int]]
    roles: frozenset[str] = field(default_factory=lambda: frozenset[str]())


# --- cluster_actor behavior ---


def _send_role_and_sync(
    coord_ref: ActorRef[CoordinatorMsg],
    shard_name: str,
    cluster_state: ClusterState,
    self_node: NodeAddress,
) -> None:
    """Send SetRole + SyncAllocations to a replicated coordinator."""
    is_leader = cluster_state.leader == self_node
    coord_ref.tell(SetRole(
        is_leader=is_leader,
        leader_node=cluster_state.leader,
    ))
    allocs = cluster_state.shard_allocations.get(shard_name, {})
    coord_ref.tell(SyncAllocations(
        allocations=allocs,
        epoch=cluster_state.allocation_epoch,
    ))


def _cluster_receive(
    *,
    gossip_ref: ActorRef[GossipMsg],
    heartbeat_ref: ActorRef[HeartbeatMsg],
    self_node: NodeAddress,
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
        return _cluster_receive(
            gossip_ref=gossip_ref,
            heartbeat_ref=heartbeat_ref,
            self_node=self_node,
            logger=logger,
            coordinators=new_coordinators if new_coordinators is not None else coordinators,
            cluster_state=new_cluster_state if new_cluster_state is not None else cluster_state,
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
                gossip_ref.tell(UpdateShardAllocations(
                    shard_type=shard_type,
                    allocations=allocations,
                    epoch=epoch,
                ))
                return Behaviors.same()

            case RegisterCoordinator(shard_name, coord_ref):
                new_coords = {**coordinators, shard_name: coord_ref}
                if cluster_state is not None:
                    _send_role_and_sync(coord_ref, shard_name, cluster_state, self_node)
                return _next(new_coordinators=new_coords)

            case _:
                if isinstance(msg, ClusterState):
                    state: ClusterState = msg  # type: ignore[assignment]
                    members = frozenset(
                        m.address
                        for m in state.members
                        if m.status in (MemberStatus.up, MemberStatus.joining)
                    )
                    heartbeat_ref.tell(HeartbeatTick(members=members))
                    for shard_name, coord_ref in coordinators.items():
                        _send_role_and_sync(coord_ref, shard_name, state, self_node)
                    return _next(new_cluster_state=state)
                return Behaviors.same()

    return Behaviors.receive(receive)


def cluster_actor(
    *,
    config: ClusterConfig,
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

        # Schedule periodic ticks
        tick_tasks: list[asyncio.Task[None]] = []

        async def _gossip_tick_loop() -> None:
            try:
                while True:
                    await asyncio.sleep(1.0)
                    gossip_ref.tell(GossipTick())
            except asyncio.CancelledError:
                pass

        async def _heartbeat_tick_loop() -> None:
            try:
                while True:
                    await asyncio.sleep(0.5)
                    gossip_ref.tell(
                        GetClusterState(reply_to=ctx.self)  # type: ignore[arg-type]
                    )
            except asyncio.CancelledError:
                pass

        async def _availability_check_loop() -> None:
            try:
                while True:
                    await asyncio.sleep(2.0)
                    heartbeat_ref.tell(
                        CheckAvailability(reply_to=ctx.self)  # type: ignore[arg-type]
                    )
            except asyncio.CancelledError:
                pass

        loop = asyncio.get_running_loop()
        tick_tasks.append(loop.create_task(_gossip_tick_loop()))
        tick_tasks.append(loop.create_task(_heartbeat_tick_loop()))
        tick_tasks.append(loop.create_task(_availability_check_loop()))

        async def _cancel_ticks(ctx: ActorContext[ClusterCmd]) -> None:
            for task in tick_tasks:
                task.cancel()
            for task in tick_tasks:
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        # Send join requests to seed nodes
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
                seed_ref: ActorRef[GossipMsg] = remote_transport.make_ref(seed_gossip_addr)  # type: ignore[assignment]
                seed_ref.tell(
                    JoinRequest(
                        node=self_node,
                        roles=config.roles,
                        reply_to=gossip_ref,  # type: ignore[arg-type]
                    )
                )

        # For single-node clusters (no seeds), initialize immediately so that
        # coordinators registered before gossip produces state get activated.
        # For multi-node clusters, wait for gossip to converge.
        has_seeds = bool(config.seed_nodes)
        initial_cluster_state = None if has_seeds else initial_state

        return Behaviors.with_lifecycle(
            _cluster_receive(
                gossip_ref=gossip_ref,
                heartbeat_ref=heartbeat_ref,
                self_node=self_node,
                logger=logger,
                coordinators={},
                cluster_state=initial_cluster_state,
            ),
            post_stop=_cancel_ticks,
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
