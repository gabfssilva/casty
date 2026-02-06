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
    gossip_actor,
)
from casty._heartbeat_actor import (
    CheckAvailability,
    HeartbeatMsg,
    HeartbeatTick,
    NodeUnreachable,
    heartbeat_actor,
)
from casty._shard_coordinator_actor import CoordinatorMsg, NodeDown
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


type ClusterCmd = GetState | NodeUnreachable


# --- ClusterConfig ---


@dataclass(frozen=True)
class ClusterConfig:
    host: str
    port: int
    seed_nodes: list[tuple[str, int]]
    roles: frozenset[str] = field(default_factory=lambda: frozenset[str]())


# --- cluster_actor behavior ---


def cluster_actor(
    *,
    config: ClusterConfig,
    remote_transport: RemoteTransport | None = None,
    system_name: str = "",
    coordinator_refs: dict[str, ActorRef[CoordinatorMsg]] | None = None,
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
        coord_refs = coordinator_refs or {}

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
                    # Get current members from gossip state
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

        async def receive(
            ctx: ActorContext[ClusterCmd], msg: ClusterCmd
        ) -> Behavior[ClusterCmd]:
            match msg:
                case GetState(reply_to):
                    gossip_ref.tell(GetClusterState(reply_to=reply_to))
                    return Behaviors.same()

                case NodeUnreachable(node):
                    logger.warning("Node unreachable: %s:%d", node.host, node.port)
                    for coord_ref in coord_refs.values():
                        coord_ref.tell(NodeDown(node=node))
                    return Behaviors.same()

                case _:
                    # ClusterState arrives at runtime via gossip but isn't part of ClusterCmd
                    if isinstance(msg, ClusterState):
                        members = frozenset(
                            m.address
                            for m in msg.members  # type: ignore[union-attr]
                            if m.status in (MemberStatus.up, MemberStatus.joining)
                        )
                        heartbeat_ref.tell(HeartbeatTick(members=members))
                    return Behaviors.same()

        return Behaviors.with_lifecycle(
            Behaviors.receive(receive),
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
        coordinator_refs: dict[str, ActorRef[CoordinatorMsg]] | None = None,
    ) -> None:
        self._system = system
        self._config = config
        self._remote_transport = remote_transport
        self._system_name = system_name
        self._coordinator_refs = coordinator_refs
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
                coordinator_refs=self._coordinator_refs,
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
