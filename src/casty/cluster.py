# src/casty/cluster.py
from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from casty.actor import Behavior, Behaviors
from casty.cluster_state import (
    ClusterState,
    Member,
    MemberStatus,
    NodeAddress,
)
from casty._gossip_actor import gossip_actor, GetClusterState, GossipMsg
from casty._heartbeat_actor import heartbeat_actor
from casty.failure_detector import PhiAccrualFailureDetector
from casty.ref import ActorRef

if TYPE_CHECKING:
    from casty.context import ActorContext
    from casty.system import ActorSystem


# --- ClusterCmd messages ---


@dataclass(frozen=True)
class GetState:
    reply_to: ActorRef[ClusterState]


type ClusterCmd = GetState


# --- ClusterConfig ---


@dataclass(frozen=True)
class ClusterConfig:
    host: str
    port: int
    seed_nodes: list[tuple[str, int]]
    roles: frozenset[str] = field(default_factory=lambda: frozenset[str]())


# --- cluster_actor behavior ---


def cluster_actor(*, config: ClusterConfig) -> Behavior[ClusterCmd]:
    async def setup(ctx: ActorContext[ClusterCmd]) -> Behavior[ClusterCmd]:
        self_node = NodeAddress(host=config.host, port=config.port)
        initial_member = Member(
            address=self_node, status=MemberStatus.up, roles=config.roles
        )
        initial_state = ClusterState().add_member(initial_member)

        gossip_ref: ActorRef[GossipMsg] = ctx.spawn(
            gossip_actor(self_node=self_node, initial_state=initial_state),
            "_gossip",
        )
        detector = PhiAccrualFailureDetector()
        ctx.spawn(
            heartbeat_actor(self_node=self_node, detector=detector),
            "_heartbeat",
        )

        async def receive(
            ctx: ActorContext[ClusterCmd], msg: ClusterCmd
        ) -> Behavior[ClusterCmd]:
            match msg:
                case GetState(reply_to):
                    gossip_ref.tell(GetClusterState(reply_to=reply_to))
                    return Behaviors.same()
                case _:
                    return Behaviors.same()

        return Behaviors.receive(receive)

    return Behaviors.setup(setup)


# --- Cluster thin wrapper ---


class Cluster:
    def __init__(self, system: ActorSystem, config: ClusterConfig) -> None:
        self._system = system
        self._config = config
        self._ref: ActorRef[ClusterCmd] | None = None

    @property
    def ref(self) -> ActorRef[ClusterCmd]:
        if self._ref is None:
            msg = "Cluster not started"
            raise RuntimeError(msg)
        return self._ref

    async def start(self) -> None:
        self._ref = self._system.spawn(
            cluster_actor(config=self._config),
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
