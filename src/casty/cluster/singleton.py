"""Cluster singleton — exactly one instance of an actor across the cluster.

The singleton always runs on the leader node (as determined by
``ClusterState.leader``).  When leadership changes, ownership is handed over:
the demoted leader stops its child, waits for it to terminate, and only then
notifies the new leader (``HandoverDone``), which spawns its own instance.
If the old leader is gone (crashed, downed, unreachable), the new leader
skips the handover and spawns immediately.

Internal actors:

* **singleton_manager** — one per node that calls ``spawn()``.  Modes:
  ``pending`` (buffers until ``TopologySnapshot`` arrives), ``active``
  (leader, owns the child), ``handing_over`` (demoted, draining the old
  child), ``taking_over`` (promoted, awaiting handover), ``standby``
  (non-leader, forwards to leader's manager via remoting).

The exactly-one guarantee is strict for graceful leader changes (both
managers connected).  Under a network partition the new leader eventually
activates via the topology fallback while the old instance may still be
running on the isolated side — overlap is possible until the partition
heals, matching Akka's cluster singleton semantics.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, TYPE_CHECKING

from casty.actor import Behavior, Behaviors
from casty.core.address import ActorAddress
from casty.core.messages import Terminated
from casty.cluster.state import MemberStatus, NodeAddress
from casty.cluster.topology import TopologySnapshot

if TYPE_CHECKING:
    from casty.context import ActorContext
    from casty.ref import ActorRef
    from casty.remote.tcp_transport import RemoteTransport


@dataclass(frozen=True)
class HandoverDone:
    """Sent by the demoted leader's manager once its child has fully stopped."""


@dataclass(frozen=True)
class SingletonConfig:
    """Shared configuration threaded through all singleton manager behaviors."""

    factory: Callable[[], Behavior[Any]]
    name: str
    remote_transport: RemoteTransport | None
    system_name: str
    logger: logging.Logger


_ALIVE_STATUSES = (MemberStatus.joining, MemberStatus.up, MemberStatus.leaving)


def node_gone(snapshot: TopologySnapshot, node: NodeAddress) -> bool:
    """Whether *node* can no longer be running a singleton child."""
    member = next((m for m in snapshot.members if m.address == node), None)
    return (
        member is None
        or member.status not in _ALIVE_STATUSES
        or node in snapshot.unreachable
    )


def singleton_manager_actor(
    *,
    factory: Callable[[], Behavior[Any]],
    name: str,
    remote_transport: RemoteTransport | None,
    system_name: str,
    logger: logging.Logger,
    topology_ref: ActorRef[Any] | None = None,
    self_node: NodeAddress | None = None,
) -> Behavior[Any]:
    """Manager actor for a cluster singleton.

    Starts in ``pending`` mode, transitions to ``active`` (leader) or
    ``standby`` (non-leader) upon receiving ``TopologySnapshot``.
    """
    cfg = SingletonConfig(
        factory=factory,
        name=name,
        remote_transport=remote_transport,
        system_name=system_name,
        logger=logger,
    )

    if topology_ref is not None and self_node is not None:

        async def setup(ctx: ActorContext[Any]) -> Behavior[Any]:
            from casty.cluster.topology import SubscribeTopology

            topology_ref.tell(SubscribeTopology(reply_to=ctx.self))  # type: ignore[arg-type]
            return pending(cfg, buffer=(), self_node=self_node)

        return Behaviors.setup(setup)

    return pending(cfg, buffer=())


def pending(
    cfg: SingletonConfig,
    buffer: tuple[Any, ...],
    self_node: NodeAddress | None = None,
) -> Behavior[Any]:
    async def receive(ctx: ActorContext[Any], msg: Any) -> Behavior[Any]:
        match msg:
            case TopologySnapshot() as snapshot if self_node is not None:
                if snapshot.leader == self_node:
                    child_ref = ctx.spawn(cfg.factory(), cfg.name)
                    cfg.logger.info("Singleton [%s] activated on leader", cfg.name)
                    for buffered in buffer:
                        child_ref.tell(buffered)
                    return active(cfg, child_ref=child_ref, self_node=self_node)
                if snapshot.leader is not None:
                    cfg.logger.info(
                        "Singleton [%s] standby (leader=%s:%d)",
                        cfg.name,
                        snapshot.leader.host,
                        snapshot.leader.port,
                    )
                    if leader_ref := make_leader_ref(cfg, snapshot.leader):
                        for buffered in buffer:
                            leader_ref.tell(buffered)
                    return standby(
                        cfg, leader_node=snapshot.leader, self_node=self_node
                    )
                return Behaviors.same()
            case HandoverDone():
                return Behaviors.same()
            case _:
                return pending(cfg, buffer=(*buffer, msg), self_node=self_node)

    return Behaviors.receive(receive)


def active(
    cfg: SingletonConfig,
    child_ref: ActorRef[Any],
    self_node: NodeAddress | None = None,
) -> Behavior[Any]:
    async def receive(ctx: ActorContext[Any], msg: Any) -> Behavior[Any]:
        match msg:
            case TopologySnapshot() as snapshot if self_node is not None:
                if snapshot.leader == self_node:
                    return Behaviors.same()
                cfg.logger.info(
                    "Singleton [%s] demoted, handing over", cfg.name
                )
                ctx.watch(child_ref)
                ctx.stop(child_ref)
                return handing_over(
                    cfg,
                    child_ref=child_ref,
                    next_leader=snapshot.leader,
                    buffer=(),
                    self_node=self_node,
                )
            case HandoverDone():
                return Behaviors.same()
            case _:
                child_ref.tell(msg)
                return Behaviors.same()

    return Behaviors.receive(receive)


def handing_over(
    cfg: SingletonConfig,
    child_ref: ActorRef[Any],
    next_leader: NodeAddress | None,
    buffer: tuple[Any, ...],
    self_node: NodeAddress,
) -> Behavior[Any]:
    """Demoted leader draining its child before releasing ownership."""

    async def receive(ctx: ActorContext[Any], msg: Any) -> Behavior[Any]:
        match msg:
            case TopologySnapshot() as snapshot:
                return handing_over(
                    cfg,
                    child_ref=child_ref,
                    next_leader=snapshot.leader,
                    buffer=buffer,
                    self_node=self_node,
                )
            case Terminated(ref=ref) if ref.id == child_ref.id:
                if next_leader == self_node:
                    new_child = ctx.spawn(cfg.factory(), cfg.name)
                    cfg.logger.info(
                        "Singleton [%s] re-promoted during handover", cfg.name
                    )
                    for buffered in buffer:
                        new_child.tell(buffered)
                    return active(cfg, child_ref=new_child, self_node=self_node)
                if next_leader is not None:
                    cfg.logger.info(
                        "Singleton [%s] handover complete (leader=%s:%d)",
                        cfg.name,
                        next_leader.host,
                        next_leader.port,
                    )
                    if leader_ref := make_leader_ref(cfg, next_leader):
                        leader_ref.tell(HandoverDone())
                        for buffered in buffer:
                            leader_ref.tell(buffered)
                    return standby(
                        cfg, leader_node=next_leader, self_node=self_node
                    )
                return pending(cfg, buffer=buffer, self_node=self_node)
            case HandoverDone():
                return Behaviors.same()
            case _:
                return handing_over(
                    cfg,
                    child_ref=child_ref,
                    next_leader=next_leader,
                    buffer=(*buffer, msg),
                    self_node=self_node,
                )

    return Behaviors.receive(receive)


def taking_over(
    cfg: SingletonConfig,
    previous_leader: NodeAddress,
    buffer: tuple[Any, ...],
    self_node: NodeAddress,
) -> Behavior[Any]:
    """Promoted leader awaiting handover from the previous leader."""

    def activate(ctx: ActorContext[Any], reason: str) -> Behavior[Any]:
        child_ref = ctx.spawn(cfg.factory(), cfg.name)
        cfg.logger.info("Singleton [%s] activated (%s)", cfg.name, reason)
        for buffered in buffer:
            child_ref.tell(buffered)
        return active(cfg, child_ref=child_ref, self_node=self_node)

    async def receive(ctx: ActorContext[Any], msg: Any) -> Behavior[Any]:
        match msg:
            case HandoverDone():
                return activate(ctx, "handover complete")
            case TopologySnapshot() as snapshot:
                if snapshot.leader == self_node:
                    if node_gone(snapshot, previous_leader):
                        return activate(ctx, "previous leader gone")
                    return Behaviors.same()
                if snapshot.leader is not None:
                    if leader_ref := make_leader_ref(cfg, snapshot.leader):
                        for buffered in buffer:
                            leader_ref.tell(buffered)
                    return standby(
                        cfg, leader_node=snapshot.leader, self_node=self_node
                    )
                return pending(cfg, buffer=buffer, self_node=self_node)
            case _:
                return taking_over(
                    cfg,
                    previous_leader=previous_leader,
                    buffer=(*buffer, msg),
                    self_node=self_node,
                )

    return Behaviors.receive(receive)


def standby(
    cfg: SingletonConfig,
    leader_node: NodeAddress,
    self_node: NodeAddress | None = None,
    handover_done: bool = False,
) -> Behavior[Any]:
    leader_ref = make_leader_ref(cfg, leader_node)

    async def receive(ctx: ActorContext[Any], msg: Any) -> Behavior[Any]:
        match msg:
            case TopologySnapshot() as snapshot if self_node is not None:
                if snapshot.leader == self_node:
                    if handover_done or node_gone(snapshot, leader_node):
                        child_ref = ctx.spawn(cfg.factory(), cfg.name)
                        cfg.logger.info(
                            "Singleton [%s] promoted to leader", cfg.name
                        )
                        return active(
                            cfg, child_ref=child_ref, self_node=self_node
                        )
                    cfg.logger.info(
                        "Singleton [%s] promoted, awaiting handover from %s:%d",
                        cfg.name,
                        leader_node.host,
                        leader_node.port,
                    )
                    return taking_over(
                        cfg,
                        previous_leader=leader_node,
                        buffer=(),
                        self_node=self_node,
                    )
                if snapshot.leader is not None and snapshot.leader != leader_node:
                    cfg.logger.info(
                        "Singleton [%s] leader changed to %s:%d",
                        cfg.name,
                        snapshot.leader.host,
                        snapshot.leader.port,
                    )
                    return standby(
                        cfg, leader_node=snapshot.leader, self_node=self_node
                    )
                if snapshot.leader is None:
                    return pending(cfg, buffer=(), self_node=self_node)
                return Behaviors.same()
            case HandoverDone():
                return standby(
                    cfg,
                    leader_node=leader_node,
                    self_node=self_node,
                    handover_done=True,
                )
            case _:
                if leader_ref is not None:
                    leader_ref.tell(msg)
                return Behaviors.same()

    return Behaviors.receive(receive)


def make_leader_ref(
    cfg: SingletonConfig,
    leader_node: NodeAddress,
) -> ActorRef[Any] | None:
    if cfg.remote_transport is None:
        return None
    addr = ActorAddress(
        system=cfg.system_name,
        path=f"/_singleton-{cfg.name}",
        host=leader_node.host,
        port=leader_node.port,
    )
    return cfg.remote_transport.make_ref(addr)
