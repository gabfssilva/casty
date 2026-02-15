"""Cluster singleton — exactly one instance of an actor across the cluster.

The singleton always runs on the leader node (as determined by
``ClusterState.leader``).  When leadership changes, the singleton is stopped
on the old leader and respawned on the new one.

Internal actors:

* **singleton_manager** — one per node that calls ``spawn()``.  Three modes:
  ``pending`` (buffers until ``TopologySnapshot`` arrives), ``active`` (leader,
  owns the child), ``standby`` (non-leader, forwards to leader's manager via
  remoting).
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, TYPE_CHECKING

from casty.actor import Behavior, Behaviors
from casty.core.address import ActorAddress
from casty.cluster.state import NodeAddress
from casty.cluster.topology import TopologySnapshot

if TYPE_CHECKING:
    from casty.context import ActorContext
    from casty.ref import ActorRef
    from casty.remote.tcp_transport import RemoteTransport


@dataclass(frozen=True)
class SingletonConfig:
    """Shared configuration threaded through all singleton manager behaviors."""

    factory: Callable[[], Behavior[Any]]
    name: str
    remote_transport: RemoteTransport | None
    system_name: str
    logger: logging.Logger


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
                        cfg.name, snapshot.leader.host, snapshot.leader.port,
                    )
                    if leader_ref := make_leader_ref(cfg, snapshot.leader):
                        for buffered in buffer:
                            leader_ref.tell(buffered)
                    return standby(cfg, leader_node=snapshot.leader, self_node=self_node)
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
                if snapshot.leader is not None:
                    cfg.logger.info("Singleton [%s] demoted, stopping child", cfg.name)
                    ctx.stop(child_ref)
                    return standby(cfg, leader_node=snapshot.leader, self_node=self_node)
                cfg.logger.info("Singleton [%s] demoted (no leader), stopping child", cfg.name)
                ctx.stop(child_ref)
                return pending(cfg, buffer=(), self_node=self_node)
            case _:
                child_ref.tell(msg)
                return Behaviors.same()

    return Behaviors.receive(receive)


def standby(
    cfg: SingletonConfig,
    leader_node: NodeAddress,
    self_node: NodeAddress | None = None,
) -> Behavior[Any]:
    leader_ref = make_leader_ref(cfg, leader_node)

    async def receive(ctx: ActorContext[Any], msg: Any) -> Behavior[Any]:
        match msg:
            case TopologySnapshot() as snapshot if self_node is not None:
                if snapshot.leader == self_node:
                    child_ref = ctx.spawn(cfg.factory(), cfg.name)
                    cfg.logger.info("Singleton [%s] promoted to leader", cfg.name)
                    return active(cfg, child_ref=child_ref, self_node=self_node)
                if snapshot.leader is not None and snapshot.leader != leader_node:
                    cfg.logger.info(
                        "Singleton [%s] leader changed to %s:%d",
                        cfg.name, snapshot.leader.host, snapshot.leader.port,
                    )
                    return standby(cfg, leader_node=snapshot.leader, self_node=self_node)
                if snapshot.leader is None:
                    return pending(cfg, buffer=(), self_node=self_node)
                return Behaviors.same()
            case _:
                if leader_ref is not None:
                    leader_ref.tell(msg)
                return Behaviors.same()

    return Behaviors.receive(receive)


def make_leader_ref(
    cfg: SingletonConfig, leader_node: NodeAddress,
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
