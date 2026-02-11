"""Cluster singleton — exactly one instance of an actor across the cluster.

The singleton always runs on the leader node (as determined by
``ClusterState.leader``).  When leadership changes, the singleton is stopped
on the old leader and respawned on the new one.

Internal actors:

* **singleton_manager** — one per node that calls ``spawn()``.  Three modes:
  ``pending`` (buffers until ``SetRole`` arrives), ``active`` (leader, owns the
  child), ``standby`` (non-leader, forwards to leader's manager via remoting).
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, TYPE_CHECKING

from casty.actor import Behavior, Behaviors
from casty.address import ActorAddress
from casty.cluster_state import NodeAddress
from casty.shard_coordinator_actor import SetRole

if TYPE_CHECKING:
    from casty.context import ActorContext
    from casty.ref import ActorRef
    from casty.remote_transport import RemoteTransport


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
) -> Behavior[Any]:
    """Manager actor for a cluster singleton.

    Starts in ``pending`` mode, transitions to ``active`` (leader) or
    ``standby`` (non-leader) upon receiving ``SetRole`` from the cluster actor.
    """
    cfg = SingletonConfig(
        factory=factory,
        name=name,
        remote_transport=remote_transport,
        system_name=system_name,
        logger=logger,
    )
    return pending(cfg, buffer=())


def pending(cfg: SingletonConfig, buffer: tuple[Any, ...]) -> Behavior[Any]:
    async def receive(ctx: ActorContext[Any], msg: Any) -> Behavior[Any]:
        match msg:
            case SetRole(is_leader=True):
                child_ref = ctx.spawn(cfg.factory(), cfg.name)
                cfg.logger.info("Singleton [%s] activated on leader", cfg.name)
                for buffered in buffer:
                    child_ref.tell(buffered)
                return active(cfg, child_ref=child_ref)

            case SetRole(is_leader=False, leader_node=leader) if leader is not None:
                cfg.logger.info("Singleton [%s] standby (leader=%s:%d)", cfg.name, leader.host, leader.port)
                if leader_ref := make_leader_ref(cfg, leader):
                    for buffered in buffer:
                        leader_ref.tell(buffered)
                return standby(cfg, leader_node=leader)

            case SetRole(is_leader=False, leader_node=None):
                return Behaviors.same()

            case _:
                return pending(cfg, buffer=(*buffer, msg))

    return Behaviors.receive(receive)


def active(cfg: SingletonConfig, child_ref: ActorRef[Any]) -> Behavior[Any]:
    async def receive(ctx: ActorContext[Any], msg: Any) -> Behavior[Any]:
        match msg:
            case SetRole(is_leader=True):
                return Behaviors.same()

            case SetRole(is_leader=False, leader_node=leader) if leader is not None:
                cfg.logger.info("Singleton [%s] demoted, stopping child", cfg.name)
                ctx.stop(child_ref)
                return standby(cfg, leader_node=leader)

            case SetRole(is_leader=False, leader_node=None):
                cfg.logger.info("Singleton [%s] demoted (no leader), stopping child", cfg.name)
                ctx.stop(child_ref)
                return pending(cfg, buffer=())

            case _:
                child_ref.tell(msg)
                return Behaviors.same()

    return Behaviors.receive(receive)


def standby(cfg: SingletonConfig, leader_node: NodeAddress) -> Behavior[Any]:
    leader_ref = make_leader_ref(cfg, leader_node)

    async def receive(ctx: ActorContext[Any], msg: Any) -> Behavior[Any]:
        match msg:
            case SetRole(is_leader=True):
                child_ref = ctx.spawn(cfg.factory(), cfg.name)
                cfg.logger.info("Singleton [%s] promoted to leader", cfg.name)
                return active(cfg, child_ref=child_ref)

            case SetRole(is_leader=False, leader_node=new_leader) if new_leader is not None:
                if new_leader != leader_node:
                    cfg.logger.info("Singleton [%s] leader changed to %s:%d", cfg.name, new_leader.host, new_leader.port)
                return standby(cfg, leader_node=new_leader)

            case SetRole(is_leader=False, leader_node=None):
                return pending(cfg, buffer=())

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
