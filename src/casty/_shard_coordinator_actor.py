# src/casty/_shard_coordinator_actor.py
from __future__ import annotations

import logging
from collections import Counter
from dataclasses import dataclass
from typing import Any, Protocol

from casty.actor import Behavior, Behaviors
from casty.cluster_state import NodeAddress
from casty.ref import ActorRef
from casty.replication import ReplicationConfig, ShardAllocation


logger = logging.getLogger("casty.coordinator")


@dataclass(frozen=True)
class GetShardLocation:
    shard_id: int
    reply_to: ActorRef[ShardLocation]


@dataclass(frozen=True)
class ShardLocation:
    shard_id: int
    node: NodeAddress
    replicas: tuple[NodeAddress, ...] = ()


@dataclass(frozen=True)
class UpdateTopology:
    available_nodes: frozenset[NodeAddress]


@dataclass(frozen=True)
class NodeDown:
    node: NodeAddress


@dataclass(frozen=True)
class SetRole:
    is_leader: bool
    leader_node: NodeAddress | None


@dataclass(frozen=True)
class SyncAllocations:
    allocations: dict[int, ShardAllocation]
    epoch: int


@dataclass(frozen=True)
class PublishAllocations:
    shard_type: str
    allocations: dict[int, ShardAllocation]
    epoch: int


type CoordinatorMsg = (
    GetShardLocation
    | UpdateTopology
    | NodeDown
    | SetRole
    | SyncAllocations
    | PublishAllocations
)


class ShardAllocationStrategy(Protocol):
    def allocate(
        self,
        shard_id: int,
        current_allocations: dict[int, NodeAddress],
        available_nodes: frozenset[NodeAddress],
    ) -> NodeAddress: ...


class LeastShardStrategy:
    def allocate(
        self,
        shard_id: int,
        current_allocations: dict[int, NodeAddress],
        available_nodes: frozenset[NodeAddress],
    ) -> NodeAddress:
        counts: Counter[NodeAddress] = Counter(current_allocations.values())
        for node in available_nodes:
            if node not in counts:
                counts[node] = 0
        return min(available_nodes, key=lambda n: counts.get(n, 0))


def _allocate_shard(
    shard_id: int,
    strategy: ShardAllocationStrategy,
    allocations: dict[int, ShardAllocation],
    nodes: frozenset[NodeAddress],
    num_replicas: int,
) -> ShardAllocation:
    """Shared allocation logic used by leader mode."""
    primary_allocs = {sid: a.primary for sid, a in allocations.items()}
    primary = strategy.allocate(shard_id, primary_allocs, nodes)

    replica_nodes_list: list[NodeAddress] = []
    remaining = nodes - {primary}
    for _ in range(min(num_replicas, len(remaining))):
        replica = min(
            remaining,
            key=lambda n: sum(
                1 for a in allocations.values() if n in a.replicas
            ),
        )
        replica_nodes_list.append(replica)
        remaining = remaining - {replica}

    return ShardAllocation(primary=primary, replicas=tuple(replica_nodes_list))


def _handle_node_down(
    failed_node: NodeAddress,
    allocations: dict[int, ShardAllocation],
    nodes: frozenset[NodeAddress],
) -> tuple[dict[int, ShardAllocation], frozenset[NodeAddress]]:
    """Shared NodeDown handling used by leader mode."""
    new_allocations = dict(allocations)
    for shard_id, alloc in list(allocations.items()):
        if alloc.primary == failed_node:
            if alloc.replicas:
                new_primary = alloc.replicas[0]
                new_replicas = tuple(
                    r for r in alloc.replicas[1:] if r != failed_node
                )
                new_allocations[shard_id] = ShardAllocation(
                    primary=new_primary, replicas=new_replicas
                )
            else:
                del new_allocations[shard_id]
        else:
            new_replicas = tuple(
                r for r in alloc.replicas if r != failed_node
            )
            if new_replicas != alloc.replicas:
                new_allocations[shard_id] = ShardAllocation(
                    primary=alloc.primary, replicas=new_replicas
                )
    new_nodes = nodes - {failed_node}
    return new_allocations, new_nodes


def shard_coordinator_actor(
    *,
    strategy: ShardAllocationStrategy,
    available_nodes: frozenset[NodeAddress],
    replication: ReplicationConfig | None = None,
    shard_type: str = "",
    publish_ref: ActorRef[PublishAllocations] | None = None,
) -> Behavior[CoordinatorMsg]:
    num_replicas = replication.replicas if replication is not None else 0

    # Legacy mode: no shard_type → direct leader behavior (backward-compatible)
    if not shard_type:
        return _leader_behavior(
            allocations={},
            nodes=available_nodes,
            strategy=strategy,
            num_replicas=num_replicas,
            publish_ref=None,
            shard_type="",
            epoch=0,
        )

    # New mode: start in pending, wait for SetRole AND SyncAllocations
    return _pending_behavior(
        strategy=strategy,
        num_replicas=num_replicas,
        shard_type=shard_type,
        publish_ref=publish_ref,
        nodes=available_nodes,
    )


def _pending_behavior(
    *,
    strategy: ShardAllocationStrategy,
    num_replicas: int,
    shard_type: str,
    publish_ref: ActorRef[PublishAllocations] | None,
    nodes: frozenset[NodeAddress],
    buffer: tuple[GetShardLocation, ...] = (),
    pending_role: SetRole | None = None,
) -> Behavior[CoordinatorMsg]:
    """Buffers requests until BOTH SetRole and SyncAllocations arrive."""

    def _try_activate(
        role: SetRole,
        allocations: dict[int, ShardAllocation],
        epoch: int,
    ) -> Behavior[CoordinatorMsg]:
        if role.is_leader:
            return _leader_behavior(
                allocations=allocations,
                nodes=nodes,
                strategy=strategy,
                num_replicas=num_replicas,
                publish_ref=publish_ref,
                shard_type=shard_type,
                epoch=epoch,
            )
        return _follower_behavior(
            allocations=allocations,
            nodes=nodes,
            epoch=epoch,
            strategy=strategy,
            num_replicas=num_replicas,
            publish_ref=publish_ref,
            shard_type=shard_type,
        )

    async def receive(ctx: Any, msg: CoordinatorMsg) -> Any:
        match msg:
            case SetRole() as role:
                # Store role, wait for SyncAllocations
                return _pending_behavior(
                    strategy=strategy,
                    num_replicas=num_replicas,
                    shard_type=shard_type,
                    publish_ref=publish_ref,
                    nodes=nodes,
                    buffer=buffer,
                    pending_role=role,
                )

            case SyncAllocations(allocations, epoch):
                role = pending_role
                if role is not None:
                    # Have both — activate
                    behavior = _try_activate(role, allocations, epoch)
                    for buffered in buffer:
                        ctx.self.tell(buffered)
                    return behavior
                # Got sync before role — activate as follower with data
                behavior = _follower_behavior(
                    allocations=allocations,
                    nodes=nodes,
                    epoch=epoch,
                    strategy=strategy,
                    num_replicas=num_replicas,
                    publish_ref=publish_ref,
                    shard_type=shard_type,
                )
                for buffered in buffer:
                    ctx.self.tell(buffered)
                return behavior

            case GetShardLocation():
                return _pending_behavior(
                    strategy=strategy,
                    num_replicas=num_replicas,
                    shard_type=shard_type,
                    publish_ref=publish_ref,
                    nodes=nodes,
                    buffer=(*buffer, msg),
                    pending_role=pending_role,
                )

            case UpdateTopology(new_nodes):
                return _pending_behavior(
                    strategy=strategy,
                    num_replicas=num_replicas,
                    shard_type=shard_type,
                    publish_ref=publish_ref,
                    nodes=new_nodes,
                    buffer=buffer,
                    pending_role=pending_role,
                )

            case _:
                return Behaviors.same()

    return Behaviors.receive(receive)


def _leader_behavior(
    *,
    allocations: dict[int, ShardAllocation],
    nodes: frozenset[NodeAddress],
    strategy: ShardAllocationStrategy,
    num_replicas: int,
    publish_ref: ActorRef[PublishAllocations] | None,
    shard_type: str,
    epoch: int,
) -> Behavior[CoordinatorMsg]:
    """Leader mode: allocates new shards, publishes via publish_ref."""

    def _publish(allocs: dict[int, ShardAllocation], new_epoch: int) -> None:
        if publish_ref is not None and shard_type:
            publish_ref.tell(PublishAllocations(
                shard_type=shard_type,
                allocations=allocs,
                epoch=new_epoch,
            ))

    async def receive(ctx: Any, msg: CoordinatorMsg) -> Any:
        match msg:
            case GetShardLocation(shard_id, reply_to):
                if shard_id in allocations:
                    alloc = allocations[shard_id]
                    reply_to.tell(ShardLocation(
                        shard_id=shard_id,
                        node=alloc.primary,
                        replicas=alloc.replicas,
                    ))
                    return Behaviors.same()

                new_alloc = _allocate_shard(
                    shard_id, strategy, allocations, nodes, num_replicas
                )
                new_allocations = {**allocations, shard_id: new_alloc}
                reply_to.tell(ShardLocation(
                    shard_id=shard_id,
                    node=new_alloc.primary,
                    replicas=new_alloc.replicas,
                ))
                new_epoch = epoch + 1
                _publish(new_allocations, new_epoch)
                return _leader_behavior(
                    allocations=new_allocations,
                    nodes=nodes,
                    strategy=strategy,
                    num_replicas=num_replicas,
                    publish_ref=publish_ref,
                    shard_type=shard_type,
                    epoch=new_epoch,
                )

            case UpdateTopology(new_nodes):
                return _leader_behavior(
                    allocations=allocations,
                    nodes=new_nodes,
                    strategy=strategy,
                    num_replicas=num_replicas,
                    publish_ref=publish_ref,
                    shard_type=shard_type,
                    epoch=epoch,
                )

            case NodeDown(failed_node):
                new_allocations, new_nodes = _handle_node_down(
                    failed_node, allocations, nodes
                )
                new_epoch = epoch + 1
                _publish(new_allocations, new_epoch)
                return _leader_behavior(
                    allocations=new_allocations,
                    nodes=new_nodes,
                    strategy=strategy,
                    num_replicas=num_replicas,
                    publish_ref=publish_ref,
                    shard_type=shard_type,
                    epoch=new_epoch,
                )

            case SetRole(is_leader, _):
                if is_leader:
                    return Behaviors.same()
                # Demoted to follower
                return _follower_behavior(
                    allocations=allocations,
                    nodes=nodes,
                    epoch=epoch,
                    strategy=strategy,
                    num_replicas=num_replicas,
                    publish_ref=publish_ref,
                    shard_type=shard_type,
                )

            case SyncAllocations(new_allocs, new_epoch):
                # Leader receiving sync — adopt if epoch is higher
                if new_epoch > epoch:
                    return _leader_behavior(
                        allocations=new_allocs,
                        nodes=nodes,
                        strategy=strategy,
                        num_replicas=num_replicas,
                        publish_ref=publish_ref,
                        shard_type=shard_type,
                        epoch=new_epoch,
                    )
                return Behaviors.same()

            case _:
                return Behaviors.same()

    return Behaviors.receive(receive)


def _follower_behavior(
    *,
    allocations: dict[int, ShardAllocation],
    nodes: frozenset[NodeAddress],
    epoch: int,
    strategy: ShardAllocationStrategy,
    num_replicas: int,
    publish_ref: ActorRef[PublishAllocations] | None,
    shard_type: str,
    buffer: tuple[GetShardLocation, ...] = (),
) -> Behavior[CoordinatorMsg]:
    """Follower mode: serves cached allocations, buffers unknown shards."""

    async def receive(ctx: Any, msg: CoordinatorMsg) -> Any:
        match msg:
            case GetShardLocation(shard_id, reply_to):
                if shard_id in allocations:
                    alloc = allocations[shard_id]
                    reply_to.tell(ShardLocation(
                        shard_id=shard_id,
                        node=alloc.primary,
                        replicas=alloc.replicas,
                    ))
                    return Behaviors.same()
                return _follower_behavior(
                    allocations=allocations,
                    nodes=nodes,
                    epoch=epoch,
                    strategy=strategy,
                    num_replicas=num_replicas,
                    publish_ref=publish_ref,
                    shard_type=shard_type,
                    buffer=(*buffer, msg),
                )

            case SyncAllocations(new_allocs, new_epoch):
                if new_epoch >= epoch:
                    behavior = _follower_behavior(
                        allocations=new_allocs,
                        nodes=nodes,
                        epoch=new_epoch,
                        strategy=strategy,
                        num_replicas=num_replicas,
                        publish_ref=publish_ref,
                        shard_type=shard_type,
                    )
                    for buffered in buffer:
                        ctx.self.tell(buffered)
                    return behavior
                return Behaviors.same()

            case SetRole(is_leader, _):
                if is_leader:
                    return _leader_behavior(
                        allocations=allocations,
                        nodes=nodes,
                        strategy=strategy,
                        num_replicas=num_replicas,
                        publish_ref=publish_ref,
                        shard_type=shard_type,
                        epoch=epoch,
                    )
                return Behaviors.same()

            case UpdateTopology(new_nodes):
                return _follower_behavior(
                    allocations=allocations,
                    nodes=new_nodes,
                    epoch=epoch,
                    strategy=strategy,
                    num_replicas=num_replicas,
                    publish_ref=publish_ref,
                    shard_type=shard_type,
                    buffer=buffer,
                )

            case _:
                return Behaviors.same()

    return Behaviors.receive(receive)
