# src/casty/_shard_coordinator_actor.py
from __future__ import annotations

import logging
from collections import Counter
from dataclasses import dataclass
from typing import Any, Protocol, TYPE_CHECKING

from casty.actor import Behavior, Behaviors
from casty.address import ActorAddress
from casty.cluster_state import NodeAddress
from casty.ref import ActorRef
from casty.replication import ReplicationConfig, ShardAllocation

if TYPE_CHECKING:
    from casty.remote_transport import RemoteTransport


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


@dataclass(frozen=True)
class RegisterRegion:
    node: NodeAddress


type CoordinatorMsg = (
    GetShardLocation
    | UpdateTopology
    | NodeDown
    | SetRole
    | SyncAllocations
    | PublishAllocations
    | RegisterRegion
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


def allocate_shard(
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


def handle_node_down(
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
    remote_transport: RemoteTransport | None = None,
    system_name: str = "",
    logger: logging.Logger | None = None,
) -> Behavior[CoordinatorMsg]:
    log = logger or logging.getLogger(f"casty.coordinator.{system_name}")
    num_replicas = replication.replicas if replication is not None else 0

    return pending_behavior(
        strategy=strategy,
        num_replicas=num_replicas,
        shard_type=shard_type,
        publish_ref=publish_ref,
        nodes=available_nodes,
        region_nodes=frozenset(),
        remote_transport=remote_transport,
        system_name=system_name,
        logger=log,
    )


def pending_behavior(
    *,
    strategy: ShardAllocationStrategy,
    num_replicas: int,
    shard_type: str,
    publish_ref: ActorRef[PublishAllocations] | None,
    nodes: frozenset[NodeAddress],
    region_nodes: frozenset[NodeAddress],
    remote_transport: RemoteTransport | None = None,
    system_name: str = "",
    buffer: tuple[GetShardLocation, ...] = (),
    pending_role: SetRole | None = None,
    pending_sync: SyncAllocations | None = None,
    logger: logging.Logger | None = None,
) -> Behavior[CoordinatorMsg]:
    """Buffers requests until BOTH SetRole and SyncAllocations arrive."""
    log = logger or logging.getLogger(f"casty.coordinator.{system_name}")

    def try_activate(
        role: SetRole,
        sync: SyncAllocations,
    ) -> Behavior[CoordinatorMsg]:
        if role.is_leader:
            log.info("Coordinator [%s] activated as leader (buffered=%d)", shard_type, len(buffer))
            return leader_behavior(
                allocations=sync.allocations,
                nodes=nodes,
                region_nodes=region_nodes,
                strategy=strategy,
                num_replicas=num_replicas,
                publish_ref=publish_ref,
                shard_type=shard_type,
                epoch=sync.epoch,
                remote_transport=remote_transport,
                system_name=system_name,
                logger=log,
            )
        log.info("Coordinator [%s] activated as follower (buffered=%d)", shard_type, len(buffer))
        return follower_behavior(
            allocations=sync.allocations,
            nodes=nodes,
            region_nodes=region_nodes,
            epoch=sync.epoch,
            strategy=strategy,
            num_replicas=num_replicas,
            publish_ref=publish_ref,
            shard_type=shard_type,
            leader_node=role.leader_node,
            remote_transport=remote_transport,
            system_name=system_name,
            logger=log,
        )

    async def receive(ctx: Any, msg: CoordinatorMsg) -> Any:
        match msg:
            case RegisterRegion(node):
                return pending_behavior(
                    strategy=strategy,
                    num_replicas=num_replicas,
                    shard_type=shard_type,
                    publish_ref=publish_ref,
                    nodes=nodes,
                    region_nodes=region_nodes | {node},
                    remote_transport=remote_transport,
                    system_name=system_name,
                    buffer=buffer,
                    pending_role=pending_role,
                    pending_sync=pending_sync,
                    logger=log,
                )

            case SetRole() as role:
                if pending_sync is not None:
                    behavior = try_activate(role, pending_sync)
                    for buffered in buffer:
                        ctx.self.tell(buffered)
                    if not role.is_leader:
                        for node in region_nodes:
                            ctx.self.tell(RegisterRegion(node=node))
                    return behavior
                return pending_behavior(
                    strategy=strategy,
                    num_replicas=num_replicas,
                    shard_type=shard_type,
                    publish_ref=publish_ref,
                    nodes=nodes,
                    region_nodes=region_nodes,
                    remote_transport=remote_transport,
                    system_name=system_name,
                    buffer=buffer,
                    pending_role=role,
                    pending_sync=pending_sync,
                    logger=log,
                )

            case SyncAllocations() as sync:
                if pending_role is not None:
                    behavior = try_activate(pending_role, sync)
                    for buffered in buffer:
                        ctx.self.tell(buffered)
                    if not pending_role.is_leader:
                        for node in region_nodes:
                            ctx.self.tell(RegisterRegion(node=node))
                    return behavior
                return pending_behavior(
                    strategy=strategy,
                    num_replicas=num_replicas,
                    shard_type=shard_type,
                    publish_ref=publish_ref,
                    nodes=nodes,
                    region_nodes=region_nodes,
                    remote_transport=remote_transport,
                    system_name=system_name,
                    buffer=buffer,
                    pending_role=pending_role,
                    pending_sync=sync,
                    logger=log,
                )

            case GetShardLocation():
                return pending_behavior(
                    strategy=strategy,
                    num_replicas=num_replicas,
                    shard_type=shard_type,
                    publish_ref=publish_ref,
                    nodes=nodes,
                    region_nodes=region_nodes,
                    remote_transport=remote_transport,
                    system_name=system_name,
                    buffer=(*buffer, msg),
                    pending_role=pending_role,
                    pending_sync=pending_sync,
                    logger=log,
                )

            case UpdateTopology(new_nodes):
                return pending_behavior(
                    strategy=strategy,
                    num_replicas=num_replicas,
                    shard_type=shard_type,
                    publish_ref=publish_ref,
                    nodes=new_nodes,
                    region_nodes=region_nodes,
                    remote_transport=remote_transport,
                    system_name=system_name,
                    buffer=buffer,
                    pending_role=pending_role,
                    pending_sync=pending_sync,
                    logger=log,
                )

            case _:
                return Behaviors.same()

    return Behaviors.receive(receive)


def leader_behavior(
    *,
    allocations: dict[int, ShardAllocation],
    nodes: frozenset[NodeAddress],
    region_nodes: frozenset[NodeAddress],
    strategy: ShardAllocationStrategy,
    num_replicas: int,
    publish_ref: ActorRef[PublishAllocations] | None,
    shard_type: str,
    epoch: int,
    remote_transport: RemoteTransport | None = None,
    system_name: str = "",
    logger: logging.Logger | None = None,
) -> Behavior[CoordinatorMsg]:
    """Leader mode: allocates new shards, publishes via publish_ref."""
    log = logger or logging.getLogger(f"casty.coordinator.{system_name}")

    def publish(allocs: dict[int, ShardAllocation], new_epoch: int) -> None:
        if publish_ref is not None:
            publish_ref.tell(PublishAllocations(
                shard_type=shard_type,
                allocations=allocs,
                epoch=new_epoch,
            ))

    effective_nodes = nodes & region_nodes

    async def receive(ctx: Any, msg: CoordinatorMsg) -> Any:
        match msg:
            case RegisterRegion(node):
                return leader_behavior(
                    allocations=allocations,
                    nodes=nodes,
                    region_nodes=region_nodes | {node},
                    strategy=strategy,
                    num_replicas=num_replicas,
                    publish_ref=publish_ref,
                    shard_type=shard_type,
                    epoch=epoch,
                    remote_transport=remote_transport,
                    system_name=system_name,
                    logger=log,
                )

            case GetShardLocation(shard_id, reply_to):
                if shard_id in allocations:
                    alloc = allocations[shard_id]
                    reply_to.tell(ShardLocation(
                        shard_id=shard_id,
                        node=alloc.primary,
                        replicas=alloc.replicas,
                    ))
                    return Behaviors.same()

                new_alloc = allocate_shard(
                    shard_id, strategy, allocations, effective_nodes, num_replicas
                )
                new_allocations = {**allocations, shard_id: new_alloc}
                reply_to.tell(ShardLocation(
                    shard_id=shard_id,
                    node=new_alloc.primary,
                    replicas=new_alloc.replicas,
                ))
                new_epoch = epoch + 1
                log.info("Shard %d -> %s:%d (epoch=%d)", shard_id, new_alloc.primary.host, new_alloc.primary.port, new_epoch)
                publish(new_allocations, new_epoch)
                return leader_behavior(
                    allocations=new_allocations,
                    nodes=nodes,
                    region_nodes=region_nodes,
                    strategy=strategy,
                    num_replicas=num_replicas,
                    publish_ref=publish_ref,
                    shard_type=shard_type,
                    epoch=new_epoch,
                    remote_transport=remote_transport,
                    system_name=system_name,
                    logger=log,
                )

            case UpdateTopology(new_nodes):
                return leader_behavior(
                    allocations=allocations,
                    nodes=new_nodes,
                    region_nodes=region_nodes,
                    strategy=strategy,
                    num_replicas=num_replicas,
                    publish_ref=publish_ref,
                    shard_type=shard_type,
                    epoch=epoch,
                    remote_transport=remote_transport,
                    system_name=system_name,
                    logger=log,
                )

            case NodeDown(failed_node):
                log.warning("NodeDown %s:%d (shards affected)", failed_node.host, failed_node.port)
                new_allocations, new_nodes = handle_node_down(
                    failed_node, allocations, nodes
                )
                new_epoch = epoch + 1
                publish(new_allocations, new_epoch)
                return leader_behavior(
                    allocations=new_allocations,
                    nodes=new_nodes,
                    region_nodes=region_nodes,
                    strategy=strategy,
                    num_replicas=num_replicas,
                    publish_ref=publish_ref,
                    shard_type=shard_type,
                    epoch=new_epoch,
                    remote_transport=remote_transport,
                    system_name=system_name,
                    logger=log,
                )

            case SetRole(is_leader, leader_node):
                if is_leader:
                    return Behaviors.same()
                log.info("Coordinator [%s] demoted to follower", shard_type)
                behavior = follower_behavior(
                    allocations=allocations,
                    nodes=nodes,
                    region_nodes=region_nodes,
                    epoch=epoch,
                    strategy=strategy,
                    num_replicas=num_replicas,
                    publish_ref=publish_ref,
                    shard_type=shard_type,
                    leader_node=leader_node,
                    remote_transport=remote_transport,
                    system_name=system_name,
                    logger=log,
                )
                for node in region_nodes:
                    ctx.self.tell(RegisterRegion(node=node))
                return behavior

            case SyncAllocations(new_allocs, new_epoch):
                if new_epoch > epoch:
                    return leader_behavior(
                        allocations=new_allocs,
                        nodes=nodes,
                        region_nodes=region_nodes,
                        strategy=strategy,
                        num_replicas=num_replicas,
                        publish_ref=publish_ref,
                        shard_type=shard_type,
                        epoch=new_epoch,
                        remote_transport=remote_transport,
                        system_name=system_name,
                        logger=log,
                    )
                return Behaviors.same()

            case _:
                return Behaviors.same()

    return Behaviors.receive(receive)


def follower_behavior(
    *,
    allocations: dict[int, ShardAllocation],
    nodes: frozenset[NodeAddress],
    region_nodes: frozenset[NodeAddress],
    epoch: int,
    strategy: ShardAllocationStrategy,
    num_replicas: int,
    publish_ref: ActorRef[PublishAllocations] | None,
    shard_type: str,
    leader_node: NodeAddress | None = None,
    remote_transport: RemoteTransport | None = None,
    system_name: str = "",
    buffer: tuple[GetShardLocation, ...] = (),
    logger: logging.Logger | None = None,
) -> Behavior[CoordinatorMsg]:
    """Follower mode: serves cached allocations, forwards unknown to leader."""
    log = logger or logging.getLogger(f"casty.coordinator.{system_name}")

    async def receive(ctx: Any, msg: CoordinatorMsg) -> Any:
        match msg:
            case RegisterRegion(node):
                # Forward to leader so it knows about this region
                if leader_node is not None and remote_transport is not None:
                    leader_coord_addr = ActorAddress(
                        system=system_name,
                        path=f"/_coord-{shard_type}",
                        host=leader_node.host,
                        port=leader_node.port,
                    )
                    leader_ref: ActorRef[CoordinatorMsg] = remote_transport.make_ref(
                        leader_coord_addr
                    )
                    leader_ref.tell(msg)
                return follower_behavior(
                    allocations=allocations,
                    nodes=nodes,
                    region_nodes=region_nodes | {node},
                    epoch=epoch,
                    strategy=strategy,
                    num_replicas=num_replicas,
                    publish_ref=publish_ref,
                    shard_type=shard_type,
                    leader_node=leader_node,
                    remote_transport=remote_transport,
                    system_name=system_name,
                    buffer=buffer,
                    logger=log,
                )

            case GetShardLocation(shard_id, reply_to):
                if shard_id in allocations:
                    alloc = allocations[shard_id]
                    reply_to.tell(ShardLocation(
                        shard_id=shard_id,
                        node=alloc.primary,
                        replicas=alloc.replicas,
                    ))
                    return Behaviors.same()
                # Forward to leader if possible — leader replies directly to reply_to
                if leader_node is not None and remote_transport is not None:
                    log.debug("Forwarding shard %d to leader", shard_id)
                    leader_coord_addr = ActorAddress(
                        system=system_name,
                        path=f"/_coord-{shard_type}",
                        host=leader_node.host,
                        port=leader_node.port,
                    )
                    leader_ref: ActorRef[CoordinatorMsg] = remote_transport.make_ref(
                        leader_coord_addr
                    )
                    leader_ref.tell(msg)
                    return Behaviors.same()
                # No leader known yet — buffer until SyncAllocations arrives
                return follower_behavior(
                    allocations=allocations,
                    nodes=nodes,
                    region_nodes=region_nodes,
                    epoch=epoch,
                    strategy=strategy,
                    num_replicas=num_replicas,
                    publish_ref=publish_ref,
                    shard_type=shard_type,
                    leader_node=leader_node,
                    remote_transport=remote_transport,
                    system_name=system_name,
                    buffer=(*buffer, msg),
                    logger=log,
                )

            case SyncAllocations(new_allocs, new_epoch):
                if new_epoch >= epoch:
                    log.debug("Synced allocations epoch %d -> %d (%d shards)", epoch, new_epoch, len(new_allocs))
                    behavior = follower_behavior(
                        allocations=new_allocs,
                        nodes=nodes,
                        region_nodes=region_nodes,
                        epoch=new_epoch,
                        strategy=strategy,
                        num_replicas=num_replicas,
                        publish_ref=publish_ref,
                        shard_type=shard_type,
                        leader_node=leader_node,
                        remote_transport=remote_transport,
                        system_name=system_name,
                        logger=log,
                    )
                    for buffered in buffer:
                        ctx.self.tell(buffered)
                    return behavior
                return Behaviors.same()

            case SetRole(is_leader, new_leader_node):
                if is_leader:
                    log.info("Coordinator [%s] promoted to leader", shard_type)
                    return leader_behavior(
                        allocations=allocations,
                        nodes=nodes,
                        region_nodes=region_nodes,
                        strategy=strategy,
                        num_replicas=num_replicas,
                        publish_ref=publish_ref,
                        shard_type=shard_type,
                        epoch=epoch,
                        remote_transport=remote_transport,
                        system_name=system_name,
                        logger=log,
                    )
                # Update leader_node and replay buffer (leader may have changed)
                behavior = follower_behavior(
                    allocations=allocations,
                    nodes=nodes,
                    region_nodes=region_nodes,
                    epoch=epoch,
                    strategy=strategy,
                    num_replicas=num_replicas,
                    publish_ref=publish_ref,
                    shard_type=shard_type,
                    leader_node=new_leader_node,
                    remote_transport=remote_transport,
                    system_name=system_name,
                    logger=log,
                )
                for buffered in buffer:
                    ctx.self.tell(buffered)
                return behavior

            case UpdateTopology(new_nodes):
                return follower_behavior(
                    allocations=allocations,
                    nodes=new_nodes,
                    region_nodes=region_nodes,
                    epoch=epoch,
                    strategy=strategy,
                    num_replicas=num_replicas,
                    publish_ref=publish_ref,
                    shard_type=shard_type,
                    leader_node=leader_node,
                    remote_transport=remote_transport,
                    system_name=system_name,
                    buffer=buffer,
                    logger=log,
                )

            case _:
                return Behaviors.same()

    return Behaviors.receive(receive)
