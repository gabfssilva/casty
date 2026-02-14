from __future__ import annotations

import logging
from collections import Counter
from dataclasses import dataclass
from typing import Any, Protocol, TYPE_CHECKING

from casty.actor import Behavior, Behaviors
from casty.address import ActorAddress
from casty.cluster_state import MemberStatus, NodeAddress
from casty.ref import ActorRef
from casty.replication import ReplicationConfig, ShardAllocation
from casty.topology import TopologySnapshot

if TYPE_CHECKING:
    from casty.context import ActorContext
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
class PublishAllocations:
    shard_type: str
    allocations: dict[int, ShardAllocation]
    epoch: int


@dataclass(frozen=True)
class RegisterRegion:
    node: NodeAddress


type CoordinatorMsg = (
    GetShardLocation
    | PublishAllocations
    | RegisterRegion
    | TopologySnapshot
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


@dataclass(frozen=True)
class CoordinatorConfig:
    """Shared configuration threaded through all coordinator behaviors."""

    strategy: ShardAllocationStrategy
    num_replicas: int
    shard_type: str
    publish_ref: ActorRef[PublishAllocations] | None
    remote_transport: RemoteTransport | None
    system_name: str
    logger: logging.Logger
    self_node: NodeAddress | None


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
    topology_ref: ActorRef[Any] | None = None,
    self_node: NodeAddress | None = None,
) -> Behavior[CoordinatorMsg]:
    log = logger or logging.getLogger(f"casty.coordinator.{system_name}")

    cfg = CoordinatorConfig(
        strategy=strategy,
        num_replicas=replication.replicas if replication is not None else 0,
        shard_type=shard_type,
        publish_ref=publish_ref,
        remote_transport=remote_transport,
        system_name=system_name,
        logger=log,
        self_node=self_node,
    )

    initial = pending_behavior(
        cfg=cfg,
        nodes=available_nodes,
        region_nodes=frozenset(),
    )

    if topology_ref is not None and self_node is not None:
        async def setup(ctx: ActorContext[CoordinatorMsg]) -> Behavior[CoordinatorMsg]:
            from casty.topology import SubscribeTopology
            topology_ref.tell(SubscribeTopology(reply_to=ctx.self))  # type: ignore[arg-type]
            return initial
        return Behaviors.setup(setup)

    return initial


def pending_behavior(
    *,
    cfg: CoordinatorConfig,
    nodes: frozenset[NodeAddress],
    region_nodes: frozenset[NodeAddress],
    buffer: tuple[GetShardLocation, ...] = (),
) -> Behavior[CoordinatorMsg]:
    """Buffers requests until first TopologySnapshot arrives."""

    async def receive(
        ctx: ActorContext[CoordinatorMsg], msg: CoordinatorMsg,
    ) -> Behavior[CoordinatorMsg]:
        match msg:
            case TopologySnapshot() as snapshot:
                if cfg.self_node is None:
                    return Behaviors.same()
                is_leader = snapshot.leader == cfg.self_node
                up_nodes = frozenset(
                    m.address for m in snapshot.members
                    if m.status == MemberStatus.up
                )
                allocs = snapshot.shard_allocations.get(cfg.shard_type, {})
                if is_leader:
                    cfg.logger.info(
                        "Coordinator [%s] activated as leader (buffered=%d)",
                        cfg.shard_type, len(buffer),
                    )
                    behavior: Behavior[CoordinatorMsg] = leader_behavior(
                        cfg=cfg,
                        allocations=allocs,
                        nodes=up_nodes,
                        region_nodes=region_nodes,
                        epoch=snapshot.allocation_epoch,
                    )
                else:
                    cfg.logger.info(
                        "Coordinator [%s] activated as follower (buffered=%d)",
                        cfg.shard_type, len(buffer),
                    )
                    behavior = follower_behavior(
                        cfg=cfg,
                        allocations=allocs,
                        nodes=up_nodes,
                        region_nodes=region_nodes,
                        epoch=snapshot.allocation_epoch,
                        leader_node=snapshot.leader,
                    )
                    for node in region_nodes:
                        ctx.self.tell(RegisterRegion(node=node))
                for buffered in buffer:
                    ctx.self.tell(buffered)
                return behavior

            case RegisterRegion(node):
                return pending_behavior(
                    cfg=cfg,
                    nodes=nodes,
                    region_nodes=region_nodes | {node},
                    buffer=buffer,
                )

            case GetShardLocation():
                return pending_behavior(
                    cfg=cfg,
                    nodes=nodes,
                    region_nodes=region_nodes,
                    buffer=(*buffer, msg),
                )

            case _:
                return Behaviors.same()

    return Behaviors.receive(receive)


def leader_behavior(
    *,
    cfg: CoordinatorConfig,
    allocations: dict[int, ShardAllocation],
    nodes: frozenset[NodeAddress],
    region_nodes: frozenset[NodeAddress],
    epoch: int,
) -> Behavior[CoordinatorMsg]:
    """Leader mode: allocates new shards, publishes via publish_ref."""

    def publish(allocs: dict[int, ShardAllocation], new_epoch: int) -> None:
        if cfg.publish_ref is not None:
            cfg.publish_ref.tell(PublishAllocations(
                shard_type=cfg.shard_type,
                allocations=allocs,
                epoch=new_epoch,
            ))

    effective_nodes = nodes & region_nodes

    async def receive(
        ctx: ActorContext[CoordinatorMsg], msg: CoordinatorMsg,
    ) -> Behavior[CoordinatorMsg]:
        match msg:
            case TopologySnapshot() as snapshot:
                if cfg.self_node is None:
                    return Behaviors.same()
                is_leader = snapshot.leader == cfg.self_node
                up_nodes = frozenset(
                    m.address for m in snapshot.members
                    if m.status == MemberStatus.up
                )
                if not is_leader:
                    cfg.logger.info("Coordinator [%s] demoted to follower", cfg.shard_type)
                    allocs = snapshot.shard_allocations.get(cfg.shard_type, {})
                    behavior: Behavior[CoordinatorMsg] = follower_behavior(
                        cfg=cfg,
                        allocations=allocs if snapshot.allocation_epoch >= epoch else allocations,
                        nodes=up_nodes,
                        region_nodes=region_nodes,
                        epoch=max(snapshot.allocation_epoch, epoch),
                        leader_node=snapshot.leader,
                    )
                    for node in region_nodes:
                        ctx.self.tell(RegisterRegion(node=node))
                    return behavior

                current_allocs = allocations
                current_nodes = nodes
                current_epoch = epoch
                for unreachable_node in snapshot.unreachable:
                    if any(
                        a.primary == unreachable_node or unreachable_node in a.replicas
                        for a in current_allocs.values()
                    ):
                        cfg.logger.warning(
                            "NodeDown %s:%d (shards affected)",
                            unreachable_node.host, unreachable_node.port,
                        )
                        current_allocs, current_nodes = handle_node_down(
                            unreachable_node, current_allocs, current_nodes,
                        )
                        current_epoch += 1
                        publish(current_allocs, current_epoch)
                if snapshot.allocation_epoch > current_epoch:
                    allocs = snapshot.shard_allocations.get(cfg.shard_type, {})
                    return leader_behavior(
                        cfg=cfg,
                        allocations=allocs,
                        nodes=up_nodes,
                        region_nodes=region_nodes,
                        epoch=snapshot.allocation_epoch,
                    )
                effective_up = up_nodes - snapshot.unreachable
                if effective_up != nodes or current_allocs != allocations:
                    return leader_behavior(
                        cfg=cfg,
                        allocations=current_allocs,
                        nodes=effective_up,
                        region_nodes=region_nodes,
                        epoch=current_epoch,
                    )
                return Behaviors.same()

            case RegisterRegion(node):
                return leader_behavior(
                    cfg=cfg,
                    allocations=allocations,
                    nodes=nodes,
                    region_nodes=region_nodes | {node},
                    epoch=epoch,
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
                    shard_id, cfg.strategy, allocations, effective_nodes, cfg.num_replicas,
                )
                new_allocations = {**allocations, shard_id: new_alloc}
                reply_to.tell(ShardLocation(
                    shard_id=shard_id,
                    node=new_alloc.primary,
                    replicas=new_alloc.replicas,
                ))
                new_epoch = epoch + 1
                cfg.logger.info(
                    "Shard %d -> %s:%d (epoch=%d)",
                    shard_id, new_alloc.primary.host, new_alloc.primary.port, new_epoch,
                )
                publish(new_allocations, new_epoch)
                return leader_behavior(
                    cfg=cfg,
                    allocations=new_allocations,
                    nodes=nodes,
                    region_nodes=region_nodes,
                    epoch=new_epoch,
                )

            case _:
                return Behaviors.same()

    return Behaviors.receive(receive)


def follower_behavior(
    *,
    cfg: CoordinatorConfig,
    allocations: dict[int, ShardAllocation],
    nodes: frozenset[NodeAddress],
    region_nodes: frozenset[NodeAddress],
    epoch: int,
    leader_node: NodeAddress | None = None,
    buffer: tuple[GetShardLocation, ...] = (),
) -> Behavior[CoordinatorMsg]:
    """Follower mode: serves cached allocations, forwards unknown to leader."""

    async def receive(
        ctx: ActorContext[CoordinatorMsg], msg: CoordinatorMsg,
    ) -> Behavior[CoordinatorMsg]:
        match msg:
            case TopologySnapshot() as snapshot:
                if cfg.self_node is None:
                    return Behaviors.same()
                is_leader = snapshot.leader == cfg.self_node
                up_nodes = frozenset(
                    m.address for m in snapshot.members
                    if m.status == MemberStatus.up
                )
                allocs = snapshot.shard_allocations.get(cfg.shard_type, {})
                if is_leader:
                    cfg.logger.info("Coordinator [%s] promoted to leader", cfg.shard_type)
                    return leader_behavior(
                        cfg=cfg,
                        allocations=allocs if snapshot.allocation_epoch >= epoch else allocations,
                        nodes=up_nodes,
                        region_nodes=region_nodes,
                        epoch=max(snapshot.allocation_epoch, epoch),
                    )
                if snapshot.allocation_epoch >= epoch:
                    behavior: Behavior[CoordinatorMsg] = follower_behavior(
                        cfg=cfg,
                        allocations=allocs,
                        nodes=up_nodes,
                        region_nodes=region_nodes,
                        epoch=snapshot.allocation_epoch,
                        leader_node=snapshot.leader,
                    )
                    for buffered in buffer:
                        ctx.self.tell(buffered)
                    return behavior
                if snapshot.leader != leader_node:
                    behavior = follower_behavior(
                        cfg=cfg,
                        allocations=allocations,
                        nodes=up_nodes,
                        region_nodes=region_nodes,
                        epoch=epoch,
                        leader_node=snapshot.leader,
                    )
                    for buffered in buffer:
                        ctx.self.tell(buffered)
                    return behavior
                return Behaviors.same()

            case RegisterRegion(node):
                if leader_node is not None and cfg.remote_transport is not None:
                    leader_coord_addr = ActorAddress(
                        system=cfg.system_name,
                        path=f"/_coord-{cfg.shard_type}",
                        host=leader_node.host,
                        port=leader_node.port,
                    )
                    leader_ref: ActorRef[CoordinatorMsg] = cfg.remote_transport.make_ref(
                        leader_coord_addr
                    )
                    leader_ref.tell(msg)
                return follower_behavior(
                    cfg=cfg,
                    allocations=allocations,
                    nodes=nodes,
                    region_nodes=region_nodes | {node},
                    epoch=epoch,
                    leader_node=leader_node,
                    buffer=buffer,
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
                if leader_node is not None and cfg.remote_transport is not None:
                    cfg.logger.debug("Forwarding shard %d to leader", shard_id)
                    leader_coord_addr = ActorAddress(
                        system=cfg.system_name,
                        path=f"/_coord-{cfg.shard_type}",
                        host=leader_node.host,
                        port=leader_node.port,
                    )
                    leader_ref: ActorRef[CoordinatorMsg] = cfg.remote_transport.make_ref(
                        leader_coord_addr
                    )
                    leader_ref.tell(msg)
                    return Behaviors.same()
                return follower_behavior(
                    cfg=cfg,
                    allocations=allocations,
                    nodes=nodes,
                    region_nodes=region_nodes,
                    epoch=epoch,
                    leader_node=leader_node,
                    buffer=(*buffer, msg),
                )

            case _:
                return Behaviors.same()

    return Behaviors.receive(receive)
