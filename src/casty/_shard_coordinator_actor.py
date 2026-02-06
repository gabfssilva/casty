# src/casty/_shard_coordinator_actor.py
from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from typing import Any, Protocol

from casty.actor import Behavior, Behaviors
from casty.cluster_state import NodeAddress
from casty.ref import ActorRef
from casty.replication import ReplicationConfig, ShardAllocation


@dataclass(frozen=True)
class GetShardLocation:
    shard_id: int
    reply_to: ActorRef[ShardLocation]


@dataclass(frozen=True)
class ShardLocation:
    shard_id: int
    node: NodeAddress
    replicas: list[NodeAddress] = field(
        default_factory=lambda: list[NodeAddress]()
    )


@dataclass(frozen=True)
class UpdateTopology:
    available_nodes: frozenset[NodeAddress]


@dataclass(frozen=True)
class NodeDown:
    node: NodeAddress


type CoordinatorMsg = GetShardLocation | UpdateTopology | NodeDown


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


def shard_coordinator_actor(
    *,
    strategy: ShardAllocationStrategy,
    available_nodes: frozenset[NodeAddress],
    replication: ReplicationConfig | None = None,
) -> Behavior[CoordinatorMsg]:
    num_replicas = replication.replicas if replication is not None else 0

    def active(
        allocations: dict[int, ShardAllocation],
        nodes: frozenset[NodeAddress],
    ) -> Behavior[CoordinatorMsg]:
        async def receive(ctx: Any, msg: CoordinatorMsg) -> Any:
            match msg:
                case GetShardLocation(shard_id, reply_to):
                    if shard_id in allocations:
                        alloc = allocations[shard_id]
                        reply_to.tell(
                            ShardLocation(
                                shard_id=shard_id,
                                node=alloc.primary,
                                replicas=list(alloc.replicas),
                            )
                        )
                        return Behaviors.same()

                    primary_allocs = {
                        sid: a.primary for sid, a in allocations.items()
                    }
                    primary = strategy.allocate(shard_id, primary_allocs, nodes)

                    replica_nodes: list[NodeAddress] = []
                    remaining = nodes - {primary}
                    for _ in range(min(num_replicas, len(remaining))):
                        replica = min(
                            remaining,
                            key=lambda n: sum(
                                1 for a in allocations.values() if n in a.replicas
                            ),
                        )
                        replica_nodes.append(replica)
                        remaining = remaining - {replica}

                    new_alloc = ShardAllocation(
                        primary=primary, replicas=replica_nodes
                    )
                    new_allocations = {**allocations, shard_id: new_alloc}
                    reply_to.tell(
                        ShardLocation(
                            shard_id=shard_id,
                            node=primary,
                            replicas=replica_nodes,
                        )
                    )
                    return active(new_allocations, nodes)

                case UpdateTopology(new_nodes):
                    return active(allocations, new_nodes)

                case NodeDown(failed_node):
                    new_allocations = dict(allocations)
                    for shard_id, alloc in list(allocations.items()):
                        if alloc.primary == failed_node:
                            if alloc.replicas:
                                new_primary = alloc.replicas[0]
                                new_replicas = [
                                    r
                                    for r in alloc.replicas[1:]
                                    if r != failed_node
                                ]
                                new_allocations[shard_id] = ShardAllocation(
                                    primary=new_primary,
                                    replicas=new_replicas,
                                )
                            else:
                                del new_allocations[shard_id]
                        else:
                            new_replicas = [
                                r
                                for r in alloc.replicas
                                if r != failed_node
                            ]
                            if new_replicas != list(alloc.replicas):
                                new_allocations[shard_id] = ShardAllocation(
                                    primary=alloc.primary,
                                    replicas=new_replicas,
                                )
                    new_nodes = nodes - {failed_node}
                    return active(new_allocations, new_nodes)

                case _:
                    return Behaviors.same()

        return Behaviors.receive(receive)

    return active({}, available_nodes)
