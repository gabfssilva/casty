# src/casty/_shard_coordinator_actor.py
from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from typing import Any, Protocol

from casty.actor import Behavior, Behaviors
from casty.cluster_state import NodeAddress
from casty.ref import ActorRef


@dataclass(frozen=True)
class GetShardLocation:
    shard_id: int
    reply_to: ActorRef[ShardLocation]


@dataclass(frozen=True)
class ShardLocation:
    shard_id: int
    node: NodeAddress


@dataclass(frozen=True)
class UpdateTopology:
    available_nodes: frozenset[NodeAddress]


type CoordinatorMsg = GetShardLocation | UpdateTopology


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
) -> Behavior[CoordinatorMsg]:
    def active(
        allocations: dict[int, NodeAddress],
        nodes: frozenset[NodeAddress],
    ) -> Behavior[CoordinatorMsg]:
        async def receive(ctx: Any, msg: CoordinatorMsg) -> Any:
            match msg:
                case GetShardLocation(shard_id, reply_to):
                    if shard_id in allocations:
                        reply_to.tell(
                            ShardLocation(shard_id=shard_id, node=allocations[shard_id])
                        )
                        return Behaviors.same()

                    node = strategy.allocate(shard_id, allocations, nodes)
                    new_allocations = {**allocations, shard_id: node}
                    reply_to.tell(ShardLocation(shard_id=shard_id, node=node))
                    return active(new_allocations, nodes)

                case UpdateTopology(new_nodes):
                    return active(allocations, new_nodes)

                case _:
                    return Behaviors.same()

        return Behaviors.receive(receive)

    return active({}, available_nodes)
