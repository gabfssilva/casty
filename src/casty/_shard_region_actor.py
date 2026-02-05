# src/casty/_shard_region_actor.py
from __future__ import annotations

from collections.abc import Callable
from typing import Any, cast

from casty.actor import Behavior, Behaviors
from casty.cluster_state import NodeAddress
from casty.ref import ActorRef
from casty.sharding import ShardEnvelope
from casty._shard_coordinator_actor import CoordinatorMsg, ShardLocation


def shard_region_actor(
    *,
    self_node: NodeAddress,
    coordinator: ActorRef[CoordinatorMsg],
    entity_factory: Callable[[str], Behavior[Any]],
    num_shards: int,
) -> Behavior[Any]:
    async def setup(ctx: Any) -> Any:
        entities: dict[str, ActorRef[Any]] = {}
        shard_locations: dict[int, NodeAddress] = {}

        async def receive(ctx: Any, msg: Any) -> Any:
            if isinstance(msg, ShardEnvelope):
                envelope = cast(ShardEnvelope[Any], msg)
                entity_id: str = envelope.entity_id
                inner_msg: Any = envelope.message
                shard_id = hash(entity_id) % num_shards

                # For single-node: assume local allocation
                if shard_id not in shard_locations:
                    shard_locations[shard_id] = self_node

                node = shard_locations[shard_id]
                if node == self_node:
                    _deliver_local(ctx, entities, entity_id, inner_msg, entity_factory)

                return Behaviors.same()

            if isinstance(msg, ShardLocation):
                shard_locations[msg.shard_id] = msg.node
                return Behaviors.same()

            return Behaviors.same()

        return Behaviors.receive(receive)

    return Behaviors.setup(setup)


def _deliver_local(
    ctx: Any,
    entities: dict[str, ActorRef[Any]],
    entity_id: str,
    msg: Any,
    entity_factory: Callable[[str], Behavior[Any]],
) -> None:
    if entity_id not in entities:
        ref = ctx.spawn(entity_factory(entity_id), f"entity-{entity_id}")
        entities[entity_id] = ref
    entities[entity_id].tell(msg)
