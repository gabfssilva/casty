# src/casty/_shard_region_actor.py
from __future__ import annotations

from collections.abc import Callable
from typing import Any, cast

from casty.actor import Behavior, Behaviors
from casty.ref import ActorRef
from casty.sharding import ShardEnvelope


def shard_region_actor(
    *,
    entity_factory: Callable[[str], Behavior[Any]],
    **_kwargs: Any,
) -> Behavior[Any]:
    """Region that delivers ShardEnvelopes to local entities.

    Extra keyword arguments (self_node, coordinator, num_shards) are
    accepted for caller compatibility but not used.
    """
    async def setup(ctx: Any) -> Any:
        entities: dict[str, ActorRef[Any]] = {}

        async def receive(ctx: Any, msg: Any) -> Any:
            match msg:
                case ShardEnvelope():
                    envelope = cast(ShardEnvelope[Any], msg)
                    _deliver_local(ctx, entities, envelope.entity_id, envelope.message, entity_factory)
                    return Behaviors.same()

                case _:
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
