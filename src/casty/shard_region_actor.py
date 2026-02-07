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

    def active(entities: dict[str, ActorRef[Any]]) -> Behavior[Any]:
        async def receive(ctx: Any, msg: Any) -> Any:
            match msg:
                case ShardEnvelope():
                    envelope = cast(ShardEnvelope[Any], msg)
                    entity_id = envelope.entity_id
                    if entity_id not in entities:
                        ref = ctx.spawn(
                            entity_factory(entity_id), f"entity-{entity_id}"
                        )
                        new_entities = {**entities, entity_id: ref}
                        new_entities[entity_id].tell(envelope.message)
                        return active(new_entities)
                    entities[entity_id].tell(envelope.message)
                    return Behaviors.same()

                case _:
                    return Behaviors.same()

        return Behaviors.receive(receive)

    return active({})
