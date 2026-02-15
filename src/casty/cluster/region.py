# src/casty/_shard_region_actor.py
from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any, cast

from casty.actor import Behavior, Behaviors
from casty.core.messages import Terminated
from casty.ref import ActorRef
from casty.cluster.envelope import ShardEnvelope


def shard_region_actor(
    *,
    entity_factory: Callable[[str], Behavior[Any]],
    logger: logging.Logger | None = None,
    **_kwargs: Any,
) -> Behavior[Any]:
    """Region that delivers ShardEnvelopes to local entities.

    Extra keyword arguments (self_node, coordinator, num_shards) are
    accepted for caller compatibility but not used.
    """
    log = logger or logging.getLogger("casty.region")

    def active(
        entities: dict[str, ActorRef[Any]],
        refs_to_ids: dict[ActorRef[Any], str],
    ) -> Behavior[Any]:
        async def receive(ctx: Any, msg: Any) -> Any:
            match msg:
                case ShardEnvelope():
                    envelope = cast(ShardEnvelope[Any], msg)
                    entity_id = envelope.entity_id
                    if entity_id not in entities:
                        log.debug("Spawning entity: %s", entity_id)
                        ref = ctx.spawn(
                            entity_factory(entity_id), f"entity-{entity_id}"
                        )
                        ctx.watch(ref)
                        new_entities = {**entities, entity_id: ref}
                        new_refs = {**refs_to_ids, ref: entity_id}
                        new_entities[entity_id].tell(envelope.message)
                        return active(new_entities, new_refs)
                    entities[entity_id].tell(envelope.message)
                    return Behaviors.same()

                case Terminated(ref=ref) if ref in refs_to_ids:
                    entity_id = refs_to_ids[ref]
                    log.debug("Entity stopped: %s", entity_id)
                    new_entities = {k: v for k, v in entities.items() if k != entity_id}
                    new_refs = {k: v for k, v in refs_to_ids.items() if k != ref}
                    return active(new_entities, new_refs)

                case _:
                    return Behaviors.same()

        return Behaviors.receive(receive)

    return active({}, {})
