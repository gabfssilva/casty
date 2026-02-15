# src/casty/_replica_region_actor.py
from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any

from casty.actor import Behavior, Behaviors
from casty.core.journal import EventJournal, JournalKind
from casty.core.replication import ReplicateEvents, ReplicateEventsAck


def replica_region_actor[S, E](
    *,
    journal: EventJournal,
    on_event: Callable[[S, E], S],
    initial_state: S,
    logger: logging.Logger | None = None,
) -> Behavior[Any]:
    """Manages passive entity replicas on a node.

    Receives ReplicateEvents from primary entities, persists to local journal,
    and applies events to maintain replica state.
    """
    log = logger or logging.getLogger("casty.replica")

    def active(
        entity_states: dict[str, S],
        entity_sequence_nrs: dict[str, int],
    ) -> Behavior[Any]:
        async def receive(ctx: Any, msg: Any) -> Any:
            match msg:
                case ReplicateEvents(
                    entity_id=entity_id, events=events, reply_to=reply_to
                ):
                    state = entity_states.get(entity_id, initial_state)

                    if journal.kind == JournalKind.local:
                        await journal.persist(entity_id, events)

                    highest_seq = entity_sequence_nrs.get(entity_id, 0)
                    for persisted in events:
                        state = on_event(state, persisted.event)
                        highest_seq = persisted.sequence_nr

                    log.debug(
                        "Replicated %d events for %s (seq_nr=%d)",
                        len(events),
                        entity_id,
                        highest_seq,
                    )
                    new_entity_states = {**entity_states, entity_id: state}
                    new_sequence_nrs = {**entity_sequence_nrs, entity_id: highest_seq}

                    if reply_to is not None:
                        reply_to.tell(
                            ReplicateEventsAck(
                                entity_id=entity_id, sequence_nr=highest_seq
                            )
                        )

                    return active(new_entity_states, new_sequence_nrs)

                case _:
                    return Behaviors.same()

        return Behaviors.receive(receive)

    return active({}, {})
