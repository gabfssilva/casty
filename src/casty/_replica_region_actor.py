# src/casty/_replica_region_actor.py
from __future__ import annotations

from collections.abc import Callable
from typing import Any

from casty.actor import Behavior, Behaviors
from casty.journal import EventJournal
from casty.replication import ReplicateEvents


def replica_region_actor[S, E](
    *,
    journal: EventJournal,
    on_event: Callable[[S, E], S],
    initial_state: S,
) -> Behavior[Any]:
    """Manages passive entity replicas on a node.

    Receives ReplicateEvents from primary entities, persists to local journal,
    and applies events to maintain replica state.
    """

    async def setup(ctx: Any) -> Any:
        entity_states: dict[str, S] = {}
        entity_sequence_nrs: dict[str, int] = {}

        async def receive(ctx: Any, msg: Any) -> Any:
            match msg:
                case ReplicateEvents(entity_id=entity_id, events=events, reply_to=reply_to):
                    state = entity_states.get(entity_id, initial_state)

                    # Persist events to local journal
                    await journal.persist(entity_id, events)

                    # Apply events to state
                    for persisted in events:
                        state = on_event(state, persisted.event)
                        entity_sequence_nrs[entity_id] = persisted.sequence_nr

                    entity_states[entity_id] = state

                    # Send ack if requested
                    if reply_to is not None:
                        from casty.replication import ReplicateEventsAck
                        highest_seq = entity_sequence_nrs.get(entity_id, 0)
                        reply_to.tell(ReplicateEventsAck(entity_id=entity_id, sequence_nr=highest_seq))

                    return Behaviors.same()

                case _:
                    return Behaviors.same()

        return Behaviors.receive(receive)

    return Behaviors.setup(setup)
