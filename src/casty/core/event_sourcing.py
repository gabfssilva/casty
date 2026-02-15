"""Event sourcing behavior wrapper.

Encapsulates all event sourcing logic (recovery, persistence, snapshotting,
replication) as a self-contained setup behavior. Replaces the baked-in ES
fields that previously lived in ``ActorCell``.
"""

from __future__ import annotations

import asyncio
import logging
import time as _time
from collections.abc import Awaitable, Callable
from typing import Any, TYPE_CHECKING

from casty.core.behavior import Behavior, Signal
from casty.behaviors import Behaviors
from casty.core.journal import PersistedEvent, Snapshot

if TYPE_CHECKING:
    from casty.actor import PersistedBehavior, SnapshotPolicy
    from casty.core.context import ActorContext
    from casty.core.journal import EventJournal
    from casty.core.ref import ActorRef
    from casty.core.replication import ReplicationConfig

type CommandResult[M, E] = Behavior[M] | PersistedBehavior[M, E]


def event_sourced_wrapper[M, E, S](
    *,
    entity_id: str,
    journal: EventJournal,
    initial_state: S,
    on_event: Callable[[S, E], S],
    on_command: Callable[[ActorContext[M], S, M], Awaitable[CommandResult[M, E]]],
    snapshot_policy: SnapshotPolicy | None = None,
    replica_refs: tuple[ActorRef[Any], ...] = (),
    replication: ReplicationConfig | None = None,
) -> Behavior[Any]:
    logger = logging.getLogger(f"casty.es.{entity_id}")

    async def setup(ctx: ActorContext[Any]) -> Behavior[Any]:
        state = initial_state
        sequence_nr = 0

        snapshot = await journal.load_snapshot(entity_id)
        if snapshot is not None:
            state = snapshot.state
            sequence_nr = snapshot.sequence_nr
            logger.debug("Recovering from snapshot seq_nr=%d", snapshot.sequence_nr)

        events = await journal.load(entity_id, from_sequence_nr=sequence_nr + 1)
        for persisted in events:
            state = on_event(state, persisted.event)
            sequence_nr = persisted.sequence_nr
        if events:
            logger.debug(
                "Recovered: replayed %d events to seq_nr=%d",
                len(events),
                sequence_nr,
            )

        ack_queue: asyncio.Queue[Any] = asyncio.Queue()

        def ack_interceptor(msg: object) -> bool:
            from casty.core.replication import ReplicateEventsAck

            match msg:
                case ReplicateEventsAck():
                    ack_queue.put_nowait(msg)
                    return True
                case _:
                    return False

        if replica_refs:
            ctx.register_interceptor(ack_interceptor)

        return active(state, sequence_nr, 0, ack_queue)

    def active(
        state: S,
        sequence_nr: int,
        events_since_snapshot: int,
        ack_queue: asyncio.Queue[Any],
    ) -> Behavior[Any]:
        async def receive(ctx: ActorContext[Any], msg: Any) -> Behavior[Any]:
            from casty.actor import PersistedBehavior

            result = await on_command(ctx, state, msg)

            if isinstance(result, PersistedBehavior):
                persisted: PersistedBehavior[Any, Any] = result
                new_state, new_seq, new_snap_count = await _persist_and_apply(
                    entity_id=entity_id,
                    journal=journal,
                    on_event=on_event,
                    state=state,
                    sequence_nr=sequence_nr,
                    events_since_snapshot=events_since_snapshot,
                    new_events=persisted.events,
                    snapshot_policy=snapshot_policy,
                    replica_refs=replica_refs,
                    replication=replication,
                    self_ref=ctx.self,
                    ack_queue=ack_queue,
                    logger=logger,
                )
                if persisted.then.signal is Signal.stopped:
                    return Behaviors.stopped()
                return active(new_state, new_seq, new_snap_count, ack_queue)

            return result

        return Behaviors.receive(receive)

    return Behaviors.setup(setup)


async def _persist_and_apply[S, E](
    *,
    entity_id: str,
    journal: EventJournal,
    on_event: Callable[[S, E], S],
    state: S,
    sequence_nr: int,
    events_since_snapshot: int,
    new_events: tuple[E, ...],
    snapshot_policy: SnapshotPolicy | None,
    replica_refs: tuple[ActorRef[Any], ...],
    replication: ReplicationConfig | None,
    self_ref: ActorRef[Any],
    ack_queue: asyncio.Queue[Any],
    logger: logging.Logger,
) -> tuple[S, int, int]:
    wrapped: list[PersistedEvent[Any]] = []
    new_seq = sequence_nr
    for event in new_events:
        new_seq += 1
        wrapped.append(
            PersistedEvent(
                sequence_nr=new_seq,
                event=event,
                timestamp=_time.time(),
            )
        )

    await journal.persist(entity_id, wrapped)
    logger.debug("Persisted %d events (seq_nr=%d)", len(wrapped), new_seq)

    new_state = state
    for persisted_event in wrapped:
        new_state = on_event(new_state, persisted_event.event)

    new_snap_count = events_since_snapshot + len(wrapped)

    from casty.actor import SnapshotEvery

    match snapshot_policy:
        case SnapshotEvery(n_events=n_events):
            if new_snap_count >= n_events:
                snapshot = Snapshot(
                    sequence_nr=new_seq,
                    state=new_state,
                    timestamp=_time.time(),
                )
                await journal.save_snapshot(entity_id, snapshot)
                new_snap_count = 0
                logger.debug("Snapshot saved at seq_nr=%d", new_seq)
        case _:
            pass

    if replica_refs:
        from casty.core.replication import ReplicateEvents

        needs_acks = (
            replication is not None
            and hasattr(replication, "min_acks")
            and replication.min_acks > 0
        )
        replication_msg = ReplicateEvents(
            entity_id=entity_id,
            shard_id=0,
            events=tuple(wrapped),
            reply_to=self_ref if needs_acks else None,
        )
        for replica_ref in replica_refs:
            replica_ref.tell(replication_msg)

    if replica_refs and replication is not None:
        from casty.core.replication import ReplicationConfig as _RC

        match replication:
            case _RC(min_acks=min_acks, ack_timeout=ack_timeout) if min_acks > 0:
                acks_needed = min(min_acks, len(replica_refs))
                ack_count = 0
                deadline = _time.monotonic() + ack_timeout

                while ack_count < acks_needed and _time.monotonic() < deadline:
                    try:
                        remaining_time = max(0.001, deadline - _time.monotonic())
                        await asyncio.wait_for(
                            ack_queue.get(),
                            timeout=remaining_time,
                        )
                        ack_count += 1
                    except asyncio.TimeoutError:
                        logger.warning(
                            "Replication ack timeout for %s: got %d/%d acks",
                            entity_id,
                            ack_count,
                            acks_needed,
                        )
                        break
            case _:
                pass

    return new_state, new_seq, new_snap_count
