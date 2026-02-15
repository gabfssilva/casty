# tests/test_event_sourcing.py
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

from casty import ActorRef, ActorSystem, Behaviors
from casty.actor import Behavior, SnapshotEvery
from casty.core.journal import InMemoryJournal, JournalKind, PersistedEvent, Snapshot


@dataclass(frozen=True)
class Increment:
    amount: int


@dataclass(frozen=True)
class GetValue:
    reply_to: ActorRef[int]


type CounterMsg = Increment | GetValue


@dataclass(frozen=True)
class Incremented:
    amount: int


@dataclass(frozen=True)
class CounterState:
    value: int


def apply_event(state: CounterState, event: Incremented) -> CounterState:
    return CounterState(value=state.value + event.amount)


async def handle_command(ctx: Any, state: CounterState, cmd: CounterMsg) -> Any:
    match cmd:
        case Increment(amount):
            return Behaviors.persisted(events=[Incremented(amount)])
        case GetValue(reply_to):
            reply_to.tell(state.value)
            return Behaviors.same()
    return Behaviors.unhandled()


def counter_entity(entity_id: str, journal: InMemoryJournal) -> Behavior[CounterMsg]:
    return Behaviors.event_sourced(
        entity_id=entity_id,
        journal=journal,
        initial_state=CounterState(value=0),
        on_event=apply_event,
        on_command=handle_command,
    )


def counter_entity_with_snapshots(
    entity_id: str, journal: InMemoryJournal
) -> Behavior[CounterMsg]:
    return Behaviors.event_sourced(
        entity_id=entity_id,
        journal=journal,
        initial_state=CounterState(value=0),
        on_event=apply_event,
        on_command=handle_command,
        snapshot_policy=SnapshotEvery(n_events=3),
    )


async def test_event_sourced_basic_persist_and_read() -> None:
    journal = InMemoryJournal()
    async with ActorSystem(name="test") as system:
        ref = system.spawn(counter_entity("c1", journal), "counter")
        await asyncio.sleep(0.1)

        ref.tell(Increment(10))
        ref.tell(Increment(5))
        await asyncio.sleep(0.1)

        value = await system.ask(ref, lambda r: GetValue(reply_to=r), timeout=5.0)
        assert value == 15

    # Verify events were persisted
    events = await journal.load("c1")
    assert len(events) == 2
    assert events[0].event == Incremented(10)
    assert events[1].event == Incremented(5)


async def test_event_sourced_recovery_from_journal() -> None:
    journal = InMemoryJournal()

    # Pre-populate journal with events (simulating previous run)
    await journal.persist(
        "c1",
        [
            PersistedEvent(sequence_nr=1, event=Incremented(100), timestamp=1.0),
            PersistedEvent(sequence_nr=2, event=Incremented(50), timestamp=2.0),
        ],
    )

    async with ActorSystem(name="test") as system:
        ref = system.spawn(counter_entity("c1", journal), "counter")
        await asyncio.sleep(0.1)

        # State should be recovered: 100 + 50 = 150
        value = await system.ask(ref, lambda r: GetValue(reply_to=r), timeout=5.0)
        assert value == 150


async def test_snapshot_taken_after_n_events() -> None:
    journal = InMemoryJournal()
    async with ActorSystem(name="test") as system:
        ref = system.spawn(counter_entity_with_snapshots("c1", journal), "counter")
        await asyncio.sleep(0.1)

        for _i in range(3):
            ref.tell(Increment(1))
        await asyncio.sleep(0.1)

        snapshot = await journal.load_snapshot("c1")
        assert snapshot is not None
        assert snapshot.state == CounterState(value=3)
        assert snapshot.sequence_nr == 3


async def test_recovery_from_snapshot_plus_events() -> None:
    journal = InMemoryJournal()

    # Pre-populate with snapshot + newer events
    await journal.save_snapshot(
        "c1",
        Snapshot(sequence_nr=3, state=CounterState(value=30), timestamp=3.0),
    )
    await journal.persist(
        "c1",
        [
            PersistedEvent(sequence_nr=1, event=Incremented(10), timestamp=1.0),
            PersistedEvent(sequence_nr=2, event=Incremented(10), timestamp=2.0),
            PersistedEvent(sequence_nr=3, event=Incremented(10), timestamp=3.0),
            PersistedEvent(sequence_nr=4, event=Incremented(7), timestamp=4.0),
        ],
    )

    async with ActorSystem(name="test") as system:
        ref = system.spawn(counter_entity("c1", journal), "counter")
        await asyncio.sleep(0.1)

        # Should recover from snapshot (30) + event 4 (7) = 37
        value = await system.ask(ref, lambda r: GetValue(reply_to=r), timeout=5.0)
        assert value == 37


async def test_event_sourced_pushes_to_replicas() -> None:
    """Primary entity pushes persisted events to replica refs."""
    from casty.core.replication import ReplicateEvents

    journal = InMemoryJournal()
    received_replications: list[Any] = []

    async def mock_replica_handler(ctx: Any, msg: Any) -> Any:
        match msg:
            case ReplicateEvents():
                received_replications.append(msg)
        return Behaviors.same()

    async with ActorSystem(name="test") as system:
        replica_ref = system.spawn(Behaviors.receive(mock_replica_handler), "replica")
        await asyncio.sleep(0.1)

        def counter_with_replicas(entity_id: str) -> Behavior[CounterMsg]:
            return Behaviors.event_sourced(
                entity_id=entity_id,
                journal=journal,
                initial_state=CounterState(value=0),
                on_event=apply_event,
                on_command=handle_command,
                replica_refs=[replica_ref],
            )

        ref = system.spawn(counter_with_replicas("c1"), "counter")
        await asyncio.sleep(0.1)

        ref.tell(Increment(10))
        await asyncio.sleep(0.2)

        assert len(received_replications) == 1
        assert received_replications[0].entity_id == "c1"
        assert len(received_replications[0].events) == 1


async def test_event_sourced_waits_for_acks() -> None:
    """When min_acks > 0, primary waits for replica acks."""
    from casty.core.replication import (
        ReplicateEvents,
        ReplicateEventsAck,
        ReplicationConfig,
    )

    journal = InMemoryJournal()
    replica_journal = InMemoryJournal()
    ack_events: list[Any] = []

    async def acking_replica_handler(ctx: Any, msg: Any) -> Any:
        match msg:
            case ReplicateEvents(entity_id=eid, events=events, reply_to=reply_to):
                await replica_journal.persist(eid, events)
                ack_events.append(msg)
                if reply_to is not None:
                    highest_seq = events[-1].sequence_nr if events else 0
                    reply_to.tell(
                        ReplicateEventsAck(entity_id=eid, sequence_nr=highest_seq)
                    )
        return Behaviors.same()

    async with ActorSystem(name="test") as system:
        replica_ref = system.spawn(Behaviors.receive(acking_replica_handler), "replica")
        await asyncio.sleep(0.1)

        def counter_with_acks(entity_id: str) -> Behavior[CounterMsg]:
            return Behaviors.event_sourced(
                entity_id=entity_id,
                journal=journal,
                initial_state=CounterState(value=0),
                on_event=apply_event,
                on_command=handle_command,
                replica_refs=[replica_ref],
                replication=ReplicationConfig(replicas=1, min_acks=1, ack_timeout=5.0),
            )

        ref = system.spawn(counter_with_acks("c1"), "counter")
        await asyncio.sleep(0.1)

        ref.tell(Increment(10))
        await asyncio.sleep(0.3)

        # Verify replication was done
        assert len(ack_events) == 1

        # Verify state is correct (meaning ack was processed and didn't break anything)
        value = await system.ask(ref, lambda r: GetValue(reply_to=r), timeout=5.0)
        assert value == 10


async def test_event_sourced_fire_and_forget_replication() -> None:
    """When min_acks=0, primary doesn't wait for acks."""
    from casty.core.replication import ReplicateEvents, ReplicationConfig

    journal = InMemoryJournal()
    replications: list[Any] = []

    async def slow_replica_handler(ctx: Any, msg: Any) -> Any:
        match msg:
            case ReplicateEvents():
                await asyncio.sleep(0.5)  # Simulate slow replica
                replications.append(msg)
        return Behaviors.same()

    async with ActorSystem(name="test") as system:
        replica_ref = system.spawn(Behaviors.receive(slow_replica_handler), "replica")
        await asyncio.sleep(0.1)

        def counter_fire_forget(entity_id: str) -> Behavior[CounterMsg]:
            return Behaviors.event_sourced(
                entity_id=entity_id,
                journal=journal,
                initial_state=CounterState(value=0),
                on_event=apply_event,
                on_command=handle_command,
                replica_refs=[replica_ref],
                replication=ReplicationConfig(replicas=1, min_acks=0),
            )

        ref = system.spawn(counter_fire_forget("c1"), "counter")
        await asyncio.sleep(0.1)

        ref.tell(Increment(10))
        await asyncio.sleep(0.1)

        # Primary should respond quickly even though replica is slow
        value = await system.ask(ref, lambda r: GetValue(reply_to=r), timeout=5.0)
        assert value == 10


async def test_centralized_journal_replica_skips_persist() -> None:
    """With a centralized journal, replicas apply events but skip persist."""
    from casty.cluster.replica import replica_region_actor
    from casty.core.replication import ReplicateEvents, ReplicateEventsAck

    class CentralizedJournal(InMemoryJournal):
        @property
        def kind(self) -> JournalKind:
            return JournalKind.centralized

    replica_journal = CentralizedJournal()

    events = (
        PersistedEvent(sequence_nr=1, event=Incremented(10), timestamp=1.0),
        PersistedEvent(sequence_nr=2, event=Incremented(5), timestamp=2.0),
    )

    async with ActorSystem(name="test") as system:
        replica = system.spawn(
            replica_region_actor(
                journal=replica_journal,
                on_event=apply_event,
                initial_state=CounterState(value=0),
            ),
            "replica",
        )
        await asyncio.sleep(0.1)

        # Simulate primary sending ReplicateEvents
        ack_ref: ActorRef[ReplicateEventsAck] = system.spawn(
            Behaviors.receive(lambda ctx, msg: Behaviors.same()), "ack-sink"
        )
        await asyncio.sleep(0.1)

        replica.tell(
            ReplicateEvents(entity_id="c1", shard_id=0, events=events, reply_to=ack_ref)
        )
        await asyncio.sleep(0.1)

        loaded = await replica_journal.load("c1")
        assert loaded == []
