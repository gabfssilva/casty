# tests/test_replica_region.py
from __future__ import annotations

import asyncio
from dataclasses import dataclass

from casty import ActorSystem
from casty.journal import InMemoryJournal, PersistedEvent
from casty.replication import ReplicateEvents
from casty.replica_region_actor import replica_region_actor


@dataclass(frozen=True)
class Deposited:
    amount: int


@dataclass(frozen=True)
class AccountState:
    balance: int


def apply_event(state: AccountState, event: Deposited) -> AccountState:
    return AccountState(balance=state.balance + event.amount)


async def test_replica_region_receives_and_applies_events() -> None:
    journal = InMemoryJournal()

    async with ActorSystem(name="test") as system:
        region = system.spawn(
            replica_region_actor(
                journal=journal,
                on_event=apply_event,
                initial_state=AccountState(balance=0),
            ),
            "replica-region",
        )
        await asyncio.sleep(0.1)

        events = [
            PersistedEvent(sequence_nr=1, event=Deposited(100), timestamp=1.0),
            PersistedEvent(sequence_nr=2, event=Deposited(50), timestamp=2.0),
        ]
        region.tell(ReplicateEvents(entity_id="alice", shard_id=0, events=events))
        await asyncio.sleep(0.1)

        loaded = await journal.load("alice")
        assert len(loaded) == 2


async def test_replica_region_tracks_multiple_entities() -> None:
    journal = InMemoryJournal()

    async with ActorSystem(name="test") as system:
        region = system.spawn(
            replica_region_actor(
                journal=journal,
                on_event=apply_event,
                initial_state=AccountState(balance=0),
            ),
            "replica-region",
        )
        await asyncio.sleep(0.1)

        region.tell(ReplicateEvents(
            entity_id="alice", shard_id=0,
            events=[PersistedEvent(sequence_nr=1, event=Deposited(100), timestamp=1.0)],
        ))
        region.tell(ReplicateEvents(
            entity_id="bob", shard_id=1,
            events=[PersistedEvent(sequence_nr=1, event=Deposited(200), timestamp=1.0)],
        ))
        await asyncio.sleep(0.1)

        alice_events = await journal.load("alice")
        assert len(alice_events) == 1
        bob_events = await journal.load("bob")
        assert len(bob_events) == 1


async def test_replica_region_appends_events() -> None:
    journal = InMemoryJournal()

    async with ActorSystem(name="test") as system:
        region = system.spawn(
            replica_region_actor(
                journal=journal,
                on_event=apply_event,
                initial_state=AccountState(balance=0),
            ),
            "replica-region",
        )
        await asyncio.sleep(0.1)

        region.tell(ReplicateEvents(
            entity_id="alice", shard_id=0,
            events=[PersistedEvent(sequence_nr=1, event=Deposited(100), timestamp=1.0)],
        ))
        region.tell(ReplicateEvents(
            entity_id="alice", shard_id=0,
            events=[PersistedEvent(sequence_nr=2, event=Deposited(50), timestamp=2.0)],
        ))
        await asyncio.sleep(0.1)

        loaded = await journal.load("alice")
        assert len(loaded) == 2
