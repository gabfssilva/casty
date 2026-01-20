"""Event Log Example - Event Sourcing Pattern.

Demonstrates:
- Append-only event log for storing domain events
- State reconstruction via event replay
- Bank account example with Deposited/Withdrawn events
- Snapshotting for efficient replay

Run with:
    uv run python examples/practical/05-event-log.py
"""

import asyncio
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any
from uuid import uuid4

from casty import actor, ActorSystem, Mailbox


@dataclass(frozen=True)
class AccountOpened:
    account_id: str
    owner: str
    initial_balance: float = 0.0
    event_id: str = field(default_factory=lambda: str(uuid4())[:8])
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass(frozen=True)
class Deposited:
    account_id: str
    amount: float
    event_id: str = field(default_factory=lambda: str(uuid4())[:8])
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass(frozen=True)
class Withdrawn:
    account_id: str
    amount: float
    event_id: str = field(default_factory=lambda: str(uuid4())[:8])
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass(frozen=True)
class AccountClosed:
    account_id: str
    event_id: str = field(default_factory=lambda: str(uuid4())[:8])
    timestamp: datetime = field(default_factory=datetime.now)


DomainEvent = AccountOpened | Deposited | Withdrawn | AccountClosed


@dataclass
class Append:
    event: DomainEvent


@dataclass
class GetEvents:
    aggregate_id: str | None = None


@dataclass
class GetEventsSince:
    since_seq: int


@dataclass
class CreateSnapshot:
    aggregate_id: str
    state: dict[str, Any]


@dataclass
class GetSnapshot:
    aggregate_id: str


@dataclass
class _StoredEvent:
    seq: int
    event: DomainEvent


@dataclass
class _Snapshot:
    aggregate_id: str
    seq: int
    state: dict[str, Any]
    created_at: datetime = field(default_factory=datetime.now)


EventLogMsg = Append | GetEvents | GetEventsSince | CreateSnapshot | GetSnapshot


@actor
async def event_log(log_name: str = "default", *, mailbox: Mailbox[EventLogMsg]):
    events: list[_StoredEvent] = []
    snapshots: dict[str, _Snapshot] = {}
    next_seq = 1

    async for msg, ctx in mailbox:
        match msg:
            case Append(event):
                nonlocal next_seq
                stored = _StoredEvent(seq=next_seq, event=event)
                events.append(stored)
                next_seq += 1
                print(f"[EventLog:{log_name}] #{stored.seq} {type(event).__name__}")
                await ctx.reply(stored.seq)

            case GetEvents(aggregate_id):
                if aggregate_id:
                    result = [
                        se.event for se in events
                        if getattr(se.event, "account_id", None) == aggregate_id
                    ]
                else:
                    result = [se.event for se in events]
                await ctx.reply(result)

            case GetEventsSince(since_seq):
                result = [se.event for se in events if se.seq > since_seq]
                await ctx.reply(result)

            case CreateSnapshot(aggregate_id, state):
                snapshot = _Snapshot(
                    aggregate_id=aggregate_id,
                    seq=next_seq - 1,
                    state=state,
                )
                snapshots[aggregate_id] = snapshot
                print(f"[EventLog:{log_name}] Snapshot created for {aggregate_id} at seq {snapshot.seq}")

            case GetSnapshot(aggregate_id):
                snapshot = snapshots.get(aggregate_id)
                await ctx.reply(snapshot)


@dataclass
class AccountState:
    account_id: str
    owner: str
    balance: float
    is_open: bool
    version: int


class BankAccount:
    def __init__(self, account_id: str):
        self.account_id = account_id
        self.state: AccountState | None = None

    def apply(self, event: DomainEvent) -> None:
        match event:
            case AccountOpened(account_id=aid, owner=owner, initial_balance=balance):
                self.state = AccountState(
                    account_id=aid,
                    owner=owner,
                    balance=balance,
                    is_open=True,
                    version=1,
                )
            case Deposited(amount=amount):
                if self.state:
                    self.state.balance += amount
                    self.state.version += 1
            case Withdrawn(amount=amount):
                if self.state:
                    self.state.balance -= amount
                    self.state.version += 1
            case AccountClosed():
                if self.state:
                    self.state.is_open = False
                    self.state.version += 1

    def replay(self, events: list[DomainEvent]) -> None:
        for event in events:
            self.apply(event)

    def restore_from_snapshot(self, snapshot: _Snapshot) -> None:
        self.state = AccountState(**snapshot.state)


async def main():
    print("=" * 60)
    print("Casty Event Sourcing Example")
    print("=" * 60)
    print()

    async with ActorSystem() as system:
        log = await system.actor(event_log(log_name="bank"), name="event-log-bank")
        print("Event log 'bank' created")
        print()

        account_id = "ACC-001"

        print("--- Recording events ---")
        events_to_record = [
            AccountOpened(account_id=account_id, owner="Alice", initial_balance=100.0),
            Deposited(account_id=account_id, amount=50.0),
            Deposited(account_id=account_id, amount=25.0),
            Withdrawn(account_id=account_id, amount=30.0),
            Deposited(account_id=account_id, amount=100.0),
        ]

        for event in events_to_record:
            await log.ask(Append(event))
        print()

        print("--- Reconstructing state from events ---")
        events: list[DomainEvent] = await log.ask(GetEvents(account_id))
        print(f"Retrieved {len(events)} events")

        account = BankAccount(account_id)
        account.replay(events)

        print(f"Account state after replay:")
        print(f"  Account ID: {account.state.account_id}")
        print(f"  Owner:      {account.state.owner}")
        print(f"  Balance:    ${account.state.balance:.2f}")
        print(f"  Is Open:    {account.state.is_open}")
        print(f"  Version:    {account.state.version}")
        print()

        print("--- Creating snapshot ---")
        await log.send(CreateSnapshot(
            aggregate_id=account_id,
            state={
                "account_id": account.state.account_id,
                "owner": account.state.owner,
                "balance": account.state.balance,
                "is_open": account.state.is_open,
                "version": account.state.version,
            },
        ))
        await asyncio.sleep(0.1)
        print()

        print("--- Recording more events ---")
        more_events = [
            Withdrawn(account_id=account_id, amount=45.0),
            Deposited(account_id=account_id, amount=200.0),
        ]
        for event in more_events:
            await log.ask(Append(event))
        print()

        print("--- Reconstructing from snapshot + new events ---")
        snapshot: _Snapshot = await log.ask(GetSnapshot(account_id))
        new_events: list[DomainEvent] = await log.ask(GetEventsSince(snapshot.seq))

        print(f"Snapshot at seq {snapshot.seq}")
        print(f"New events since snapshot: {len(new_events)}")

        account2 = BankAccount(account_id)
        account2.restore_from_snapshot(snapshot)
        print(f"State from snapshot: balance=${account2.state.balance:.2f}")

        for event in new_events:
            account2.apply(event)

        print(f"State after applying new events:")
        print(f"  Balance:  ${account2.state.balance:.2f}")
        print(f"  Version:  {account2.state.version}")
        print()

        print("--- All events in log ---")
        all_events: list[DomainEvent] = await log.ask(GetEvents())
        for i, event in enumerate(all_events, 1):
            event_type = type(event).__name__
            match event:
                case AccountOpened(owner=owner, initial_balance=balance):
                    print(f"  {i}. {event_type}: owner={owner}, initial=${balance:.2f}")
                case Deposited(amount=amount):
                    print(f"  {i}. {event_type}: +${amount:.2f}")
                case Withdrawn(amount=amount):
                    print(f"  {i}. {event_type}: -${amount:.2f}")
                case _:
                    print(f"  {i}. {event_type}")

    print()
    print("=" * 60)
    print("Event log closed")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
