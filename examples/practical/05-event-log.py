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

from casty import Actor, ActorSystem, Context, on


# --- Domain Events ---

@dataclass(frozen=True)
class AccountOpened:
    """Account was opened."""
    account_id: str
    owner: str
    initial_balance: float = 0.0
    event_id: str = field(default_factory=lambda: str(uuid4())[:8])
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass(frozen=True)
class Deposited:
    """Money was deposited."""
    account_id: str
    amount: float
    event_id: str = field(default_factory=lambda: str(uuid4())[:8])
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass(frozen=True)
class Withdrawn:
    """Money was withdrawn."""
    account_id: str
    amount: float
    event_id: str = field(default_factory=lambda: str(uuid4())[:8])
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass(frozen=True)
class AccountClosed:
    """Account was closed."""
    account_id: str
    event_id: str = field(default_factory=lambda: str(uuid4())[:8])
    timestamp: datetime = field(default_factory=datetime.now)


type DomainEvent = AccountOpened | Deposited | Withdrawn | AccountClosed


# --- Event Log Messages ---

@dataclass
class Append:
    """Append an event to the log."""
    event: DomainEvent


@dataclass
class GetEvents:
    """Get all events (optionally filtered by aggregate ID)."""
    aggregate_id: str | None = None


@dataclass
class GetEventsSince:
    """Get events since a specific sequence number."""
    since_seq: int


@dataclass
class CreateSnapshot:
    """Create a snapshot of current state."""
    aggregate_id: str
    state: dict[str, Any]


@dataclass
class GetSnapshot:
    """Get the latest snapshot for an aggregate."""
    aggregate_id: str


# --- Event Log Actor ---

@dataclass
class _StoredEvent:
    """Event with sequence number."""
    seq: int
    event: DomainEvent


@dataclass
class _Snapshot:
    """Stored snapshot."""
    aggregate_id: str
    seq: int  # Sequence number at snapshot time
    state: dict[str, Any]
    created_at: datetime = field(default_factory=datetime.now)


type EventLogMessage = Append | GetEvents | GetEventsSince | CreateSnapshot | GetSnapshot


class EventLog(Actor[EventLogMessage]):
    """Append-only event log with snapshotting support.

    Stores events in sequence order and supports:
    - Appending events
    - Querying events (all or by aggregate)
    - Creating and retrieving snapshots
    """

    def __init__(self, name: str = "default"):
        self.name = name
        self.events: list[_StoredEvent] = []
        self.snapshots: dict[str, _Snapshot] = {}  # aggregate_id -> latest snapshot
        self.next_seq = 1

    @on(Append)
    async def handle_append(self, msg: Append, ctx: Context) -> None:
        stored = _StoredEvent(seq=self.next_seq, event=msg.event)
        self.events.append(stored)
        self.next_seq += 1
        print(f"[EventLog:{self.name}] #{stored.seq} {type(msg.event).__name__}")
        await ctx.reply(stored.seq)

    @on(GetEvents)
    async def handle_get_events(self, msg: GetEvents, ctx: Context) -> None:
        if msg.aggregate_id:
            # Filter by aggregate ID (assumes events have an account_id field)
            events = [
                se.event for se in self.events
                if getattr(se.event, "account_id", None) == msg.aggregate_id
            ]
        else:
            events = [se.event for se in self.events]
        await ctx.reply(events)

    @on(GetEventsSince)
    async def handle_get_since(self, msg: GetEventsSince, ctx: Context) -> None:
        events = [se.event for se in self.events if se.seq > msg.since_seq]
        await ctx.reply(events)

    @on(CreateSnapshot)
    async def handle_create_snapshot(self, msg: CreateSnapshot, ctx: Context) -> None:
        snapshot = _Snapshot(
            aggregate_id=msg.aggregate_id,
            seq=self.next_seq - 1,
            state=msg.state,
        )
        self.snapshots[msg.aggregate_id] = snapshot
        print(f"[EventLog:{self.name}] Snapshot created for {msg.aggregate_id} at seq {snapshot.seq}")

    @on(GetSnapshot)
    async def handle_get_snapshot(self, msg: GetSnapshot, ctx: Context) -> None:
        snapshot = self.snapshots.get(msg.aggregate_id)
        await ctx.reply(snapshot)


# --- Bank Account (Event-Sourced Aggregate) ---

@dataclass
class AccountState:
    """Current state of an account."""
    account_id: str
    owner: str
    balance: float
    is_open: bool
    version: int  # Number of events applied


class BankAccount:
    """Event-sourced bank account aggregate.

    State is rebuilt by replaying events.
    """

    def __init__(self, account_id: str):
        self.account_id = account_id
        self.state: AccountState | None = None

    def apply(self, event: DomainEvent) -> None:
        """Apply an event to update state."""
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
        """Replay events to reconstruct state."""
        for event in events:
            self.apply(event)

    def restore_from_snapshot(self, snapshot: _Snapshot) -> None:
        """Restore state from a snapshot."""
        self.state = AccountState(**snapshot.state)


# --- Main ---

async def main():
    print("=" * 60)
    print("Casty Event Sourcing Example")
    print("=" * 60)
    print()

    async with ActorSystem() as system:
        # Create event log
        event_log = await system.spawn(EventLog, name="bank")
        print("Event log 'bank' created")
        print()

        account_id = "ACC-001"

        # Record events for a bank account
        print("--- Recording events ---")
        events_to_record = [
            AccountOpened(account_id=account_id, owner="Alice", initial_balance=100.0),
            Deposited(account_id=account_id, amount=50.0),
            Deposited(account_id=account_id, amount=25.0),
            Withdrawn(account_id=account_id, amount=30.0),
            Deposited(account_id=account_id, amount=100.0),
        ]

        for event in events_to_record:
            seq = await event_log.ask(Append(event))
        print()

        # Replay events to reconstruct account state
        print("--- Reconstructing state from events ---")
        events: list[DomainEvent] = await event_log.ask(GetEvents(account_id))
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

        # Create a snapshot
        print("--- Creating snapshot ---")
        await event_log.send(CreateSnapshot(
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

        # Record more events
        print("--- Recording more events ---")
        more_events = [
            Withdrawn(account_id=account_id, amount=45.0),
            Deposited(account_id=account_id, amount=200.0),
        ]
        for event in more_events:
            await event_log.ask(Append(event))
        print()

        # Reconstruct from snapshot + new events
        print("--- Reconstructing from snapshot + new events ---")
        snapshot: _Snapshot = await event_log.ask(GetSnapshot(account_id))
        new_events: list[DomainEvent] = await event_log.ask(GetEventsSince(snapshot.seq))

        print(f"Snapshot at seq {snapshot.seq}")
        print(f"New events since snapshot: {len(new_events)}")

        # Restore from snapshot
        account2 = BankAccount(account_id)
        account2.restore_from_snapshot(snapshot)
        print(f"State from snapshot: balance=${account2.state.balance:.2f}")

        # Apply new events
        for event in new_events:
            account2.apply(event)

        print(f"State after applying new events:")
        print(f"  Balance:  ${account2.state.balance:.2f}")
        print(f"  Version:  {account2.state.version}")
        print()

        # Show all events
        print("--- All events in log ---")
        all_events: list[DomainEvent] = await event_log.ask(GetEvents())
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
