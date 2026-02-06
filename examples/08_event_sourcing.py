"""Event Sourcing — bank account with persistent event log.

State is derived from a sequence of events, never stored directly.
On recovery, events are replayed from the journal to reconstruct state.

    Command (Deposit/Withdraw)
        |
        v
    on_command(ctx, state, cmd) -> Behaviors.persisted(events=[...])
        |
        v
    Persist events to journal
        |
        v
    on_event(state, event) -> new state   (pure, sync)
        |
        v
    Snapshot policy check (every N events)

Key concepts:
  - EventSourcedBehavior: wraps command handling with persist-then-apply
  - on_command: async handler that returns Behaviors.persisted() or Behaviors.same()
  - on_event: pure function that folds an event into state (used for replay too)
  - InMemoryJournal: stores events + snapshots in memory (for demos/tests)
  - SnapshotEvery: takes a snapshot every N events for faster recovery
"""

import asyncio
from dataclasses import dataclass
from typing import Any

from casty import ActorRef, ActorSystem, Behaviors
from casty.actor import Behavior, SnapshotEvery
from casty.journal import InMemoryJournal


# --- State ---


@dataclass(frozen=True)
class AccountState:
    balance: int
    tx_count: int


# --- Events (what gets persisted) ---


@dataclass(frozen=True)
class Deposited:
    amount: int


@dataclass(frozen=True)
class Withdrawn:
    amount: int


type AccountEvent = Deposited | Withdrawn


# --- Commands (what users send) ---


@dataclass(frozen=True)
class Deposit:
    amount: int


@dataclass(frozen=True)
class Withdraw:
    amount: int
    reply_to: ActorRef[str]


@dataclass(frozen=True)
class GetBalance:
    reply_to: ActorRef[AccountState]


type AccountCommand = Deposit | Withdraw | GetBalance


# --- Event handler (pure, sync — used for replay and live updates) ---


def on_event(state: AccountState, event: AccountEvent) -> AccountState:
    match event:
        case Deposited(amount):
            return AccountState(balance=state.balance + amount, tx_count=state.tx_count + 1)
        case Withdrawn(amount):
            return AccountState(balance=state.balance - amount, tx_count=state.tx_count + 1)
    return state


# --- Command handler (async — decides what events to persist) ---


async def on_command(
    ctx: Any, state: AccountState, cmd: AccountCommand
) -> Any:
    match cmd:
        case Deposit(amount):
            print(f"  [account] deposit +{amount}")
            return Behaviors.persisted(events=[Deposited(amount)])

        case Withdraw(amount, reply_to) if state.balance >= amount:
            print(f"  [account] withdraw -{amount}")
            reply_to.tell("ok")
            return Behaviors.persisted(events=[Withdrawn(amount)])

        case Withdraw(amount, reply_to):
            print(f"  [account] insufficient funds for -{amount} (balance={state.balance})")
            reply_to.tell(f"insufficient funds (balance={state.balance})")
            return Behaviors.same()

        case GetBalance(reply_to):
            reply_to.tell(state)
            return Behaviors.same()

    return Behaviors.unhandled()


# --- Entity factory ---


def bank_account(entity_id: str, journal: InMemoryJournal) -> Behavior[AccountCommand]:
    return Behaviors.event_sourced(
        entity_id=entity_id,
        journal=journal,
        initial_state=AccountState(balance=0, tx_count=0),
        on_event=on_event,
        on_command=on_command,
        snapshot_policy=SnapshotEvery(n_events=5),
    )


# --- Main ---


async def main() -> None:
    print("=== Event Sourcing ===\n")

    # Shared journal — in a real app this would be a database
    journal = InMemoryJournal()

    # --- Phase 1: Normal operations ---
    print("--- Phase 1: Deposits and withdrawals ---")
    async with ActorSystem(name="bank") as system:
        account = system.spawn(bank_account("acc-001", journal), "account")
        await asyncio.sleep(0.1)

        account.tell(Deposit(1000))
        account.tell(Deposit(500))
        account.tell(Deposit(250))
        await asyncio.sleep(0.1)

        result = await system.ask(
            account, lambda r: Withdraw(300, reply_to=r), timeout=2.0
        )
        print(f"  withdraw result: {result}")

        result = await system.ask(
            account, lambda r: Withdraw(9999, reply_to=r), timeout=2.0
        )
        print(f"  withdraw result: {result}")

        state = await system.ask(
            account, lambda r: GetBalance(reply_to=r), timeout=2.0
        )
        print(f"  balance: {state.balance}, transactions: {state.tx_count}")

    # --- Phase 2: Recovery from journal ---
    print("\n--- Phase 2: Recovery (new actor, same journal) ---")

    # Check what's in the journal
    events = await journal.load("acc-001")
    snapshot = await journal.load_snapshot("acc-001")
    print(f"  journal has {len(events)} events")
    if snapshot:
        print(f"  snapshot at seq={snapshot.sequence_nr}: {snapshot.state}")

    async with ActorSystem(name="bank") as system:
        # Spawn a NEW actor with the SAME journal — it recovers automatically
        account = system.spawn(bank_account("acc-001", journal), "account")
        await asyncio.sleep(0.1)

        state = await system.ask(
            account, lambda r: GetBalance(reply_to=r), timeout=2.0
        )
        print(f"  recovered balance: {state.balance}, transactions: {state.tx_count}")

        # Continue from where we left off
        account.tell(Deposit(100))
        await asyncio.sleep(0.1)

        state = await system.ask(
            account, lambda r: GetBalance(reply_to=r), timeout=2.0
        )
        print(f"  after new deposit: {state.balance}, transactions: {state.tx_count}")

    print("\n=== Done ===")


asyncio.run(main())
