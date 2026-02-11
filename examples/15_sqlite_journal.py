"""SQLite Journal — durable event sourcing with SqliteJournal.

Same bank account as example 08, but events persist to a real SQLite file.
Kill the process mid-way, re-run, and the account recovers from disk.

    $ uv run examples/15_sqlite_journal.py
    $ ls -la bank.db          # SQLite file with events + snapshots
    $ uv run examples/15_sqlite_journal.py   # re-run: balance survives!

Key difference from 08_event_sourcing:
  - SqliteJournal("bank.db") replaces InMemoryJournal()
  - Data survives process restarts — no external database needed
"""

import asyncio
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from casty import ActorRef, ActorSystem, Behaviors
from casty.actor import Behavior, SnapshotEvery
from casty.journal import SqliteJournal


# --- State ---


@dataclass(frozen=True)
class AccountState:
    balance: int
    tx_count: int


# --- Events ---


@dataclass(frozen=True)
class Deposited:
    amount: int


@dataclass(frozen=True)
class Withdrawn:
    amount: int


type AccountEvent = Deposited | Withdrawn


# --- Commands ---


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


# --- Event handler ---


def on_event(state: AccountState, event: AccountEvent) -> AccountState:
    match event:
        case Deposited(amount):
            return AccountState(balance=state.balance + amount, tx_count=state.tx_count + 1)
        case Withdrawn(amount):
            return AccountState(balance=state.balance - amount, tx_count=state.tx_count + 1)
    return state


# --- Command handler ---


async def on_command(ctx: Any, state: AccountState, cmd: AccountCommand) -> Any:
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


def bank_account(entity_id: str, journal: SqliteJournal) -> Behavior[AccountCommand]:
    return Behaviors.event_sourced(
        entity_id=entity_id,
        journal=journal,
        initial_state=AccountState(balance=0, tx_count=0),
        on_event=on_event,
        on_command=on_command,
        snapshot_policy=SnapshotEvery(n_events=5),
    )


# --- Main ---

DB_PATH = Path("bank.db")


async def main() -> None:
    journal = SqliteJournal(DB_PATH)
    fresh = not DB_PATH.exists() or (await journal.load("acc-001")) == []

    if fresh:
        print("=== First run — creating account ===\n")
    else:
        snapshot = await journal.load_snapshot("acc-001")
        events = await journal.load("acc-001")
        print(f"=== Recovering from disk ({len(events)} events")
        if snapshot:
            print(f"    snapshot at seq={snapshot.sequence_nr}: {snapshot.state}")
        print("===\n")

    async with ActorSystem(name="bank") as system:
        account = system.spawn(bank_account("acc-001", journal), "account")
        await asyncio.sleep(0.1)

        if fresh:
            account.tell(Deposit(1000))
            account.tell(Deposit(500))
            account.tell(Deposit(250))
            await asyncio.sleep(0.1)

            result = await system.ask(
                account, lambda r: Withdraw(300, reply_to=r), timeout=2.0
            )
            print(f"  withdraw result: {result}")

        state = await system.ask(
            account, lambda r: GetBalance(reply_to=r), timeout=2.0
        )
        print(f"\n  balance: {state.balance}, transactions: {state.tx_count}")

        account.tell(Deposit(100))
        await asyncio.sleep(0.1)

        state = await system.ask(
            account, lambda r: GetBalance(reply_to=r), timeout=2.0
        )
        print(f"  after +100 deposit: {state.balance}, transactions: {state.tx_count}")

    journal.close()
    print(f"\n  events persisted to {DB_PATH.resolve()}")
    print("  run again to see recovery from disk!\n")


asyncio.run(main())
