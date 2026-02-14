"""Event Sourcing — command, event, state, snapshots, and replay."""

import asyncio
from dataclasses import dataclass

from casty import (
    ActorContext,
    ActorRef,
    ActorSystem,
    Behavior,
    Behaviors,
    InMemoryJournal,
    SnapshotEvery,
)


# ── State ────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class AccountState:
    balance: int = 0
    transactions: int = 0


# ── Events (persisted to the journal) ────────────────────────────────


@dataclass(frozen=True)
class Deposited:
    amount: int


@dataclass(frozen=True)
class Withdrawn:
    amount: int


type AccountEvent = Deposited | Withdrawn


# ── Commands (messages the actor receives) ───────────────────────────


@dataclass(frozen=True)
class Deposit:
    amount: int


@dataclass(frozen=True)
class Withdraw:
    amount: int
    reply_to: ActorRef[str]


@dataclass(frozen=True)
class GetBalance:
    reply_to: ActorRef[int]


type AccountCmd = Deposit | Withdraw | GetBalance


# ── Event handler (pure function: event + state → new state) ─────────


def apply_event(state: AccountState, event: AccountEvent) -> AccountState:
    match event:
        case Deposited(amount):
            return AccountState(
                balance=state.balance + amount,
                transactions=state.transactions + 1,
            )
        case Withdrawn(amount):
            return AccountState(
                balance=state.balance - amount,
                transactions=state.transactions + 1,
            )


# ── Command handler (decides which events to persist) ────────────────


async def handle_command(
    ctx: ActorContext[AccountCmd], state: AccountState, cmd: AccountCmd
) -> Behavior[AccountCmd]:
    match cmd:
        case Deposit(amount):
            print(f"  Depositing {amount}")
            return Behaviors.persisted([Deposited(amount)])

        case Withdraw(amount, reply_to) if amount <= state.balance:
            print(f"  Withdrawing {amount}")
            reply_to.tell("ok")
            return Behaviors.persisted([Withdrawn(amount)])

        case Withdraw(amount, reply_to):
            reply_to.tell(f"insufficient funds (balance: {state.balance})")
            return Behaviors.same()

        case GetBalance(reply_to):
            reply_to.tell(state.balance)
            return Behaviors.same()


# ── Actor factory ────────────────────────────────────────────────────


def bank_account(entity_id: str, journal: InMemoryJournal) -> Behavior[AccountCmd]:
    return Behaviors.event_sourced(
        entity_id=entity_id,
        journal=journal,
        initial_state=AccountState(),
        on_event=apply_event,
        on_command=handle_command,
        snapshot_policy=SnapshotEvery(n_events=5),
    )


# ── Main ─────────────────────────────────────────────────────────────


async def main() -> None:
    journal = InMemoryJournal()

    # --- Phase 1: build up state ---
    print("── Phase 1: transactions ──")
    async with ActorSystem(name="bank") as system:
        account: ActorRef[AccountCmd] = system.spawn(
            bank_account("acct-1", journal), "account"
        )

        account.tell(Deposit(100))
        account.tell(Deposit(50))
        await asyncio.sleep(0.1)

        result = await system.ask(
            account,
            lambda r: Withdraw(30, reply_to=r),
            timeout=5.0,
        )
        print(f"  Withdraw result: {result}")

        result = await system.ask(
            account,
            lambda r: Withdraw(999, reply_to=r),
            timeout=5.0,
        )
        print(f"  Withdraw result: {result}")

        balance = await system.ask(
            account, lambda r: GetBalance(reply_to=r), timeout=5.0
        )
        print(f"  Balance: {balance}")

    # Show what was persisted
    events = await journal.load("acct-1")
    print(f"\n── Journal has {len(events)} events ──")
    for e in events:
        print(f"  #{e.sequence_nr}: {e.event}")

    # --- Phase 2: recover from journal ---
    print("\n── Phase 2: recovery ──")
    async with ActorSystem(name="bank") as system:
        account = system.spawn(bank_account("acct-1", journal), "account")
        await asyncio.sleep(0.1)

        balance = await system.ask(
            account, lambda r: GetBalance(reply_to=r), timeout=5.0
        )
        print(f"  Recovered balance: {balance}")

    # --- Phase 3: demonstrate snapshot recovery ---
    print("\n── Phase 3: snapshot recovery ──")
    snap_journal = InMemoryJournal()

    async with ActorSystem(name="bank") as system:
        account = system.spawn(
            bank_account("acct-2", snap_journal), "account"
        )
        for _ in range(7):
            account.tell(Deposit(10))
        await asyncio.sleep(0.2)

        balance = await system.ask(
            account, lambda r: GetBalance(reply_to=r), timeout=5.0
        )
        print(f"  Balance after 7 deposits: {balance}")

    snapshot = await snap_journal.load_snapshot("acct-2")
    events = await snap_journal.load("acct-2")
    print(f"  Snapshot at seq #{snapshot.sequence_nr}: {snapshot.state}" if snapshot else "  No snapshot")
    print(f"  Journal has {len(events)} events total")
    print(f"  Recovery replays only {len(events) - (snapshot.sequence_nr if snapshot else 0)} events")


asyncio.run(main())
