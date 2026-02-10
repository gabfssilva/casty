# Event Sourcing

Traditional persistence stores the **current state** — a row in a database with `balance = 500`. Event sourcing stores the **sequence of facts that produced the state** — `Deposited(100)`, `Deposited(500)`, `Withdrawn(100)`. The current state is a derived value, computed by folding events from left to right.

This distinction has profound consequences:

- **Complete audit trail.** Every state change is recorded as an immutable event. You can reconstruct the state at any point in time by replaying events up to that moment.
- **Recovery after failure.** When an actor restarts (via supervision) or a process crashes and restarts, the actor replays its events from the journal and recovers its exact state. Supervision provides the restart; event sourcing provides the memory.
- **Separation of concerns.** The *decision* to change state (command handling) is separated from the *application* of the change (event handling). Commands can be rejected; events are facts that have already occurred and cannot be rejected.

Event sourcing introduces three distinct concepts:

- **Commands** — Messages from the outside world requesting a state change. "Deposit 100" is a command. It may be accepted or rejected (e.g., "insufficient funds" for a withdrawal).
- **Events** — Immutable records of what actually happened. "Deposited 100" is an event. Events are persisted to a journal and never modified.
- **State** — The current value derived from the event sequence. Computed by folding `on_event` over all persisted events from an initial state.

A regular Casty actor holds state in a closure — but closures cannot be replayed. Event sourcing requires a different behavior shape with two explicit functions:

- `on_event(state, event) -> state` — A pure, synchronous function that applies one event to the current state. Used both for live processing and for recovery (replay).
- `on_command(ctx, state, command) -> Behavior` — An async function that receives the current state and a command, decides what events to persist, and returns `Behaviors.persisted(events=[...])`.

```python
# --- State ---

@dataclass(frozen=True)
class AccountState:
    balance: int
    tx_count: int

# --- Events (persisted to the journal) ---

@dataclass(frozen=True)
class Deposited:
    amount: int

@dataclass(frozen=True)
class Withdrawn:
    amount: int

type AccountEvent = Deposited | Withdrawn

# --- Commands (sent by the outside world) ---

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

# --- Event handler (pure — used for replay and live updates) ---

def on_event(state: AccountState, event: AccountEvent) -> AccountState:
    match event:
        case Deposited(amount):
            return AccountState(balance=state.balance + amount, tx_count=state.tx_count + 1)
        case Withdrawn(amount):
            return AccountState(balance=state.balance - amount, tx_count=state.tx_count + 1)
    return state

# --- Command handler (async — decides what events to persist) ---

async def on_command(ctx: Any, state: AccountState, cmd: AccountCommand) -> Any:
    match cmd:
        case Deposit(amount):
            return Behaviors.persisted(events=[Deposited(amount)])
        case Withdraw(amount, reply_to) if state.balance >= amount:
            reply_to.tell("ok")
            return Behaviors.persisted(events=[Withdrawn(amount)])
        case Withdraw(_, reply_to):
            reply_to.tell(f"insufficient funds (balance={state.balance})")
            return Behaviors.same()
        case GetBalance(reply_to):
            reply_to.tell(state)
            return Behaviors.same()
    return Behaviors.unhandled()

# --- Entity factory ---

journal = InMemoryJournal()

def bank_account(entity_id: str) -> Behavior[AccountCommand]:
    return Behaviors.event_sourced(
        entity_id=entity_id,
        journal=journal,
        initial_state=AccountState(balance=0, tx_count=0),
        on_event=on_event,
        on_command=on_command,
        snapshot_policy=SnapshotEvery(n_events=100),
    )

# --- Recovery demonstration ---

async def main() -> None:
    # Phase 1: Normal operations
    async with ActorSystem(name="bank") as system:
        account = system.spawn(bank_account("acc-001"), "account")
        await asyncio.sleep(0.1)

        account.tell(Deposit(1000))
        account.tell(Deposit(500))
        await asyncio.sleep(0.1)

        state = await system.ask(
            account, lambda r: GetBalance(reply_to=r), timeout=2.0
        )
        print(f"Balance: {state.balance}, transactions: {state.tx_count}")
        # Balance: 1500, transactions: 2

    # Phase 2: Recovery — new actor, same journal
    async with ActorSystem(name="bank") as system:
        account = system.spawn(bank_account("acc-001"), "account")
        await asyncio.sleep(0.1)

        state = await system.ask(
            account, lambda r: GetBalance(reply_to=r), timeout=2.0
        )
        print(f"Recovered balance: {state.balance}, transactions: {state.tx_count}")
        # Recovered balance: 1500, transactions: 2

asyncio.run(main())
```

In Phase 2, a new actor is spawned with the same `entity_id`. The framework automatically loads the latest snapshot (if any), replays events from the journal since that snapshot, and reconstructs the state. The actor resumes exactly where it left off.

The `SnapshotEvery(n_events=100)` policy periodically saves the current state to the journal. Without snapshots, recovery requires replaying every event from the beginning — acceptable for entities with few events, but expensive for long-lived entities with thousands.

The `EventJournal` protocol is storage-agnostic. `InMemoryJournal` is included for testing and development. For production, implement the protocol for any database backend — the interface requires four methods: `persist`, `load`, `save_snapshot`, and `load_snapshot`.

---

**Next:** [Cluster Sharding](../clustering/cluster-sharding.md)
