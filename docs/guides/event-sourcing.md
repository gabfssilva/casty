# Event Sourcing

In this guide you'll build a **bank account** that persists every transaction as an event. Along the way you'll learn the command-event-state flow, how actors recover from the journal on restart, and how snapshots speed up recovery.

## The Idea

Instead of saving current state ("balance is 120"), event sourcing saves *what happened* ("deposited 100, deposited 50, withdrew 30"). State is derived by replaying events. This gives you a full audit trail and the ability to reconstruct any past state.

## State

A frozen dataclass holding the derived state:

```python
--8<-- "examples/guides/05_event_sourcing.py:20:23"
```

## Events

Events are facts that already happened. They're persisted to the journal and replayed during recovery:

```python
--8<-- "examples/guides/05_event_sourcing.py:29:39"
```

## Commands

Commands are requests from the outside world. The actor decides whether to accept or reject them:

```python
--8<-- "examples/guides/05_event_sourcing.py:45:61"
```

Notice the separation: `Deposit` (command) vs `Deposited` (event). The command is the intent; the event is the outcome.

## Event Handler

A pure function that applies one event to the current state. No side effects — this is called both during normal operation and during recovery:

```python
--8<-- "examples/guides/05_event_sourcing.py:67:78"
```

## Command Handler

The command handler decides which events to persist. It returns `Behaviors.persisted([...])` to queue events, or `Behaviors.same()` for read-only operations:

```python
--8<-- "examples/guides/05_event_sourcing.py:84:103"
```

Walking through the arms:

- **Deposit** — always succeeds. Persists a `Deposited` event.
- **Withdraw (sufficient funds)** — replies "ok", persists `Withdrawn`.
- **Withdraw (insufficient)** — replies with error, persists nothing.
- **GetBalance** — read-only. No events, just a reply.

## Wiring It Together

`Behaviors.event_sourced()` ties the pieces together:

```python
--8<-- "examples/guides/05_event_sourcing.py:109:117"
```

`SnapshotEvery(n_events=5)` tells the journal to save a full state snapshot after every 5 events. On recovery, the actor loads the snapshot first and only replays events *after* it — much faster for entities with long histories.

## Running It

The example runs in three phases: transactions, recovery, and snapshot recovery:

```python
--8<-- "examples/guides/05_event_sourcing.py:123:194"
```

Output:

```
── Phase 1: transactions ──
  Depositing 100
  Depositing 50
  Withdrawing 30
  Withdraw result: ok
  Withdraw result: insufficient funds (balance: 120)
  Balance: 120

── Journal has 3 events ──
  #1: Deposited(amount=100)
  #2: Deposited(amount=50)
  #3: Withdrawn(amount=30)

── Phase 2: recovery ──
  Recovered balance: 120

── Phase 3: snapshot recovery ──
  Depositing 10
  ...
  Balance after 7 deposits: 70
  Snapshot at seq #5: AccountState(balance=50, transactions=5)
  Journal has 7 events total
  Recovery replays only 2 events
```

Phase 2 shows recovery: a new actor with the same `entity_id` replays all events from the journal and arrives at the same state. Phase 3 shows snapshot optimization: after 7 deposits, a snapshot was taken at event #5. On recovery, only 2 events (6 and 7) need replaying.

## Run the Full Example

```bash
git clone https://github.com/gabfssilva/casty.git
cd casty
uv run python examples/guides/05_event_sourcing.py
```

---

**What you learned:**

- **Commands** are requests; **events** are facts. Commands may be rejected; events are always applied.
- **`on_event`** is a pure function that applies events to state — used for both normal operation and recovery.
- **`on_command`** decides which events to persist. Return `Behaviors.persisted([...])` or `Behaviors.same()`.
- **Recovery** replays all events from the journal, reconstructing state automatically.
- **Snapshots** speed up recovery by checkpointing state at regular intervals.
