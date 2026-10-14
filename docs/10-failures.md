# Failures and guarantees

A message can fail before delivery, while queued, during execution or after a state write has been accepted. Those stages have different implications for a retry. An exception is not generally evidence that no work happened.

## Reported failures

| Failure | Trigger | What to consider |
|---|---|---|
| `ActorFailed` | The body raised while handling an unanswered ask | Earlier writes or external effects can remain |
| `Unavailable` | Owner unreachable, ownership changing, insufficient replicas or a required store unavailable | Processing or a state change may already have happened |
| `TimeoutError` / `CancelledError` | The caller's deadline expires or it stops waiting | The runtime requests cancellation; it does not roll back completed work |
| `ReentrancyError` | An awaited ask closes a cycle of waiting actor runs | The cycle is rejected rather than allowed to deadlock |
| `MailboxFull` | A bounded mailbox refuses the ask | `on_full="wait"` instead waits for space within the deadline |
| `MessageTooLarge` | Serialized message, reply or ref's initial state exceeds its limit | An oversized outgoing value is refused before it is sent |
| `UnknownActor` | The owner cannot resolve the actor type | Members need compatible actor definitions |
| `NotStarted` | A received ref addresses a missing key whose type has no default initial state | Obtain a system ref with the required initial value |
| `SchemaError` | A declared state, message or reply type cannot be serialized | Fix the field type; the error identifies its path |
| `Refused` | A node or client cannot join because its cluster name, limits or protocol differ | The connection settings must agree |

`tell` does not wait for receiver execution and cannot return its failure. Delivery failures are reported as `MessageDropped` events to the observer. Local serialization can still reject a value before sending it.

A body failure restarts the run from saved state after backoff. The message it failed on is not a durable queue item replayed until success. Messages still in its mailbox are subsequent work. An exception after sending a reply can restart the body even though the caller already received that reply.

## External effects and retries

Quorum confirmation establishes the state write's replication outcome. It does not atomically commit an external database change, file write, email or service request performed by the actor.

If a body performs an effect before saving its checkpoint, recovery or a caller retry can repeat that effect. If it saves first, a crash between the checkpoint and the effect can omit the work. An idempotency key accepted by the external resource can make a repeated request safe.

For a non-idempotent state operation such as `Add(1)`, a timeout can follow a committed increment. Sending it again as a new message can increment twice. A protocol needing retryable commands should carry a command identifier and record the corresponding result in actor state; it must also account for the state's durability and read limits.

Reply after the state write when the reply is intended to acknowledge that transition. Sending a reply before saving allows the caller to observe success before confirmation.

## Observers

`ActorSystem(observer=...)` and `Client(observer=...)` accept a callable of `Event`. The default observer logs events through the `casty` logger.

| Event | What it describes |
|---|---|
| `MemberChanged` | A local membership status transition |
| `ActivationStarted`, `ActivationEnded`, `ActivationFailed` | An activation's lifecycle |
| `WriteFailed` | An unsuccessful state operation |
| `HandoffStarted`, `HandoffEnded` | A state-transfer lifecycle |
| `ConnectionLost` | A transport connection ending |
| `MessageDropped` | A message or unhandled piped operation that could not be delivered |

The runtime schedules observer calls on the Python event loop after the step that emitted the event. Transport threads enqueue the report through the asyncio bridge and never call the observer inside the interpreter themselves.

An observer may call back into the system. It should stay quick because it runs on the actor event loop. An observer exception is passed to the loop's exception handler while the node continues running.

Observers with a `wants` method can select event kinds at startup. Unwanted reports are filtered before constructing Python event values. For example, the default logging observer can omit activation debug events when debug logging is disabled.

## Inspection

`system.stats()` reads local counters. `system.activations()` describes current local activations, and `await system.placement(actor, key)` reports placement according to that node's view. `system.members` is also a local membership view.

These values describe what one node currently knows; they are not a consensus snapshot of the cluster. Correlate membership, handoff and write events when investigating an ownership transition. The [observer API](observer.md) defines the available counters and records.

## Guarantees

- Delivery is at most once. Messages from one node to one key arrive in order while that key's owner remains the same.
- An activation handles one inbox message at a time. Ownership changes can temporarily leave overlapping activations on different nodes.
- Confirmed writes retain state within the failure tolerance of their write level. Majority quorums require more than half of the replica set; `one` can lose confirmed writes during ownership changes.
- A failed operation may have committed, and an unconfirmed write may be recovered later.
- Reads are not linearizable. A paused old owner can answer from state that a new owner has already advanced.
- State is held in memory unless the type is durable. Losing every replica loses non-durable state.
- A confirmed `durable="write"` write survives a full cluster restart while its store retains the record. Delayed durability has an unsaved window.
- External effects can repeat or be omitted around checkpoints; they do not participate in the state replication transaction.
- Schedules and lease expiry use wall clocks. Ownership changes can miss or repeat scheduled delivery, and expiry does not stop a running lease holder.
- Pinned actors have one copy and are unavailable while their named node is down.

See the [failure example](https://github.com/gabfssilva/casty/tree/main/examples/04-failures), [replication](05-replication.md), [handoff](06-handoff.md) and [lease fencing](09-collections.md#leases-and-fencing) for the mechanisms behind these limits.

Implementation: [outcomes](https://github.com/gabfssilva/casty/blob/main/crates/casty-core/src/outcome.rs), [failure and cancellation handling](https://github.com/gabfssilva/casty/blob/main/crates/casty-py/src/node/activation.rs), [observer delivery](https://github.com/gabfssilva/casty/blob/main/crates/casty-py/src/node/observe.rs) and [event records](https://github.com/gabfssilva/casty/blob/main/src/casty/observer.py).
