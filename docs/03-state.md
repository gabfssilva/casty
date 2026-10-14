# State

An actor's state is the value declared by `S` in `Context[S, M]`. Only saved state participates in recovery, replication and durable storage. Mutating a local variable does not update it.

## Reading and writing

| Operation | Result |
|---|---|
| `ctx.state.value` | Read the activation's last confirmed state |
| `await ctx.state.set(value)` | Save a replacement value |
| `await ctx.state.update(change)` | Apply a function, save its result and return the new value |
| `await ctx.state.delete()` | Delete the key's state and schedules |

For the [counter](01-intro.md#a-first-program), `initial=0` supplies the initial value and `state.update` saves the sum. A key without saved state uses `initial=` from `system.ref` first, then the default of `@actor`, then `None` if its state type permits it. A key requiring an initial value cannot start without one.

State writes wait for the configured [replication level](05-replication.md#quorums). The activation publishes the new local value only after confirmation and, when required, a durable save. A write that raises does not publish that replacement locally, although a replica may have accepted it and a later activation may recover it.

`update` accepts a function returning a value or an awaitable of one. Computing an asynchronous update holds up the actor's next inbox message. State writes should complete before the body moves on to that message.

After deletion, the running body uses its type's default state when one exists. A type without a default has no local value to read until it saves another one. A future activation with no saved state again needs the ref's initial value, the type's default or an allowed `None`.

## Immutable values and schemas

State, messages and replies are serialized even within one process. The decorator compiles their annotations into schemas and reports an unsupported field with `SchemaError`. Collection type parameters and reply annotations also participate in serialization.

Supported types include primitives, `Literal`, timezone-aware `datetime` and `time`, `date`, `timedelta`, `Decimal`, `UUID`, enums, paths, tuples, frozensets, mappings, frozen dataclasses, unions, type aliases and refs. Integers are signed 64-bit values. Mutable lists, dicts, sets and dataclasses are not supported as state or message fields.

For example, a state containing several identifiers can use a tuple:

```python
from dataclasses import dataclass


@dataclass(frozen=True)
class Cart:
    items: tuple[str, ...] = ()
```

Build a new value and save it when the cart changes. An in-place mutation would bypass replication and cannot represent a confirmed state transition.

`Annotated[T, Opaque(encode, decode)]` allows a custom value to travel as bytes. The supplied functions define that value's representation; casty does not inspect its internal structure. Use explicit state writes with opaque values as well: mutating a wrapped object does not save the change.

## State pages

The core represents state as `Pages`, a map from names to bytes. A dataclass state schema splits its top-level fields into separate pages; other state schemas use the single page `.`. A write compares pages with its accepted base and sends changed pages and deleted names. The [replication chapter](05-replication.md#page-deltas-and-message-limits) describes how replicas apply those deltas.

The runtime also uses reserved pages:

| Page | Meaning |
|---|---|
| `@active` | The key has an activation to recover after ownership changes |
| `@behavior` | The replacement behavior installed by `become` |
| `@schedules` | Named schedules, their due times and serialized messages |
| `@deleted` | A tombstone superseding earlier state |

These pages move with the state. A new owner can therefore restore behavior and schedules along with the application value. An activation lease identifies which local activation is allowed to submit writes; an owner epoch identifies which distributed owner replicas accept.

## Memory and durable storage

By default, state lives in memory on its replicas. Deactivating an actor retains those copies, but losing every replica loses the state. A durable type also stores versioned records outside the actor processes.

Configure `ActorSystem(store=...)` and set the type's `durable` option:

| Setting | When storage is updated | What the caller waits for |
|---|---|---|
| `None` | No external save | Replica confirmation |
| `"write"` | Every confirmed write | Replicas and the store |
| A `timedelta` | The latest confirmed state within that delay; also on release and deletion | Replica confirmation |

A confirmed `durable="write"` write survives a full cluster restart if the store retains its record. Delayed durability permits recent confirmed writes to be lost when every replica disappears before the external save.

Every node of a cluster must reach the same records. `casty.stores.SQL` supports PostgreSQL, MySQL, MariaDB and SQLite. A SQLite file can serve nodes on one machine; nodes on different machines need a store they all reach. See the [durable state example](https://github.com/gabfssilva/casty/tree/main/examples/11-durable-state) and [store API](stores.md).

## Record versions and recovery

A stored record contains a version and state bytes. The version is the owner round, node incarnation and write version, encoded in that order as big-endian bytes. This preserves the ordering of replication write stamps.

The SQL store uses one row of `casty_records` per actor type and key, addressed by a fixed-size digest. A save replaces only an earlier record. A drop removes only a record no later than the deletion it represents. An old operation arriving after a new one cannot overwrite or remove the newer record.

For a durable activation in a cluster, recovery compares the latest replica state with the store record and selects the later write. A store failure leaves that activation unavailable; silently starting from the initial value would discard recoverable state.

Custom Python stores implement asynchronous `load`, `save` and `drop` calls on the event loop. The SQL implementation has its own Rust thread and is called directly from the cluster task. Both implement the versioned [Store contract][casty.Store].

## Schema changes

Dataclasses are encoded by field name. A missing field takes its declared default, and an unknown field is ignored. A missing field without a default raises `SchemaError`. Added fields therefore need defaults when old values must remain readable.

Actor type identity and value schema compatibility are separate concerns. A compatible field addition does not make renaming the actor function safe: a new function name still addresses a different type. Cluster versions must also agree on message types and the wire protocol.

Implementation: [page and record representation](https://github.com/gabfssilva/casty/blob/main/crates/casty-core/src/store.rs), [Python schema compilation](https://github.com/gabfssilva/casty/blob/main/crates/casty-py/src/schema/compile.rs), [state writes](https://github.com/gabfssilva/casty/blob/main/crates/casty-py/src/node/activation.rs), [store bridge](https://github.com/gabfssilva/casty/blob/main/crates/casty-py/src/node/storage.rs) and [SQL records](https://github.com/gabfssilva/casty/blob/main/crates/casty-store/src/lib.rs).
