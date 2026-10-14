# casty

The chapters cover both the programming model and its implementation. Read them in order for an introduction, or use the topic links to inspect a particular mechanism. All pages live directly in `docs/`.

## Chapters

| Chapter | Contents |
|---|---|
| [01 · Introduction](01-intro.md) | Installation, a first program and implementation layers |
| [02 · The actor model](02-model-actor.md) | Identity, activation, behavior changes and execution |
| [03 · State](03-state.md) | Schemas, state pages, checkpoints and durable records |
| [04 · Clustering](04-clustering.md) | Membership, consistent hashing, clients and transport |
| [05 · Replication](05-replication.md) | Recovery, epochs, quorums, page deltas and tombstones |
| [06 · Handoff](06-handoff.md) | Ring transitions, range transfer and recovery sweeps |
| [07 · Messaging](07-messaging.md) | Typed messages, replies, deadlines and asynchronous work |
| [08 · Schedules](08-schedulers.md) | Stored timers, failover, clock behavior and checkpoints |
| [09 · Collections](09-collections.md) | Native actors, shards, indexes, leases and fencing |
| [10 · Failures and guarantees](10-failures.md) | Errors, retry outcomes, observers and consistency limits |
| [11 · Development](11-development.md) | Build commands, implementation entry points and verification |

## API reference

The reference is generated from the docstrings in the typed contract and Python modules, without importing the compiled extension.

| Module | Contents |
|---|---|
| [`casty`](casty.md) | Actor types, bodies, systems and clients, cluster settings, stores and errors |
| [`casty.collections`](collections.md) | Replicated data structures and synchronization primitives |
| [`casty.observer`](observer.md) | Events, observers, counters and activation records |
| [`casty.stores`](stores.md) | SQL storage and its public contract |
