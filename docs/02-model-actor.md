# The actor model

As [Hewitt](https://arxiv.org/pdf/1008.1459) describes, an actor is a primitive of concurrent computation that communicates with other actors through asynchronous messages. Upon receiving a message, it may send messages to actors whose addresses it knows, create actors and determine how it handles its next message.

casty implements virtual actors. Their identities persist independently of the in-memory activations that execute their behavior. The application obtains references by type and key; the runtime manages the corresponding activations.

## Type, identity and reference

An `async def` decorated with `@actor` defines an actor type. `Context[S, M]` declares its state type `S` and message type `M`. An entity is the pair `(actor type, key)`, and a `Ref[M]` is a value through which messages reach it.

For the counter from the [introduction](01-intro.md), `system.ref(counter, "ana")` and `system.ref(counter, "bia")` address two independent counters. Obtaining the same type and key again addresses the same entity, even if its previous activation has ended or its owner has changed.

By default, a type is named by the `module:qualname` of its body. A node that encounters an unfamiliar type imports that name. Definitions therefore belong at module scope, and cluster members need the same actor definitions. Moving or renaming the function changes the type's identity and makes state under the previous name unreachable through the new type.

A ref can travel in a message or in actor state. It names a logical target rather than holding a pointer to the actor body. [Messaging](07-messaging.md#references-and-reply-targets) describes the entity and reply targets it can carry.

## Activation lifecycle

Obtaining a ref through `system.ref` creates and starts the key. A later message can reactivate an idle key. An activation must acquire its saved state before its body begins; the [replication protocol](05-replication.md#activation-and-recovery) performs that acquisition in a cluster.

| Transition | Runtime behavior | What survives |
|---|---|---|
| First activation | Recover state, or initialize a key that has none, then start the body | The chosen initial or recovered state |
| Idle deactivation | End `ctx.inbox` after `idle_after`, let the body return and release the activation | Saved state |
| Body failure | Report the failure and restart the body after backoff | Last saved state |
| Cancelled ask in progress | Cancel the body at its current await and restart it for subsequent work | Writes and effects already completed |
| Ownership change | End the old activation and recover state at the new owner | State available from replicas or a durable store |

The default idle interval is one minute. Local variables belong to a particular run of the body. They are recreated on restart and are not transferred to another node. Values that must survive those transitions belong in [`ctx.state`](03-state.md).

An activation has an internal lease identifier. Every state write carries it, allowing the node to refuse a write from an activation that a newer activation has replaced. Across nodes, [owner epochs](05-replication.md#epochs-and-write-stamps) provide the corresponding write fencing.

## One mailbox reader

An activation reads one message at a time. Reading the next item from `ctx.inbox` marks the end of handling the previous message. An `await` between those reads delays the next message for that activation; messages accumulate in its mailbox while other actors continue running.

Concurrency comes from independent keys and work whose results return as messages. `ctx.to_self` and `ctx.ask` allow the body to continue reading while other asynchronous work is pending. A body may also run a `TaskGroup` or consume an asynchronous stream. [Messaging](07-messaging.md#asynchronous-work) explains how to preserve the mailbox's role in state changes.

During a cluster ownership change, two nodes can briefly have activations of the same logical key. This local mailbox rule does not establish a global order across those activations. Replication fences writes, and the [read guarantees](05-replication.md#reads-and-ownership-changes) describe the remaining timing constraint.

## Changing behavior

`await ctx.become(other, state)` changes the behavior associated with the key. The replacement type must accept the same messages. Its state can have a different type, allowing each state of a state machine to be represented by a separate actor function.

The entity's original identity and its refs stay the same. The runtime records the replacement behavior in the reserved `@behavior` state page and preserves schedules. The replacement begins when the previous run reaches its next inbox read; code after `become` can still execute, but the previous context no longer owns state writes.

See the runnable [state machine example](https://github.com/gabfssilva/casty/tree/main/examples/01-state-machine).

## Execution and threading

In a clustered system, a Tokio task owns membership, routing, replication and handoff. Python actor operations reach that task through a request channel. Results cross back through a callback queue watched by asyncio:

```text
Python event loop                         Rust runtime
Ref / Context operations  -------------> node request channel
Future / mailbox delivery <------------- asyncio callback queue
```

Transport threads never attach to the Python interpreter. They put callbacks on a queue and write a wakeup byte to a connected socket pair. The event loop drains the queue in order. This avoids making protocol progress wait for the Python GIL. Actor execution can still be delayed if its event loop is blocked.

Without a cluster, a system has one local state copy and no transport threads. Clustered systems and clients can share threads through `Runtime(threads=n)`. Each still owns its node, listener and connections.

Implementation: [activation](https://github.com/gabfssilva/casty/blob/main/crates/casty-py/src/node/activation.rs), [run tracking](https://github.com/gabfssilva/casty/blob/main/crates/casty-py/src/node/runs.rs), [asyncio bridge](https://github.com/gabfssilva/casty/blob/main/crates/casty-py/src/node/inbox.rs) and [node task](https://github.com/gabfssilva/casty/blob/main/crates/casty-node/src/node.rs).
