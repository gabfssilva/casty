# Handoff

Handoff transfers saved state when membership changes the replica set. It lets a node acquire keys without treating an incomplete copy as recovered state. Actor identity remains the same while ownership and physical copies move.

## A chain of rings

A membership update can add and remove several nodes at once. Placement derives a deterministic sequence of intermediate rings, applying one member per step. Joining members are added before departing members are removed.

For example, replacing A with D in a cluster of A, B and C follows the member sets below:

```text
{A, B, C} -> {A, B, C, D} -> {B, C, D}
```

Applying the departure first would temporarily leave fewer members available for replicas. Adding first keeps the intermediate ring from falling below the member count at both endpoints. Every node derives the same sequence from the same previous and target members, regardless of the order in which it learned the changes.

Each actor type tracks the steps relevant to its configured replica count. A node that becomes a replica for new token ranges must fill them before completing those steps.

## Pulling ranges

The receiving node asks surviving members of the preceding ring for the ranges it gained. It asks all relevant old members, because one range can span keys with different replica sets.

A virtual-node range is not a single physical replica group. In a four-node preceding ring, key X might have replicas A, B and C, while key Y in the same requested range had A, C and D. Transfer thresholds are therefore evaluated per key against that key's previous replicas. Counting a fixed number of replies for the whole range would not establish recovery for both keys.

Copies carry accepted write stamps, promised epochs and serialized pages. Large copies and batches are split to fit message limits. The receiving side assembles complete copies before using them.

Responses identify whether their source is also still receiving the range. An incomplete source cannot establish that all relevant state has arrived and must be asked again when it is ready.

## Routing during transfer

Placement distinguishes the destination on the target ring from the owner used while recovery is pending. Messages can be routed to the new destination, which holds them until the range is ready. For affected keys, ownership still resolves against the preceding ring.

This prevents a new owner from starting a key from its initial value while the old state is in transit. Keys outside the gained ranges do not need to wait for that transfer.

Replication replies from a node still receiving a range do not count toward activation or write quorums. The [replication protocol](05-replication.md) can nevertheless consider a newer state included in such a response, because incomplete readiness does not make the bytes it already received obsolete.

## The recovery sweep

After placement changes, a sweep compares local saved copies, active markers and currently running activations:

| Condition | Action |
|---|---|
| A saved key is marked active, belongs here and has no local activation | Recover and reactivate it |
| A local activation's owner moved elsewhere | End that activation |
| This node no longer belongs to the key's replica set | Hand the copy to its new replicas before dropping it |

The `@active` marker matters for actors that run without incoming messages, such as stream consumers. It records that work was active, allowing the new owner to restart the body without waiting for an external message. An orderly idle deactivation removes that marker while retaining application state.

A handover also tracks which new replicas have received each copy. If the saved stamp changes during handover, acknowledgement of the older copy is insufficient to discard the newer one.

## Departure and failure

An orderly `ActorSystem` exit drains messages currently being processed and initiates departure and handover, within the leave budget. Mailbox entries and Python tasks are not durable state and are not transferred as a persistent work queue.

After a crash, surviving nodes detect the unavailable member and recover its active keys from available replicas. Locals and work since the last confirmed state write are lost. If every copy disappears, a durable key can recover from its store; an in-memory key cannot.

Pinned keys have one copy on their named node and do not participate in range handoff. Without a durable store, another process at that address begins from the type's initial state because it cannot recover an in-memory copy from a peer.

Handoff depends on local membership views and timing. Temporary overlapping activations and stale reads remain possible; the [guarantees](10-failures.md#guarantees) state those limits.

See the [distribution example](https://github.com/gabfssilva/casty/tree/main/examples/06-distribution) and [handoff tests](https://github.com/gabfssilva/casty/blob/main/tests/test_handoff.py). Implementation: [ring chains](https://github.com/gabfssilva/casty/blob/main/crates/casty-core/src/placement.rs), [range readiness](https://github.com/gabfssilva/casty/blob/main/crates/casty-core/src/handoff/ranges.rs), [range transfer](https://github.com/gabfssilva/casty/blob/main/crates/casty-node/src/handoff/service.rs) and [sweep decisions](https://github.com/gabfssilva/casty/blob/main/crates/casty-core/src/handoff/sweep.rs).
