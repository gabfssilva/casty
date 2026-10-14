# Replication

Each key has an owner that coordinates activation and state writes, and a configured set of nodes that keep copies. Replication operates on serialized state pages. It does not persist the mailbox, Python tasks or local variables.

## Epochs and write stamps

An owner epoch consists of a round and node identity. Epochs are ordered by round, then process incarnation. A write stamp adds a version to that epoch. Versions increase as the owner attempts writes.

A replica retains its promised epoch, accepted write stamp and state pages. Once it promises a later epoch, it rejects writes from an older one. An activation lease additionally prevents an older activation on the same node from submitting writes after a newer activation replaced it.

These checks protect state writes. They do not stop an already running Python function from performing an external side effect; applications must account for that in their [failure handling](10-failures.md#external-effects-and-retries).

## Activation and recovery

The owner must recover state before it starts the actor body:

1. Choose an epoch in a round above any it knows and send `Prepare` to the replicas.
2. Gather promises, accepted write stamps and available state pages.
3. Select the latest accepted state. For a durable key, also load the store and compare its record.
4. Fetch missing pages when a response contains only their sizes.
5. Write the selected state in the new epoch with the `@active` marker.
6. Start the body after the write is confirmed and any required durable save completes.

If no replica or store has state, an activation with an initial value creates it. Without saved state or an initial value, it cannot start. If a store has a newer round, the owner prepares again above that round before attempting to save.

The owner initially waits for the requested number of replicas known to be available. This can exceed the minimum read quorum. At the end of that wait it may decide with fewer responses, provided they meet the read threshold. Reading the available new replicas matters when ownership and replica sets are changing.

A replica still receiving its range may supply a newer copy, but its response does not count toward the read quorum. It may not yet have received the state needed to establish that recovery is complete.

## Quorums

For a replica set of size `R`, the write level determines the minimum activation read quorum `P` and write quorum `W`:

| Write level | `P` | `W` | Effect |
|---|---|---|---|
| `one` | 1 | 1 | A recovery read can miss a confirmed write held by another replica |
| `majority` | `R // 2 + 1` | `R // 2 + 1` | Read and write quorums intersect within the replica set |
| `all` | 1 | `R` | Every replica must confirm a write; any remaining copy can recover it |

The default is three replicas and `majority`, capped by the cluster size. For a three-replica key, majority writes need two acknowledgements. They retain confirmed state after one replica fails; two unavailable replicas prevent further confirmation.

Majority and all satisfy `W + P > R` within one replica set. The handoff protocol preserves the state needed when that set changes. `one` lacks this intersection for sets larger than one and can lose confirmed writes after an ownership change.

Quorum size uses the configured replica set, including unavailable members still on the ring. It is not recalculated from whichever replicas happen to answer. The node's [membership-based ownership check](04-clustering.md#membership-and-failure-detection) is an additional condition for ordinary actors.

## Saving a value

A write constructs a new stamp and sends the changed pages to its replicas. Each replica stages the update, verifies its base and owner epoch, then acknowledges the accepted write. The owner counts acknowledgements only from replicas whose range transfer is complete.

Once enough replicas confirm, the owner records the write as confirmed. A durable write may then wait for the external store. Only after those requirements complete does the Python activation publish the new `ctx.state.value` and resolve the caller's write future.

A timeout can occur after replicas accepted the write but before the caller received confirmation. For example, A and B may accept a majority write while the caller loses its answer. Recovery can later select that write. A failed operation therefore does not prove that its state change was absent.

## Page deltas and message limits

A delta identifies its accepted base, changed pages and dropped page names. A replica whose accepted stamp differs from that base responds with `NeedFull`, causing the owner to send the complete state.

State larger than one message is divided into numbered parts. A page can span several parts. Replicas append them in order and expose the new state only after the final part; an incomplete or out-of-order sequence is not published as an accepted write.

`Limits.message` bounds serialized messages, replies and initial state carried by a ref. A saved state value can be larger because replication splits it across messages. A field name so large that a part cannot contain any of its data is still refused with `MessageTooLarge`.

## Deletion and tombstones

Deleting state writes a tombstone containing `@deleted`. It takes the place of the previous value and fences older copies in the same way as a later state write.

A replica retains the tombstone until other replicas confirm they hold nothing older. A replica still receiving a range cannot supply that confirmation: an older copy may still arrive. Tombstones move with ordinary copies during handoff.

A later activation that recovers the deletion starts from its initial state, rather than restoring a pre-deletion value. [State](03-state.md) describes the local behavior of `state.delete`.

## Reads and ownership changes

Ordinary actor reads use the activation's confirmed local value; they do not consult a quorum for each message. The state returned by `ctx.state.value` is therefore not a linearizable cluster read.

Consider a process paused long enough for peers to replace its ownership. The new owner can confirm a later value while the paused process retains an earlier one. When the old process resumes, timing of step-down and queued work can allow an answer from that stale value. Epoch fencing prevents subsequent old-epoch writes from being accepted, but does not turn the cached read into a consensus operation.

Applications needing a cross-key transaction or a linearizable read must account for these limits. The actor's sequential mailbox and quorum-confirmed writes do not establish those guarantees.

See the [replication example](https://github.com/gabfssilva/casty/tree/main/examples/05-replication). Implementation: [owner protocol](https://github.com/gabfssilva/casty/blob/main/crates/casty-core/src/replication/owner.rs), [replica protocol](https://github.com/gabfssilva/casty/blob/main/crates/casty-core/src/replication/replica.rs), [wire messages](https://github.com/gabfssilva/casty/blob/main/crates/casty-core/src/replication/messages.rs) and [state parts](https://github.com/gabfssilva/casty/blob/main/crates/casty-core/src/replication/parts.rs).
