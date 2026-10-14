# Collections

`Collections` provides named, replicated data structures over an `ActorSystem` or a `Client`. Their operations use actors internally, so the same placement and state replication mechanisms apply. Collection state is not durable.

## Basic use

```python
import asyncio

from casty import ActorSystem, Collections
from casty.collections import MISSING


async def main() -> None:
    async with ActorSystem() as system:
        collections = Collections(system)
        visits = collections.counter("visits")
        await visits.add(5)
        print(await visits.get())

        prices = collections.dict("prices", key=str, value=int)
        await prices.put("book", 30)
        print(await prices.get("book"))
        print(await prices.get("pen") is MISSING)


asyncio.run(main())
```

It prints `5`, `30` and `True`. `MISSING` identifies absence and is distinct from a stored `None`.

## Factories

| Collection | Factory | Operations |
|---|---|---|
| Counter | `counter(name, stripes=1)` | `add`, `get`, `reset` |
| Register | `register(name, value=T)` | `get`, `set`, `compare_and_set`, `get_and_set` |
| Dict | `dict(name, key=K, value=V, index_shards=16)` | `put`, `get`, `contains`, `remove`, `scan`, `items`, `size`, `clear` |
| Set | `set(name, value=T, shards=16)` | `add`, `remove`, `contains`, `scan`, `items`, `size`, `clear`, set operations |
| MultiMap | `multimap(name, key=K, value=V, shards=16)` | `put`, `get`, `contains`, `remove`, `scan`, `size`, `clear` |
| Queue | `queue(name, value=T)` | `offer`, `poll`, `peek`, `drain`, `size`, `clear` |
| Semaphore | `semaphore(name, capacity=n)` | `try_acquire`, `acquire`, `available` |
| Lock | `lock(name, ttl=30.0, timeout=None)` | `try_lock`, `acquire`, `locked`, `async with` |
| Barrier | `barrier(name, parties=n)` | `wait`, `waiting` |

All factories accept `replicas`. Data collections accept a write level; semaphores, locks and barriers use majority writes. Keep one `Collections` instance per system to reuse its bindings and configuration checks. The [API reference](collections.md) lists complete signatures.

## Configuration and native actors

A binding identifies the collection kind, name, replicas, write level, shard count and value schema signatures. Before its first operation, it compares that configuration against a metadata register. An empty register is initialized with compare-and-set; an incompatible existing value raises `ConfigurationError`.

Actor keys include the collection kind and length-prefixed name, followed by a shard or entry identifier. This makes internal keys unambiguous even when application names contain separators. Their placement on cluster nodes is still determined by the actor ring.

Native behaviors in `casty-py` process collection messages as state-page transitions. They use the same activation, lease and replication path as Python bodies, while the facade handles typed values, shard selection and multi-actor operations.

Data mutations acknowledge saved state and are not automatically retried after an uncertain failure. A timeout may follow a committed update, so a caller retry can repeat a non-idempotent operation such as incrementing a counter.

## Counters and registers

A counter can use several stripes, each represented by an actor key. `add` chooses stripes in a local round-robin sequence. `get` sums their values, and `reset` visits each stripe. Those aggregate operations do not observe or modify all stripes atomically.

A register uses one actor key. `compare_and_set` compares encoded values and commits a replacement within one actor message. That single-key operation does not turn a group of registers into a distributed transaction, and its reads retain the actor system's [read limits](05-replication.md#reads-and-ownership-changes).

## Shards and segments

Sets, dict indexes and multimaps divide their indexes into shards. Each shard has a directory actor and segment actors. A key is hashed to a shard, then to a segment within it.

The segments grow through linear hashing. At the current 512-key threshold, the directory splits the next segment in order and moves part of its hash space into a new segment. It counts the split only after completion. A segment refuses keys outside its current hash range, allowing a caller with old directory information to refresh and retry placement.

A write therefore rewrites one segment rather than an entire unbounded shard. This index partitioning is distinct from physical-node placement: every directory, segment and entry is itself an actor key that the cluster can place and replicate.

## Dict entries and interrupted operations

A dict keeps values in entry actors and keys in the segmented index. Replacing an existing value only updates its entry. Adding a new key lists it before saving its value; removal clears the value before removing the listing.

An interrupted operation can leave an index listing without a value. Traversal checks the entry and cleans stale listings. Listing generations prevent an old removal arriving late from deleting a newer listing for the same key.

`scan` visits segments incrementally and fetches the listed entries in bounded batches. It is not a snapshot: concurrent changes can alter what later segments contain. `items`, `size` and `clear` likewise do not form atomic operations over all keys.

## Leases and fencing

Semaphores grant permits through leases with a TTL, lease identifier and increasing fencing token. A lock uses a semaphore with one permit. The lease can be renewed explicitly with `await lease.renew(ttl)`; no automatic renewal is performed.

Expiry follows node wall clocks and does not stop code that still holds the lease object. If holder A pauses, its lease can expire and holder B can obtain a later token. When A resumes, the protected resource must reject A's older token. Holding a Python lock context alone cannot enforce that check on an external database or service.

Releasing a lease sends a message without waiting for an answer. A lost release leaves the permit occupied until expiry. A lock context releases on exit but does not extend the lease while its body runs.

Semaphore acquisition and barrier arrival can wait indefinitely. Bound them with `asyncio.timeout`. Their wait protocols reuse identifiers to retain queue position or barrier arrival across request timeouts; this does not imply automatic retry of ordinary data mutations.

Implementation: [Python bindings and aggregate operations](https://github.com/gabfssilva/casty/blob/main/src/casty/collections.py), [native indexes](https://github.com/gabfssilva/casty/blob/main/crates/casty-py/src/collections/table.rs), [dict entries](https://github.com/gabfssilva/casty/blob/main/crates/casty-py/src/collections/entry.rs), [semaphore](https://github.com/gabfssilva/casty/blob/main/crates/casty-py/src/collections/semaphore.rs) and [barrier](https://github.com/gabfssilva/casty/blob/main/crates/casty-py/src/collections/barrier.rs).
