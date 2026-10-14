# Clustering

A clustered `ActorSystem` hosts actors and replicas. A `Client` routes messages without hosting either. Both expose refs with the same message and reply types as a local system.

## Joining a cluster

Use the actor definitions from the [introduction](01-intro.md), import `Cluster` and replace `main` with:

```python
from casty import Cluster


async def main() -> None:
    cluster = Cluster(
        bind="0.0.0.0:7400",
        advertise="10.0.0.5:7400",
        seeds=("10.0.0.4:7400",),
    )
    async with ActorSystem(cluster=cluster) as system:
        views = system.ref(counter, "page-views")
        print(await views.ask(Add(1)))
```

`bind` is the listening address. `advertise` is the address peers use to reach this node. `seeds` are initial contacts from which it learns membership. Seeds do not become coordinators: once members know one another, the cluster maintains its own membership view.

Every node needs the same actor definitions at module scope. The [local cluster example](https://github.com/gabfssilva/casty/tree/main/examples/06-distribution) starts several nodes on one machine; the [Docker example](https://github.com/gabfssilva/casty/tree/main/examples/09-docker-cluster) runs separate processes.

## Membership and failure detection

Members exchange records through gossip and periodically exchange their tables through anti-entropy. A record describes a node identity, membership incarnation number, status and known actor types.

| Status | Meaning |
|---|---|
| `alive` | The member is considered responsive |
| `leaving` | The member announced an orderly departure |
| `suspect` | A failure is suspected but not yet declared |
| `dead` | The failure detector considers the member unavailable |
| `left` | The member was removed from the cluster |

Two incarnation values serve different purposes. `NodeId.incarnation` identifies one process lifetime and changes on restart. The membership record's incarnation number orders announcements about that same process. Merge precedence uses the newer membership incarnation, then status; a `left` record overrides any incarnation for the removed identity.

Failure detection uses elapsed silence and the configured heartbeat and failure intervals. Its observations are local: membership is not a consensus-backed inventory, and two nodes can temporarily have different views.

A node that stops seeing a majority of members alive eventually steps down from ordinary actor ownership. It allows a grace period to avoid repeatedly ending activations when observations briefly cross the majority threshold. The current grace is half `dead_after`, leaving the remainder for step-down before peers declare the node dead.

This ownership check applies separately from a type's replication write level. Selecting `write="one"` changes the state protocol's acknowledgement threshold; it does not remove the node's membership-based ownership check.

## Consistent hashing

Placement hashes `actor/key` with an eight-byte Blake2b digest. The ring contains 128 virtual nodes per physical node, derived from its advertised address and process incarnation. Nodes with the same members derive the same ring.

To choose replicas, the runtime starts at the key's position and walks clockwise until it has the requested number of distinct physical nodes. It skips repeated virtual nodes of a machine already selected:

```text
Ring positions after a key: A₁, A₂, B₁, A₃, C₁
Three physical replicas:   A, B, C
```

The replica count is capped by the ring's member count. The owner is the first replica still present and considered alive or suspect. Ownership can therefore move to a surviving replica without immediately changing which copies belong to the replica set.

A dead member stays in the ring until removed. Shrinking the ring immediately on suspicion would let each side of a partition calculate a smaller quorum and independently acknowledge writes. With replicas A, B and C, losing contact with B and C does not turn A's majority threshold from two acknowledgements into one.

Changes to the member set trigger [handoff](06-handoff.md). Placement keeps the preceding ring for keys whose newly assigned ranges are still being received.

## Clients and pinned actors

A `Client` obtains membership from cluster members and refreshes its view to route messages. It receives replies to its own asks but cannot act as an independently addressable actor. This fits web handlers, command-line programs and batch jobs. See the [client example](https://github.com/gabfssilva/casty/tree/main/examples/07-client).

`@actor(pinned=True)` ties a key to the node named by `system.ref(..., at=member)`. Its encoded key names that node's advertised address. Pinned actors have one copy, bypass ring placement and are not transferred to another node. They are unavailable while their node is down. Without durable storage, a replacement process starts them from their initial state.

The [agent-per-node example](https://github.com/gabfssilva/casty/tree/main/examples/10-agent-per-node) demonstrates this placement.

## Transport

Connections exchange a handshake containing the cluster name, node identity, compression options and size limits. Cluster names and frame, message and window limits must agree. The frame protocol version must also match; a refused join raises `Refused`.

The transport multiplexes control, application messages and replication on separate logical streams. Each stream has flow-control credit measured in uncompressed bytes. Replication has its own stream so transferring state does not consume application-message credit, although both still share the connection's bandwidth.

`Cluster` supports mutual TLS, compression and message limits. `address_map` can translate advertised addresses to dialable addresses for NAT or tunnels. The [cluster API](casty.md#casty.Cluster) specifies these options.

Implementation: [membership table](https://github.com/gabfssilva/casty/blob/main/crates/casty-core/src/membership/table.rs), [ring](https://github.com/gabfssilva/casty/blob/main/crates/casty-core/src/placement.rs), [placement views](https://github.com/gabfssilva/casty/blob/main/crates/casty-node/src/placement/mod.rs), [ownership standing](https://github.com/gabfssilva/casty/blob/main/crates/casty-node/src/node.rs), [handshake](https://github.com/gabfssilva/casty/blob/main/crates/casty-net/src/handshake.rs) and [stream multiplexing](https://github.com/gabfssilva/casty/blob/main/crates/casty-net/src/mux.rs).
