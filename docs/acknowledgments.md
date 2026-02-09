# Acknowledgments

Casty didn't emerge from a vacuum. Every feature builds on research and engineering that, without it, wouldn't be possible to implement. This page maps each feature to the ideas and papers that shaped it.

## Actors and Message Passing

Actors are the primitive unit of computation in Casty: they receive messages, make local decisions, create other actors, and send messages. No shared state, no locks. This model was introduced by Hewitt, Bishop, and Steiger in 1973.

> Hewitt, C., Bishop, P., & Steiger, R. (1973). *A Universal Modular ACTOR Formalism for Artificial Intelligence*. IJCAI.

## Behaviors as Values

In Casty, behaviors are frozen dataclasses produced by factory functions (`receive`, `setup`, `same`, `stopped`), not classes with a `receive` method to override. State transitions happen by returning a new behavior with new closed-over values. This functional, compositional approach to actor definition comes directly from Akka Typed.

> [Akka Typed](https://doc.akka.io/libraries/akka-core/current/typed/index.html). Lightbend, Inc.

## Supervision

When an actor fails, its supervisor decides what happens next: restart, stop, or escalate. Actors don't defend themselves with try/except around every operation. They fail fast and let the supervision hierarchy handle recovery. This "let it crash" philosophy was pioneered by Erlang/OTP and articulated in Armstrong's thesis on building reliable systems through isolation and message passing.

> Armstrong, J. (2003). *Making reliable distributed systems in the presence of software errors*. PhD thesis, Royal Institute of Technology, Stockholm.

## Event Sourcing

Event-sourced actors persist the sequence of events that produced their state, not the state itself. On recovery, events are replayed to reconstruct the actor. Snapshots periodically checkpoint the state to bound replay time. This pattern, popularized by Greg Young and described by Martin Fowler, provides a complete audit trail and the ability to rebuild state from any point in history.

## Cluster Membership via Gossip

Cluster membership is disseminated via gossip: each node periodically picks random peers and exchanges its view of the cluster. This epidemic approach guarantees eventual consistency without a central coordinator or consensus protocol. The protocol in Casty follows the epidemic dissemination model introduced by Demers et al.

> Demers, A., Greene, D., Hauser, C., et al. (1987). *Epidemic Algorithms for Replicated Database Maintenance*. ACM PODC.

## Failure Detection

Casty uses the phi accrual failure detector for heartbeat monitoring. Unlike traditional binary detectors (alive or dead), this one outputs a continuous suspicion level. Each consumer chooses its own threshold, making the system adaptable to varying network conditions without retuning.

> Hayashibara, N., Défago, X., Yared, R., & Katayama, T. (2004). *The Phi Accrual Failure Detector*. IEEE Symposium on Reliable Distributed Systems (SRDS).

## Cluster State Convergence

The cluster state that nodes gossip to each other uses CRDT merge semantics. When two nodes have divergent views, they merge deterministically: member status advances along a lattice (joining &rarr; up &rarr; leaving &rarr; down &rarr; removed), and vector clocks are merged component-wise. Convergence is guaranteed regardless of message ordering or duplication.

> Shapiro, M., Preguiça, N., Baquero, C., & Zawirski, M. (2011). *Conflict-free Replicated Data Types*. Symposium on Self-Stabilizing Systems (SSS).

State versions are tracked with vector clocks to establish causal ordering across nodes. Each node increments its own component on state changes, and during gossip the clocks merge to determine which state is newer or whether a CRDT join is needed.

> Lamport, L. (1978). *Time, Clocks, and the Ordering of Events in a Distributed System*. Communications of the ACM.
>
> Fidge, C. J. (1988). *Timestamps in Message-Passing Systems That Preserve the Partial Ordering*. Australian Computer Science Conference.
>
> Mattern, F. (1989). *Virtual Time and Global States of Distributed Systems*. Parallel and Distributed Algorithms.

## Cluster Sharding

Sharding distributes actors across nodes using a coordinator, region, and proxy architecture. The coordinator assigns shards to nodes using allocation strategies; regions manage the local entities for their assigned shards; proxies route messages transparently to the correct region, wherever it lives. This three-layer architecture follows Akka Cluster Sharding's design.

> [Akka Cluster Sharding](https://doc.akka.io/libraries/akka-core/current/typed/cluster-sharding.html). Lightbend, Inc.
