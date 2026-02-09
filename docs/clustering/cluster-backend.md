# Casty as a Cluster Backend

Most Python libraries that need distributed coordination depend on external services. Celery requires Redis or RabbitMQ. Distributed lock managers need etcd or ZooKeeper. Service meshes depend on Consul. Each external dependency adds operational complexity — deployment, monitoring, version compatibility, network configuration.

Casty provides the same cluster capabilities as an embeddable Python library with zero external dependencies. No broker to deploy, no configuration server to manage, no serialization protocol to learn. A library author can add `casty` to their dependencies and get cluster membership, leader election, failure detection, work distribution, and state replication out of the box. An application developer can add clustering to an existing system without introducing new infrastructure.

The following sections illustrate these capabilities through the lens of building a distributed task queue — a scenario where Celery would typically require Redis and a separate worker process.

## Cluster Formation

`ClusteredActorSystem` establishes a cluster through seed nodes. Once started, nodes discover each other automatically via the gossip protocol. The event stream publishes membership events as nodes join and leave:

```python
from casty import MemberUp, MemberLeft
from casty.sharding import ClusteredActorSystem

async with ClusteredActorSystem(
    name="task-queue",
    host="10.0.0.1",
    port=25520,
    seed_nodes=[("10.0.0.2", 25520), ("10.0.0.3", 25520)],
    required_quorum=3,  # startup blocks until 3 nodes are UP
) as system:
    system.event_stream.subscribe(
        MemberUp, lambda e: log.info(f"Worker joined: {e.member.address}")
    )
    system.event_stream.subscribe(
        MemberLeft, lambda e: log.info(f"Worker left: {e.member.address}")
    )
```

Every node in the cluster runs the same code. There is no distinction between "broker" and "worker" — every node is both. The cluster forms automatically as nodes start and discover each other through the seed list. The `required_quorum` parameter blocks startup inside `__aenter__` until the specified number of nodes have status `up` — no more guessing with `asyncio.sleep()`. When omitted, startup completes immediately after the local node initializes.

## Work Distribution

`Behaviors.sharded()` partitions work across nodes by entity ID. For a task queue, each task type or queue name can be an entity, and the cluster handles routing transparently:

```python
from casty import Behaviors, ShardEnvelope

def task_worker(entity_id: str) -> Behavior[TaskMsg]:
    def idle() -> Behavior[TaskMsg]:
        async def receive(ctx, msg):
            match msg:
                case SubmitTask(payload, reply_to):
                    result = await execute(payload)
                    reply_to.tell(TaskResult(result))
                    return idle()
        return Behaviors.receive(receive)
    return idle()

tasks = system.spawn(Behaviors.sharded(task_worker, num_shards=256), "tasks")

# Submit from any node — routing is transparent
tasks.tell(ShardEnvelope("queue:emails", SubmitTask(send_welcome, reply_to)))
tasks.tell(ShardEnvelope("queue:reports", SubmitTask(generate_pdf, reply_to)))
```

The entity ID determines which node processes the task. Tasks with the same queue name always land on the same node, providing natural ordering guarantees. Tasks with different queue names are distributed across the cluster.

## Failure Handling

When a node becomes unreachable, the phi accrual failure detector identifies it and the coordinator reallocates its shards to surviving nodes. No manual intervention, no external health check service:

```python
from casty import UnreachableMember

system.event_stream.subscribe(
    UnreachableMember,
    lambda e: log.warning(f"Node unreachable: {e.member.address}, shards will be reallocated"),
)
```

If the task worker uses event sourcing, reallocated entities replay their journal on the new node and resume exactly where they left off. Tasks in progress at the time of failure are recovered automatically — the new primary replays persisted events and the worker continues from its last known state.

## Distributed Coordination

`system.barrier(name, n)` blocks until `n` nodes reach the same named barrier, then releases all simultaneously:

```python
await system.barrier("work-complete", num_nodes)
# all nodes have reached this point before any proceeds
await system.barrier("shutdown", num_nodes)
```

Barriers, locks, and semaphores are also available through the `Distributed` facade (see [Distributed Data Structures](distributed-data-structures.md)). All coordination primitives are backed by sharded entities — no external coordination service.

## State Without External Storage

Event sourcing combined with replication provides durable distributed state without a database. The primary persists events to its journal and pushes them to replicas on other nodes. If the primary fails, a replica is promoted with its full event history intact:

```python
from casty import Behaviors
from casty.journal import InMemoryJournal
from casty.replication import ReplicationConfig

journal = InMemoryJournal()  # replace with a database-backed journal in production

tasks = system.spawn(
    Behaviors.sharded(
        lambda eid: Behaviors.event_sourced(
            entity_id=eid,
            journal=journal,
            initial_state=QueueState(pending=(), completed=()),
            on_event=apply_event,
            on_command=handle_command,
        ),
        num_shards=256,
        replication=ReplicationConfig(replicas=1, min_acks=1),
    ),
    "tasks",
)
```

No Redis. No RabbitMQ. No ZooKeeper. The cluster stack is the library — membership, leader election, failure detection, sharding, replication, distributed data structures, locks, semaphores, and barriers, all in pure Python, all in a single `pip install`.

---

**Next:** [Configuration](../configuration/index.md)
