# Casty as a Cluster Backend

Most Python libraries that need distributed coordination depend on external services. Celery requires Redis or RabbitMQ. Distributed lock managers need etcd or ZooKeeper. Service meshes depend on Consul. Each external dependency adds operational complexity — deployment, monitoring, version compatibility, network configuration.

Casty provides the same cluster capabilities as an embeddable Python library with zero external dependencies. No broker to deploy, no configuration server to manage, no serialization protocol to learn. A library author can add `casty` to their dependencies and get cluster membership, leader election, failure detection, work distribution, and state replication out of the box. An application developer can add clustering to an existing system without introducing new infrastructure.

The following sections illustrate these capabilities through the lens of building a distributed task queue — a scenario where Celery would typically require Redis and a separate worker process.

## Cluster Formation

`ClusteredActorSystem` establishes a cluster through seed nodes. Once started, nodes discover each other automatically through the topology actor — a single actor that owns cluster state, runs gossip protocol and phi accrual failure detection, and pushes `TopologySnapshot` updates to all subscribers. The event stream publishes membership events as nodes join and leave:

```python
async with ClusteredActorSystem(
    name="task-queue",
    host="10.0.0.1",
    port=25520,
    node_id="worker-1",
    seed_nodes=[("10.0.0.2", 25520), ("10.0.0.3", 25520)],
    required_quorum=3,  # startup blocks until 3 nodes are UP
) as system:
    def membership_logger() -> Behavior[MemberUp | MemberLeft]:
        async def receive(ctx, msg):
            match msg:
                case MemberUp(member=m):
                    log.info(f"Worker joined: {m.address}")
                case MemberLeft(member=m):
                    log.info(f"Worker left: {m.address}")
            return Behaviors.same()
        return Behaviors.receive(receive)

    monitor = system.spawn(membership_logger(), "membership-monitor")
    system.event_stream.tell(EventStreamSubscribe(event_type=MemberUp, handler=monitor))
    system.event_stream.tell(EventStreamSubscribe(event_type=MemberLeft, handler=monitor))
```

Every node in the cluster runs the same code. There is no distinction between "broker" and "worker" — every node is both. The cluster forms automatically as nodes start and discover each other through the seed list. The `required_quorum` parameter blocks startup inside `__aenter__` until the specified number of nodes have status `up` — no more guessing with `asyncio.sleep()`. When omitted, startup completes immediately after the local node initializes.

## Work Distribution

`Behaviors.sharded()` partitions work across nodes by entity ID. For a task queue, each task type or queue name can be an entity, and the cluster handles routing transparently:

```python
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
def unreachable_logger() -> Behavior[UnreachableMember]:
    async def receive(ctx, msg):
        match msg:
            case UnreachableMember(member=m):
                log.warning(f"Node unreachable: {m.address}, shards will be reallocated")
        return Behaviors.same()
    return Behaviors.receive(receive)

alert = system.spawn(unreachable_logger(), "unreachable-alert")
system.event_stream.tell(EventStreamSubscribe(event_type=UnreachableMember, handler=alert))
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

## External Clients

Not every process needs to be a full cluster member. `ClusterClient` connects to the cluster as an external observer — it receives topology updates (membership + shard allocations) and routes `ShardEnvelope` messages directly to the owning node with zero hops, no proxy overhead, no cluster participation:

```python
async with ClusterClient(
    cluster_host="10.0.0.1",
    cluster_port=25520,
    system_name="task-queue",
) as client:
    ref = await client.shard_ref("tasks", num_shards=256)
    ref.tell(ShardEnvelope("queue:emails", SubmitTask(send_welcome, reply_to)))
```

The client subscribes to `TopologySnapshot` from the cluster's topology actor, caches shard-to-node mappings locally, and includes a TCP circuit breaker that blacklists failed nodes and retries on topology changes. Ideal for web servers, API gateways, or any process that sends work to the cluster without hosting entities itself.

---

**Next:** [Configuration](../configuration/index.md)
