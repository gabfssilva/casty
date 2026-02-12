# Cluster Singleton

Some actors must exist exactly once in the entire cluster. A cluster-wide lock manager, a coordinator that assigns work, a single pipeline that ingests an external feed — spawning two would cause conflicts or duplicate processing. **Cluster Singleton** guarantees that exactly one instance of an actor runs across all nodes, always on the current leader.

Sharding could technically achieve this with `num_shards=1` and a fixed entity ID, but it brings coordinator/region/proxy overhead and forces the `ShardEnvelope` wrapper on every message. Singleton is the direct solution: one actor, one location, automatic failover, typed ref with no envelope.

```python
@dataclass(frozen=True)
class ProcessJob:
    job_id: str

@dataclass(frozen=True)
class GetStatus:
    reply_to: ActorRef[str]

type CoordinatorMsg = ProcessJob | GetStatus

def job_coordinator() -> Behavior[CoordinatorMsg]:
    def active(pending: tuple[str, ...] = ()) -> Behavior[CoordinatorMsg]:
        async def receive(ctx: ActorContext[CoordinatorMsg], msg: CoordinatorMsg) -> Behavior[CoordinatorMsg]:
            match msg:
                case ProcessJob(job_id):
                    return active((*pending, job_id))
                case GetStatus(reply_to):
                    reply_to.tell(f"{len(pending)} jobs pending")
                    return Behaviors.same()

        return Behaviors.receive(receive)

    return active()

async def main() -> None:
    async with (
        ClusteredActorSystem(
            name="cluster", host="127.0.0.1", port=25520,
            node_id="node-1", seed_nodes=[("127.0.0.1", 25521)],
        ) as node1,
        ClusteredActorSystem(
            name="cluster", host="127.0.0.1", port=25521,
            node_id="node-2", seed_nodes=[("127.0.0.1", 25520)],
        ) as node2,
    ):
        ref1 = node1.spawn(Behaviors.singleton(job_coordinator), "coordinator")
        ref2 = node2.spawn(Behaviors.singleton(job_coordinator), "coordinator")
        await asyncio.sleep(0.5)

        # Send from either node — messages reach the single instance
        ref1.tell(ProcessJob("job-1"))
        ref2.tell(ProcessJob("job-2"))

        status = await node1.ask(
            ref1,
            lambda r: GetStatus(reply_to=r),
            timeout=2.0,
        )
        print(status)  # 2 jobs pending

asyncio.run(main())
```

The ref returned by `spawn()` is `ActorRef[CoordinatorMsg]` — typed, no wrapper. Every node calls `spawn()` with the same name and factory, but only the leader node runs the actual actor. Non-leader nodes forward messages transparently.

## How It Works

Each call to `system.spawn(Behaviors.singleton(factory), name)` creates a **singleton manager** actor on that node. The manager has three modes:

- **Pending.** No role assigned yet. Buffers incoming messages until the cluster actor sends a `SetRole` message with the current leader information.
- **Active.** This node is the leader. The manager spawns the actual singleton actor as a child and forwards all messages to it.
- **Standby.** Another node is the leader. The manager forwards messages to the leader's manager over TCP.

When the cluster detects a leadership change (a node goes down, a new leader is elected), every singleton manager receives a new `SetRole`. The old leader's manager stops its child. The new leader's manager spawns a fresh instance from the factory.

## Failover

The singleton tracks leadership, not individual node health. When the leader node dies:

1. The phi accrual failure detector marks the node as unreachable.
2. The cluster actor sends `DownMember` to the gossip protocol, which transitions the member to `down` status.
3. Leader election (deterministic — lowest address among `up` members) produces a new leader.
4. The new leader's singleton manager receives `SetRole(is_leader=True)` and spawns the singleton.

Messages sent during the transition are buffered by the manager on the sending node and delivered once the new singleton is active.

## Event-Sourced Singleton

If the singleton needs to survive failover with its state intact, wrap it with event sourcing. The journal persists events, and the new instance replays them on startup:

```python
@dataclass(frozen=True)
class Incremented:
    pass

def on_event(state: int, _event: Incremented) -> int:
    return state + 1

async def on_command(
    _ctx: ActorContext[CounterMsg], state: int, msg: CounterMsg,
) -> Behavior[CounterMsg]:
    match msg:
        case Increment():
            return Behaviors.persisted([Incremented()])
        case GetCount(reply_to):
            reply_to.tell(state)
            return Behaviors.same()

ref = system.spawn(
    Behaviors.singleton(lambda: Behaviors.event_sourced(
        entity_id="my-singleton",
        journal=journal,
        initial_state=0,
        on_event=on_event,
        on_command=on_command,
    )),
    "counter",
)
```

Without event sourcing, the singleton restarts from scratch on the new leader — the factory is called with no prior state. Choose based on whether state loss is acceptable during failover.

---

**Next:** [Service Discovery](service-discovery.md)
