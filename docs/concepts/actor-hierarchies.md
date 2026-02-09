# Actor Hierarchies

Actors form a tree. When an actor spawns a child via `ctx.spawn()`, it becomes the parent. This hierarchy is not merely organizational — it is the foundation of fault tolerance. A parent is responsible for the lifecycle of its children: it can stop them, watch them for termination, and define supervision strategies for their failures.

`Behaviors.setup()` provides access to the `ActorContext` at initialization time, which is where children are typically spawned:

```python
import asyncio
from dataclasses import dataclass
from casty import ActorContext, ActorRef, ActorSystem, Behavior, Behaviors, Terminated

@dataclass(frozen=True)
class Task:
    description: str

@dataclass(frozen=True)
class Hire:
    name: str

@dataclass(frozen=True)
class Assign:
    worker: str
    task: str

type ManagerMsg = Hire | Assign | Terminated

def worker(name: str) -> Behavior[Task]:
    async def receive(ctx: ActorContext[Task], msg: Task) -> Behavior[Task]:
        print(f"[{name}] working on: {msg.description}")
        return Behaviors.same()

    return Behaviors.receive(receive)

def manager() -> Behavior[ManagerMsg]:
    async def setup(ctx: ActorContext[ManagerMsg]) -> Behavior[ManagerMsg]:
        workers: dict[str, ActorRef[Task]] = {}

        async def receive(ctx: ActorContext[ManagerMsg], msg: ManagerMsg) -> Behavior[ManagerMsg]:
            match msg:
                case Hire(name):
                    ref = ctx.spawn(worker(name), name)
                    ctx.watch(ref)
                    workers[name] = ref
                    return Behaviors.same()
                case Assign(worker_name, task):
                    if worker_name in workers:
                        workers[worker_name].tell(Task(task))
                    return Behaviors.same()
                case Terminated(ref):
                    print(f"Worker stopped: {ref.address}")
                    return Behaviors.same()

        return Behaviors.receive(receive)

    return Behaviors.setup(setup)

async def main() -> None:
    async with ActorSystem() as system:
        mgr = system.spawn(manager(), "manager")
        mgr.tell(Hire("alice"))
        mgr.tell(Hire("bob"))
        await asyncio.sleep(0.1)

        mgr.tell(Assign("alice", "implement login"))
        mgr.tell(Assign("bob", "write tests"))
        await asyncio.sleep(0.1)

asyncio.run(main())
```

**Death watch** is the mechanism by which an actor observes the termination of another actor. Calling `ctx.watch(ref)` registers interest; when the watched actor stops — whether gracefully or due to failure — the watcher receives a `Terminated` message containing the stopped actor's ref.

---

**Next:** [Supervision](supervision.md)
