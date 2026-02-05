"""Parent-Child — hierarquia de atores com ctx.spawn()."""

import asyncio
from dataclasses import dataclass

from casty import ActorContext, ActorRef, ActorSystem, Behavior, Behaviors


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


type ManagerMsg = Hire | Assign


def worker(name: str) -> Behavior[Task]:
    async def receive(ctx: ActorContext[Task], msg: Task) -> Behavior[Task]:
        print(f"  [{name}] trabalhando em: {msg.description}")
        return Behaviors.same()

    return Behaviors.receive(receive)


def manager() -> Behavior[ManagerMsg]:
    workers: dict[str, ActorRef[Task]] = {}

    async def setup(ctx: ActorContext[ManagerMsg]) -> Behavior[ManagerMsg]:
        async def receive(
            ctx: ActorContext[ManagerMsg], msg: ManagerMsg
        ) -> Behavior[ManagerMsg]:
            match msg:
                case Hire(name):
                    workers[name] = ctx.spawn(worker(name), name)
                    print(f"  [manager] contratou {name}")
                    return Behaviors.same()
                case Assign(worker_name, task):
                    if worker_name in workers:
                        workers[worker_name].tell(Task(task))
                    else:
                        print(f"  [manager] {worker_name} não encontrado")
                    return Behaviors.same()

        return Behaviors.receive(receive)

    return Behaviors.setup(setup)


async def main() -> None:
    async with ActorSystem() as system:
        ref = system.spawn(manager(), "manager")

        ref.tell(Hire("ana"))
        ref.tell(Hire("pedro"))
        await asyncio.sleep(0.1)

        ref.tell(Assign("ana", "implementar login"))
        ref.tell(Assign("pedro", "escrever testes"))
        ref.tell(Assign("ana", "code review"))
        await asyncio.sleep(0.1)


asyncio.run(main())
