"""Example: Clustered Counter using DevelopmentCluster."""

import asyncio
from dataclasses import dataclass

from casty import Actor, Context
from casty.cluster import DevelopmentCluster


@dataclass
class Increment:
    amount: int


@dataclass
class GetCount:
    pass


class Counter(Actor[Increment | GetCount]):
    def __init__(self):
        self.count = 0

    async def receive(self, msg: Increment | GetCount, ctx: Context):
        match msg:
            case Increment(amount):
                self.count += amount
            case GetCount():
                ctx.reply(self.count)


async def main():
    async with DevelopmentCluster(2) as (system, system2):
        print(f"Node 0: {system.node_id}")
        print(f"Node 1: {system2.node_id}")

        await asyncio.sleep(0.5)

        counter = await system.spawn(Counter, clustered=True, name="counter", replication=2, write_consistency=2)
        print(f"Spawned clustered counter: {counter.actor_id}")

        await counter.send(Increment(10))
        await counter.send(Increment(5))
        await counter.send(Increment(3))

        counter2 = await system2.spawn(Counter, clustered=True, name="counter")
        print(f"Got counter from node-1: {counter2.actor_id}")

        result = await counter2.ask(GetCount())
        print(f"Count: {result}")  # Should print 18


if __name__ == "__main__":
    asyncio.run(main())
