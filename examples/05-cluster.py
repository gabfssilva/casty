import asyncio

from casty import actor, Mailbox, message, System
from casty.state import State
from casty.cluster import DevelopmentCluster


@message
class Inc:
    pass


@message
class Get:
    pass


type CounterMsg = Inc | Get


@actor(clustered=True)
async def counter(state: State[int], *, mailbox: Mailbox[CounterMsg], system: System):
    async for msg, ctx in mailbox:
        match msg:
            case Inc():
                state.value += 1
                print(f"[{system.node_id}] inc: {state.value}")
            case Get():
                print(f"[{system.node_id}] get: {state.value}")
                await ctx.reply(state.value)


async def main():
    async with asyncio.timeout(10):
        async with DevelopmentCluster(nodes=10) as cluster:
            ref = await cluster.actor(counter(State(0)), name="counter")

            iterations = 10

            for _ in range(iterations):
                await ref.send(Inc())

            await asyncio.sleep(0.5)

            result = await ref.ask(Get())
            print(f"Counter value: {result}. Expected: {iterations}")

if __name__ == "__main__":
    asyncio.run(main())
