import asyncio

from casty import actor, Mailbox, state, message, System
from casty.cluster import DevelopmentCluster


@message
class Inc:
    pass


@message
class Get:
    pass


type CounterMsg = Inc | Get


@actor(clustered=True)
async def counter(mailbox: Mailbox[CounterMsg], system: System):
    count = state("count", 0)

    async for msg, ctx in mailbox:
        match msg:
            case Inc():
                count.value += 1
                print(f"[{system.node_id}] inc: {count.value}")
            case Get():
                print(f"[{system.node_id}] get: {count.value}")
                await ctx.reply(count.value)


async def main():
    # NÃO MUDE O TIMEOUT!!!!
    async with asyncio.timeout(10):
        async with DevelopmentCluster(nodes=5, debug=False) as cluster:
            ref = await cluster.actor(counter(), name="counter")

            iterations = 10

            for _ in range(iterations):
                await ref.send(Inc())

            await asyncio.sleep(1)

            result = await ref.ask(Get())
            print(f"Counter value: {result}. Expected: {iterations}")

if __name__ == "__main__":
    asyncio.run(main())
