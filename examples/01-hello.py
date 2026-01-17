from dataclasses import dataclass

import asyncio

from casty import Actor, ActorSystem, Context


@dataclass
class Inc: n: int = 1

@dataclass
class Get: pass

class Counter(Actor[Inc | Get]):
    def __init__(self):
        self.count = 0

    async def receive(self, msg, ctx: Context):
        match msg:
            case Inc(n):
                self.count += n
            case Get():
                await ctx.reply(self.count)


async def main():
    async with ActorSystem() as system:
        counter = await system.spawn(Counter)

        for i in range(1, 101):
            await (counter >> Inc(i))

        result: int = await (counter << Get())

        print(f'Result: {result}')


if __name__ == '__main__':
    asyncio.run(main())
