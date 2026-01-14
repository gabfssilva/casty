from dataclasses import dataclass

import asyncio

from casty import Actor, ActorSystem, Context


@dataclass
class Inc:
    n: int = 1


@dataclass
class Get:
    pass


class Counter(Actor[Inc | Get]):
    def __init__(self):
        self.count = 0

    async def receive(self, msg, ctx: Context):
        match msg:
            case Inc(n):
                self.count += n
            case Get():
                ctx.reply(self.count)


async def main():
    async with ActorSystem() as system:
        counter = await system.spawn(Counter)

        await (counter >> Inc(5))
        await (counter >> Inc(3))

        result: int = await (counter << Get())
        print(f'Result: {result}')
        assert result == 8, f'Expected 8, got {result}'


if __name__ == '__main__':
    asyncio.run(main())
