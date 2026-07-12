"""Hello world: one node, one actor, one call.

An actor is a plain class: annotated fields are its state, public async
methods are its interface. `node.actor(...)` returns a typed proxy — the
activation happens on first call, on whichever node owns the key.

Run: uv run python examples/01_hello_world.py
"""

import asyncio

import casty


@casty.actor
class Greeter:
    greetings: int = 0

    async def greet(self, who: str) -> str:
        self.greetings += 1
        return f"hello, {who}! (greeting #{self.greetings})"


async def main() -> None:
    async with casty.local() as node:
        greeter = node.actor(Greeter, "front-door")
        print(await greeter.greet("world"))
        print(await greeter.greet("again"))

        same = node.actor(Greeter, "front-door")
        print(await same.greet("same actor"))

if __name__ == "__main__":
    asyncio.run(main())
