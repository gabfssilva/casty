"""Keys spread over the nodes, and callers that never need to know where. Run with `uv run main.py`.

Each key lives on one node at a time, chosen by a consistent hash ring over the members. Any node takes a message for
any key and routes it to the owner, so a ref works the same from everywhere. When a node joins, some keys move to it
with their state; when one leaves in an orderly way, it hands its keys over before it goes.
"""

import asyncio
from collections import Counter
from dataclasses import dataclass
from datetime import timedelta
from typing import assert_never

from local_cluster import nodes

from casty import ActorSystem, Askable, Context, NodeId, Unavailable, actor

PORTS = (7411, 7412, 7413, 7414)
KEYS = tuple(f"cart-{index}" for index in range(12))
# Annotated because the state of a cart is a tuple of any number of items, which the empty literal alone is not.
EMPTY_CART: tuple[str, ...] = ()


@dataclass(frozen=True)
class Add(Askable[int]):
    item: str


@dataclass(frozen=True)
class Located:
    items: tuple[str, ...]
    node: NodeId


@dataclass(frozen=True)
class Locate(Askable[Located]):
    pass


type CartMsg = Add | Locate


@actor(initial=EMPTY_CART)
async def cart(ctx: Context[tuple[str, ...], CartMsg]) -> None:
    async for msg in ctx.inbox:
        match msg:
            case Add(item, reply_to=reply_to):
                await ctx.state.set((*ctx.state.value, item))
                reply_to.tell(len(ctx.state.value))
            case Locate(reply_to=reply_to):
                reply_to.tell(Located(ctx.state.value, ctx.system.node))
            case _:
                assert_never(msg)


async def locate(system: ActorSystem, key: str, /) -> Located:
    """Ask until the key answers: while a key changes hands it is unavailable for a moment, never in two places."""
    while True:
        try:
            async with asyncio.timeout(1):
                return await system.ref(cart, key).ask(Locate())
        except (Unavailable, TimeoutError):
            await asyncio.sleep(0.1)


async def report(title: str, through: ActorSystem, /) -> None:
    found = await asyncio.gather(*(locate(through, key) for key in KEYS))
    spread = Counter(located.node.address for located in found)
    intact = all(located.items == ("book", "pen") for located in found)
    print(f"{title}: {dict(sorted(spread.items()))}, every cart intact: {intact}")


async def main() -> None:
    async with nodes(PORTS[:3], leave_timeout=timedelta(seconds=5)) as cluster:
        systems = cluster.systems
        # Each message enters through a different node. None of them is told where the cart is.
        for index, key in enumerate(KEYS):
            await systems[index % 3].ref(cart, key).ask(Add("book"))
            await systems[(index + 1) % 3].ref(cart, key).ask(Add("pen"))
        await report("three nodes", systems[0])

        await cluster.start(PORTS[3])
        await report("a fourth joined", systems[0])

        await cluster.stop(systems[1])
        await report("the second left", systems[0])


asyncio.run(main())
