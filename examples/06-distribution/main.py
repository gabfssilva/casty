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

from casty import ActorSystem, Cluster, Context, NodeId, Ref, Unavailable, actor

SEED = "127.0.0.1:7411"
KEYS = tuple(f"cart-{index}" for index in range(12))
# Annotated because the state of a cart is a tuple of any number of items, which the empty literal alone is not.
EMPTY_CART: tuple[str, ...] = ()


@dataclass(frozen=True)
class Add:
    reply_to: Ref[int]
    item: str


@dataclass(frozen=True)
class Located:
    items: tuple[str, ...]
    node: NodeId


@dataclass(frozen=True)
class Locate:
    reply_to: Ref[Located]


type CartMsg = Add | Locate


@actor(initial=EMPTY_CART)
async def cart(ctx: Context[tuple[str, ...], CartMsg]) -> None:
    async for msg in ctx.inbox:
        match msg:
            case Add(reply_to, item):
                await ctx.state.set((*ctx.state.value, item))
                reply_to.tell(len(ctx.state.value))
            case Locate(reply_to):
                reply_to.tell(Located(ctx.state.value, ctx.system.node))
            case _:
                assert_never(msg)


def node(port: int, /) -> ActorSystem:
    cluster = Cluster(bind=f"127.0.0.1:{port}", seeds=(SEED,), heartbeat=timedelta(milliseconds=200))
    return ActorSystem(cluster=cluster, leave_timeout=timedelta(seconds=5))


async def host(system: ActorSystem, stop: asyncio.Event, /) -> None:
    async with system:
        await stop.wait()


async def formed(systems: list[ActorSystem], /) -> None:
    """Wait until every node sees every other. A node is only usable once its `async with` is entered."""
    while True:
        try:
            if all(len(system.members) == len(systems) for system in systems):
                return
        except RuntimeError:
            pass
        await asyncio.sleep(0.1)


async def locate(system: ActorSystem, key: str, /) -> Located:
    """Ask until the key answers: while a key changes hands it is unavailable for a moment, never in two places."""
    while True:
        try:
            async with asyncio.timeout(1):
                return await system.ref(cart, key).ask(Locate)
        except (Unavailable, TimeoutError):
            await asyncio.sleep(0.1)


async def report(title: str, through: ActorSystem, /) -> None:
    found = await asyncio.gather(*(locate(through, key) for key in KEYS))
    spread = Counter(located.node.address for located in found)
    intact = all(located.items == ("book", "pen") for located in found)
    print(f"{title}: {dict(sorted(spread.items()))}, every cart intact: {intact}")


async def main() -> None:
    stops = [asyncio.Event() for _ in range(4)]
    systems = [node(7411 + index) for index in range(4)]
    async with asyncio.TaskGroup() as nodes:
        for system, stop in zip(systems[:3], stops, strict=False):
            nodes.create_task(host(system, stop))
        await formed(systems[:3])

        # Each message enters through a different node. None of them is told where the cart is.
        for index, key in enumerate(KEYS):
            await systems[index % 3].ref(cart, key).ask(Add, "book")
            await systems[(index + 1) % 3].ref(cart, key).ask(Add, "pen")
        await report("three nodes", systems[0])

        nodes.create_task(host(systems[3], stops[3]))
        await formed(systems)
        await report("a fourth joined", systems[0])

        stops[1].set()
        await formed([systems[0], systems[2], systems[3]])
        await report("the second left", systems[0])

        for stop in stops:
            stop.set()


asyncio.run(main())
