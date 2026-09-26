"""State that outlives the machine it was on. Run with `uv run main.py`.

Three nodes in one process, so the example fits in one file; each is a whole `ActorSystem` on its own TCP port, and
they would behave the same in three machines. The journal keeps three copies of each key and a `state.set` returns
once a majority has it. The node running the key is killed without warning, and the key comes back on another node
with everything that was confirmed.
"""

import asyncio
from dataclasses import dataclass
from datetime import timedelta

from local_cluster import nodes

from casty import ActorSystem, Askable, Context, NodeId, Unavailable, actor

PORTS = (7401, 7402, 7403)


@dataclass(frozen=True)
class Append(Askable[int]):
    line: str


@dataclass(frozen=True)
class Page:
    lines: tuple[str, ...]
    node: NodeId


@dataclass(frozen=True)
class Read(Askable[Page]):
    pass


type JournalMsg = Append | Read


# `write` is per type: "one" answers fastest and can lose the last writes with the machine, "majority" survives the
# loss of a minority of the copies, "all" refuses writes while any copy is away.
@actor(initial=tuple[str, ...](), replicas=3, write="majority")
async def journal(ctx: Context[tuple[str, ...], JournalMsg]) -> None:
    async for msg in ctx.inbox:
        match msg:
            case Append(line, reply_to=reply_to):
                await ctx.state.set((*ctx.state.value, line))
                reply_to.tell(len(ctx.state.value))
            case Read(reply_to=reply_to):
                reply_to.tell(Page(ctx.state.value, ctx.system.node))


async def read(system: ActorSystem, key: str, /) -> Page:
    """Ask until the key answers: between the crash and its detection, the owner is a node that is not there."""
    while True:
        try:
            async with asyncio.timeout(1):
                return await system.ref(journal, key).ask(Read())
        except (Unavailable, TimeoutError):
            await asyncio.sleep(0.2)


async def main() -> None:
    async with nodes(
        PORTS,
        # Failure detection in about two seconds instead of the ten of the defaults, so the example does not drag.
        suspect_after=timedelta(seconds=1),
        dead_after=timedelta(seconds=1),
        remove_after=timedelta(seconds=2),
        # A node that leaves waits up to `leave_timeout` for the others to take its keys. The example ends by stopping
        # the whole cluster at once, when nobody is left to take them, so the wait is kept short.
        leave_timeout=timedelta(seconds=1),
    ) as cluster:
        first = cluster.systems[0]
        for line in ("monday", "tuesday", "wednesday"):
            await first.ref(journal, "diary").ask(Append(line))
        before = await read(first, "diary")
        print(f"{before.lines} on {before.node.address}")

        cluster.kill(next(system for system in cluster.systems if system.node == before.node))
        print(f"killed {before.node.address}")
        survivor = cluster.systems[0]
        after = await read(survivor, "diary")
        print(f"{after.lines} on {after.node.address}")
        print("appended:", await survivor.ref(journal, "diary").ask(Append("thursday")), "lines")


asyncio.run(main())
