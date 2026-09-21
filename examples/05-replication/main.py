"""State that outlives the machine it was on. Run with `uv run main.py`.

Three nodes in one process, so the example fits in one file; each is a whole `ActorSystem` on its own TCP port, and
they would behave the same in three machines. The journal keeps three copies of each key and a `state.set` returns
once a majority has it. The node running the key is killed without warning, and the key comes back on another node
with everything that was confirmed.
"""

import asyncio
from dataclasses import dataclass
from datetime import timedelta

from casty import ActorSystem, Cluster, Context, NodeId, Ref, Unavailable, actor

PORTS = (7401, 7402, 7403)
SEEDS = tuple(f"127.0.0.1:{port}" for port in PORTS)


@dataclass(frozen=True)
class Append:
    reply_to: Ref[int]
    line: str


@dataclass(frozen=True)
class Page:
    lines: tuple[str, ...]
    node: NodeId


@dataclass(frozen=True)
class Read:
    reply_to: Ref[Page]


type JournalMsg = Append | Read


# `write` is per type: "one" answers fastest and can lose the last writes with the machine, "majority" survives the
# loss of a minority of the copies, "all" refuses writes while any copy is away.
@actor(initial=tuple[str, ...](), replicas=3, write="majority")
async def journal(ctx: Context[tuple[str, ...], JournalMsg]) -> None:
    async for msg in ctx.inbox:
        match msg:
            case Append(reply_to, line):
                await ctx.state.set((*ctx.state.value, line))
                reply_to.tell(len(ctx.state.value))
            case Read(reply_to):
                reply_to.tell(Page(ctx.state.value, ctx.system.node))


def node(port: int, /) -> ActorSystem:
    cluster = Cluster(
        bind=f"127.0.0.1:{port}",
        seeds=SEEDS,
        # Failure detection in about two seconds instead of the ten of the defaults, so the example does not drag.
        heartbeat=timedelta(milliseconds=200),
        suspect_after=timedelta(seconds=1),
        dead_after=timedelta(seconds=1),
        remove_after=timedelta(seconds=2),
    )
    # A node that leaves waits up to `leave_timeout` for the others to take its keys. The example ends by stopping the
    # whole cluster at once, when nobody is left to take them, so the wait is kept short.
    return ActorSystem(cluster=cluster, leave_timeout=timedelta(seconds=1))


async def host(system: ActorSystem, stop: asyncio.Event, /) -> None:
    """One node. Cancelling this task is a crash; setting `stop` is an orderly exit, which hands its keys over."""
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


async def read(system: ActorSystem, key: str, /) -> Page:
    """Ask until the key answers: between the crash and its detection, the owner is a node that is not there."""
    while True:
        try:
            async with asyncio.timeout(1):
                return await system.ref(journal, key).ask(Read)
        except (Unavailable, TimeoutError):
            await asyncio.sleep(0.2)


async def main() -> None:
    stop = asyncio.Event()
    systems = [node(port) for port in PORTS]
    async with asyncio.TaskGroup() as nodes:
        # Together, as machines of a deployment would: a node with seeds waits for one of them to answer.
        tasks = [nodes.create_task(host(system, stop)) for system in systems]
        await formed(systems)
        running = {system.node: task for system, task in zip(systems, tasks, strict=True)}

        first = systems[0]
        for line in ("monday", "tuesday", "wednesday"):
            await first.ref(journal, "diary").ask(Append, line)
        before = await read(first, "diary")
        print(f"{before.lines} on {before.node.address}")

        running[before.node].cancel()
        print(f"killed {before.node.address}")
        survivor = next(system for system in systems if system.node != before.node)
        after = await read(survivor, "diary")
        print(f"{after.lines} on {after.node.address}")
        print("appended:", await survivor.ref(journal, "diary").ask(Append, "thursday"), "lines")
        stop.set()


asyncio.run(main())
