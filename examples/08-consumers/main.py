"""Workers that nobody has to restart: one consumer per partition, kept running by the cluster.

Run with `uv run main.py`. A consumer takes no message at all (`Context[Cursor]`): it reads its partition from the
offset after the last one it saved. A key that is running carries a mark in its replicated state, so when its node
dies another node brings it back by itself, from the saved offset. Nothing is sent to it, and nothing watches it.

The record is processed before the offset is saved, so a crash in between repeats that record on the next node:
at-least-once, never a gap.
"""

import asyncio
from collections import Counter
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import timedelta

from casty import ActorSystem, Cluster, Context, actor

PORTS = (7431, 7432, 7433)
SEEDS = tuple(f"127.0.0.1:{port}" for port in PORTS)
PARTITIONS = tuple(f"orders-{index}" for index in range(6))
LAST = 60


class Progress:
    """`(partition, offset, node)` of every record handled: a stand-in for the effect a real consumer has on the world.

    It is an object in memory only because the three nodes of this example share one process.
    """

    def __init__(self) -> None:
        self.log: list[tuple[str, int, str | None]] = []
        self._changed = asyncio.Event()

    def record(self, partition: str, offset: int, node: str | None, /) -> None:
        self.log.append((partition, offset, node))
        self._changed.set()

    async def reached(self, offset: int, /) -> None:
        """Return once every partition got to `offset`."""
        while True:
            done = {partition: -1 for partition in PARTITIONS}
            for partition, at, _ in self.log:
                done[partition] = max(done[partition], at)
            if all(at >= offset for at in done.values()):
                return
            self._changed.clear()
            await self._changed.wait()


PROGRESS = Progress()


@dataclass(frozen=True)
class Cursor:
    offset: int = -1


async def records(start: int, /) -> AsyncIterator[int]:
    """The partition from `start` on: what a Kafka or a queue client would hand over."""
    for offset in range(start, LAST + 1):
        await asyncio.sleep(0.05)
        yield offset


@actor(initial=Cursor())
async def consumer(ctx: Context[Cursor]) -> None:
    async for offset in ctx.merge(records(ctx.state.value.offset + 1)):
        PROGRESS.record(ctx.key, offset, ctx.system.node.address)
        await ctx.state.set(Cursor(offset))


def node(port: int, /) -> ActorSystem:
    cluster = Cluster(
        bind=f"127.0.0.1:{port}",
        seeds=SEEDS,
        heartbeat=timedelta(milliseconds=200),
        suspect_after=timedelta(seconds=1),
        dead_after=timedelta(seconds=1),
    )
    return ActorSystem(cluster=cluster, leave_timeout=timedelta(seconds=1))


async def host(system: ActorSystem, stop: asyncio.Event, /) -> None:
    async with system:
        await stop.wait()


async def formed(systems: list[ActorSystem], /) -> None:
    while True:
        try:
            if all(len(system.members) == len(systems) for system in systems):
                return
        except RuntimeError:
            pass
        await asyncio.sleep(0.1)


async def main() -> None:
    stop = asyncio.Event()
    systems = [node(port) for port in PORTS]
    async with asyncio.TaskGroup() as nodes:
        tasks = [nodes.create_task(host(system, stop)) for system in systems]
        await formed(systems)

        # Obtaining the ref creates each key and activates it, wherever the ring places it. Nothing else is ever sent.
        for partition in PARTITIONS:
            systems[0].ref(consumer, partition)
        await PROGRESS.reached(15)

        busiest, _ = Counter(address for _, _, address in PROGRESS.log).most_common(1)[0]
        victim = next(index for index, system in enumerate(systems) if system.node.address == busiest)
        lost = {partition for partition, _, address in PROGRESS.log if address == busiest}
        tasks[victim].cancel()
        print(f"killed {busiest}, which was running {sorted(lost)}")

        await PROGRESS.reached(LAST)
        stop.set()

    for partition in PARTITIONS:
        offsets = [offset for held, offset, _ in PROGRESS.log if held == partition]
        nodes_of = list(dict.fromkeys(address for held, _, address in PROGRESS.log if held == partition))
        repeated = len(offsets) - len(set(offsets))
        complete = set(offsets) == set(range(LAST + 1))
        print(f"{partition}: ran on {nodes_of}, every offset processed: {complete}, repeated: {repeated}")


asyncio.run(main())
