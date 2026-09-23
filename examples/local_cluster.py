"""Several nodes of one cluster in this process, for the examples that need more than one.

Each node is a whole `ActorSystem` on its own TCP port of 127.0.0.1, and the nodes would behave the same on as many
machines. They send a heartbeat five times a second instead of once, so that the others see them sooner.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator, Callable, Sequence
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass
from datetime import timedelta
from typing import TypedDict, Unpack

from casty import ActorSystem, Cluster, Store


class Detection(TypedDict, total=False):
    """How soon a silent node is `suspect`, `dead` and removed: the fields of `Cluster` of the same names."""

    suspect_after: timedelta
    dead_after: timedelta
    remove_after: timedelta | None


class Nodes:
    """The running nodes of a cluster that `nodes` started."""

    def __init__(self, group: asyncio.TaskGroup, node: Callable[[int], ActorSystem], /) -> None:
        self._group = group
        self._node = node
        self._running: list[_Node] = []

    @property
    def systems(self) -> tuple[ActorSystem, ...]:
        """The system of each node running, in the order they started."""
        return tuple(node.system for node in self._running)

    async def start(self, *ports: int) -> None:
        """Start a node on each of `ports`, and return once every node running sees every other.

        The nodes start together, as the machines of a deployment would: a node with seeds waits for one to answer.
        """
        for port in ports:
            system, stop = self._node(port), asyncio.Event()
            self._running.append(_Node(system, stop, self._group.create_task(_host(system, stop))))
        await self._formed()

    async def stop(self, *systems: ActorSystem) -> None:
        """Stop `systems` in an orderly way, which hands their keys over, and return once the rest see them gone."""
        leaving = [node for node in self._running if node.system in systems]
        self._running = [node for node in self._running if node.system not in systems]
        for node in leaving:
            node.stop.set()
        await asyncio.gather(*(node.task for node in leaving))
        await self._formed()

    def kill(self, system: ActorSystem, /) -> None:
        """Crash `system`: it goes without a word, and the others learn it from failure detection."""
        (node,) = (node for node in self._running if node.system is system)
        self._running.remove(node)
        node.task.cancel()

    async def _formed(self) -> None:
        while True:
            # `members` raises until the `async with` of the system is entered.
            with suppress(RuntimeError):
                if all(len(system.members) == len(self._running) for system in self.systems):
                    return
            await asyncio.sleep(0.1)


@asynccontextmanager
async def nodes(
    ports: Sequence[int], /, *, leave_timeout: timedelta, store: Store | None = None, **detection: Unpack[Detection]
) -> AsyncGenerator[Nodes]:
    """Start a node on each of `ports`, and yield them once every node sees every other.

    The ports are the seeds of the cluster, also for a node started later. `leave_timeout` is how long a node that stops
    waits for the others to take its keys, and every node is given `store`. Leaving the block stops every node still
    running, all at once.
    """
    seeds = tuple(f"127.0.0.1:{port}" for port in ports)

    def node(port: int, /) -> ActorSystem:
        cluster = Cluster(bind=f"127.0.0.1:{port}", seeds=seeds, heartbeat=timedelta(milliseconds=200), **detection)
        return ActorSystem(cluster=cluster, leave_timeout=leave_timeout, store=store)

    async with asyncio.TaskGroup() as group:
        running = Nodes(group, node)
        await running.start(*ports)
        yield running
        await running.stop(*running.systems)


@dataclass(frozen=True)
class _Node:
    system: ActorSystem
    stop: asyncio.Event
    task: asyncio.Task[None]


async def _host(system: ActorSystem, stop: asyncio.Event, /) -> None:
    """Run `system` until `stop` is set, which is an orderly exit. Cancelling this instead is a crash."""
    async with system:
        await stop.wait()
