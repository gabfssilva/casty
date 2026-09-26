"""The runner's side of a chaos run: the nodes on the slots of the site, the network between them, and the clients the
traffic goes out from.

A slot is an address the run keeps for its whole length; the node on it comes and goes, and what it says goes to the
output directory, one file per life of the slot. The network is kept here, whole, and each node is given its own part of
it whenever it changes: which addresses it cannot reach and which links are slow, as chaos on its pod, and how far off
its clock is, told to the node.
"""

from __future__ import annotations

import asyncio
from collections import Counter as Tally
from collections.abc import AsyncGenerator, Iterable, Sequence
from contextlib import AsyncExitStack, asynccontextmanager
from datetime import timedelta
from itertools import count
from pathlib import Path

from casty import Client, NodeId
from reliability.node import Launch, Network, Timing, Version, View
from reliability.site import Kubernetes, Pod, Stuck
from reliability.support import eventually


class Fleet:
    """The nodes of a run: a pod on each slot that is up, the network between them, and clients outside it.

    The clients reach every node directly: a partition is between nodes, and the traffic keeps reaching both sides of
    it, which is what makes the minority side refuse to write.
    """

    @classmethod
    @asynccontextmanager
    async def running(cls, site: Kubernetes, /, *, size: int, timing: Timing, output: Path) -> AsyncGenerator[Fleet]:
        fleet = cls(site, size=size, timing=timing, output=output)
        try:
            yield fleet
        finally:
            await fleet._close()

    def __init__(self, site: Kubernetes, /, *, size: int, timing: Timing, output: Path) -> None:
        self.addresses = site.addresses
        self.timing = timing
        self.started = 0
        """Nodes started in all, restarts included."""
        self.peak = 0
        """The most nodes that were up at once."""
        self._site = site
        self._size = size
        self._output = output
        self._machines: dict[int, Pod] = {}
        self._versions: dict[int, Version] = {}
        self._lives: Tally[int] = Tally()
        self._sides: dict[int, int] = {}
        self._isolated: set[int] = set()
        self._slow: dict[int, float] = {}
        self._skew: dict[int, float] = {}
        self._questions = count(1)
        self._clients = AsyncExitStack()

    @property
    def up(self) -> tuple[int, ...]:
        """The slots with a node running, in order."""
        return tuple(sorted(self._machines))

    def reachable(self) -> tuple[str, ...]:
        """The addresses the nodes up advertise."""
        return tuple(self.addresses[slot] for slot in self.up)

    async def boot(self, slots: Sequence[int], within: timedelta, /) -> None:
        """Start the nodes of `slots` together, seeded with the first of them, and wait until they see each other.

        Each runs the code it ran last on its slot, `reliability.actors` on a slot never started.
        """

        async def one(slot: int) -> None:
            async with starting:
                await self._start(slot, self._versions.get(slot, "app"), self._seeds(slot, slots))

        starting = asyncio.Semaphore(_STARTING)
        for failure in await asyncio.gather(*(one(slot) for slot in slots), return_exceptions=True):
            if isinstance(failure, BaseException):
                raise failure
        await self._push()
        await self.converged(within)

    async def clients(self, number: int, /) -> tuple[Client, ...]:
        """Start `number` clients, seeded with the first nodes up, closed with the fleet."""
        seeds = tuple(self.addresses[slot] for slot in self.up[:_SEEDS])
        started: list[Client] = []
        for _ in range(number):
            client = Client(seeds=seeds, ask_timeout=self.timing.ask_timeout, sync_every=self.timing.sync_every)
            async with asyncio.timeout(_JOIN):
                started.append(await self._clients.enter_async_context(client))
        return tuple(started)

    async def start(self, slot: int, version: Version, /) -> None:
        await self._start(slot, version, self._seeds(slot, self.up))
        await self._push()

    async def crash(self, slot: int, /) -> None:
        await self._machines.pop(slot).kill()

    async def leave(self, slot: int, /) -> None:
        await self._machines.pop(slot).leave(self.timing.leave_timeout.total_seconds() + _GRACE)

    async def restart(self, slot: int, /) -> None:
        await self.crash(slot)
        await self.start(slot, self._versions[slot])

    async def halt(self) -> tuple[int, ...]:
        """Kill the node on every slot at once, and answer the slots that were up."""
        slots = self.up
        machines = [self._machines.pop(slot) for slot in slots]
        await asyncio.gather(*(machine.kill() for machine in machines))
        return slots

    async def partition(self, groups: Iterable[Iterable[int]], /) -> None:
        self._sides = {slot: side for side, group in enumerate(groups) for slot in group}
        await self._push()

    async def isolate(self, slot: int, /) -> None:
        self._isolated.add(slot)
        await self._push()

    async def slow(self, slot: int, delay: float, /) -> None:
        self._slow[slot] = delay
        await self._push()

    async def skew(self, slot: int, offset: float, /) -> None:
        self._skew[slot] = offset
        await self._push()

    async def heal(self) -> None:
        """End every partition, isolation and slow link. Clocks stay as they are: they belong to the machines."""
        self._sides.clear()
        self._isolated.clear()
        self._slow.clear()
        await self._push()

    async def converged(self, within: timedelta, /) -> float:
        """Wait until every node up sees exactly the nodes up, all alive, and nothing else, and answer how long that
        took.

        Nothing else includes the dead: a member that crashed stays in the ring until it is removed, and only then does
        the ring replace its replicas, which the next fault must wait for.
        """
        loop = asyncio.get_running_loop()
        began = loop.time()

        async def every_node_sees_exactly_the_others() -> None:
            # Each question costs every node a turn of its loop, so they are asked a few times a second, not a hundred.
            await asyncio.sleep(_POLL)
            self.check()
            slots = self.up
            views = await asyncio.gather(*(self._members(slot) for slot in slots))
            everyone = frozenset((view.me, "alive") for view in views)
            for slot, view in zip(slots, views, strict=True):
                assert view.table == everyone, f"slot {slot} {_difference(view.table, everyone)}"

        try:
            await eventually(every_node_sees_exactly_the_others, within)
        except (AssertionError, TimeoutError) as failure:
            raise Stuck(f"the cluster did not converge within {within.total_seconds():.0f}s: {failure}") from None
        return loop.time() - began

    def check(self) -> None:
        """Raise `Stuck` if a node the runner did not end has gone away by itself."""
        for slot in self.up:
            if (machine := self._machines[slot]).ended:
                raise Stuck(f"the node on slot {slot} exited by itself; see {machine.log}")

    async def _members(self, slot: int, /) -> View:
        return View.decoded(await self._machines[slot].ask({"members": next(self._questions)}, _ASKING))

    async def _start(self, slot: int, version: Version, seeds: tuple[str, ...]) -> None:
        if slot in self._machines:
            raise ValueError(f"slot {slot} is already up")
        self._lives[slot] += 1
        log = self._output / f"slot-{slot:03d}-{self._lives[slot]}.log"
        launch = Launch(self.addresses[slot], seeds, self._size, self._site.store, version)
        machine = await self._site.node(slot, launch, log)
        try:
            await machine.ready(_JOIN)
        except BaseException:
            await machine.kill()
            raise
        self._machines[slot] = machine
        self._versions[slot] = version
        self.started += 1
        self.peak = max(self.peak, len(self._machines))

    async def _push(self) -> None:
        """Give each node its part of the network."""

        async def shape(slot: int) -> None:
            network = self._network(slot)
            await self._machines[slot].ask({"network": network.encoded()}, _ASKING)
            await self._site.link(slot, network)

        await asyncio.gather(*(shape(slot) for slot in self.up))

    def _network(self, slot: int) -> Network:
        others = [other for other in self.up if other != slot]
        blocked = frozenset(self.addresses[other] for other in others if self._cut(slot, other))
        slowest = {other: max(self._slow.get(slot, 0.0), self._slow.get(other, 0.0)) for other in others}
        delays = {self.addresses[other]: delay for other, delay in slowest.items() if delay > 0}
        return Network(blocked, delays, self._skew.get(slot, 0.0))

    def _cut(self, one: int, other: int) -> bool:
        isolated = one in self._isolated or other in self._isolated
        apart = one in self._sides and other in self._sides and self._sides[one] != self._sides[other]
        return isolated or apart

    def _seeds(self, slot: int, among: Iterable[int]) -> tuple[str, ...]:
        """The first few nodes of `among`, as an operator would configure them, without `slot` itself."""
        return tuple(self.addresses[other] for other in sorted(among)[:_SEEDS] if other != slot)

    async def _close(self) -> None:
        await self._clients.aclose()
        machines = list(self._machines.values())
        self._machines.clear()
        await asyncio.gather(*(machine.kill() for machine in machines))


def _difference(seen: frozenset[tuple[NodeId, str]], everyone: frozenset[tuple[NodeId, str]]) -> str:
    extra = sorted(f"{node.address} {status}" for node, status in seen - everyone)
    missing = sorted(f"{node.address}" for node, _ in everyone - seen)
    return f"also sees {', '.join(extra) or 'nobody else'}, and misses {', '.join(missing) or 'nobody'}"


_SEEDS = 3
_STARTING = 16
"""How many nodes start at once, which bounds the load of a large boot."""
_JOIN = 60.0
"""Seconds a node or a client has to join."""
_ASKING = 5.0
"""Seconds a node has to answer on its control port."""
_POLL = 0.25
"""Seconds between two rounds of questions while the cluster converges."""
_GRACE = 15.0
"""Seconds past its `leave_timeout` a leaving node has to be gone."""
