"""The runner's side of a chaos run: node processes on slots of this machine, the network between them, and the
clients the traffic goes out from.

A slot is an address the run keeps for its whole length; the process on it comes and goes. Each process is a
`tests.chaos.node`, and its stderr goes to the output directory, one file per life of the slot. The network is kept
here, whole, and each node is sent its own part of it whenever it changes: which addresses it cannot reach, which
links are slow, and how far off its clock is. Every node is given the same SQLite store, in the output directory too,
which is what the durable types outlive an outage in.
"""

from __future__ import annotations

import asyncio
import json
import os
import random
import socket
import sys
import tempfile
from collections import Counter as Tally
from collections.abc import AsyncGenerator, Iterable, Sequence
from contextlib import AsyncExitStack, asynccontextmanager, suppress
from datetime import timedelta
from itertools import count
from pathlib import Path

from casty import Client, NodeId
from tests.chaos import records
from tests.chaos.node import Launch, Network, Version, View, known
from tests.cluster import Timing
from tests.support import eventually

ROOT = Path(__file__).resolve().parents[2]


class Stuck(Exception):
    """The cluster did not get where it had to in time, or a node went away by itself: a failure of liveness."""


class Machine:
    """The process on one slot, from its start to its end."""

    @classmethod
    async def launch(cls, slot: int, launch: Launch, log: Path, /) -> Machine:
        sink = _sink(log)
        try:
            process = await asyncio.create_subprocess_exec(
                sys.executable,
                "-m",
                "tests.chaos.node",
                launch.encoded(),
                cwd=ROOT,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=sink,
                limit=_LINE,
            )
        finally:
            os.close(sink)
        return cls(slot, process, log)

    def __init__(self, slot: int, process: asyncio.subprocess.Process, log: Path, /) -> None:
        self.slot = slot
        self.log = log
        self._process = process
        self._ready: asyncio.Future[NodeId] = asyncio.get_running_loop().create_future()
        self._answers: dict[int, asyncio.Future[View]] = {}
        self._questions = count(1)
        self._reading = asyncio.create_task(self._read())

    @property
    def ended(self) -> bool:
        """Whether the process is gone."""
        return self._reading.done()

    async def ready(self, within: float, /) -> NodeId:
        """Wait until the node is in the cluster."""
        try:
            async with asyncio.timeout(within):
                return await self._ready
        except TimeoutError:
            raise Stuck(f"slot {self.slot} did not join within {within:.0f}s; see {self.log}") from None

    async def members(self, within: float, /) -> View:
        """The member table of the node, as it answers now."""
        question = next(self._questions)
        answer: asyncio.Future[View] = asyncio.get_running_loop().create_future()
        self._answers[question] = answer
        try:
            await self.send({"members": question})
            async with asyncio.timeout(within):
                return await answer
        finally:
            self._answers.pop(question, None)

    async def send(self, command: dict[str, object], /) -> None:
        stdin = self._process.stdin
        if stdin is None or self.ended:
            raise Stuck(f"the node on slot {self.slot} is gone; see {self.log}")
        try:
            stdin.write((json.dumps(command) + "\n").encode())
            await stdin.drain()
        except OSError as broken:
            raise Stuck(f"the node on slot {self.slot} is gone; see {self.log}") from broken

    async def kill(self) -> None:
        if self._process.returncode is None:
            with suppress(ProcessLookupError):
                self._process.kill()
        await self._process.wait()
        await self._reading

    async def leave(self, within: float, /) -> None:
        """Ask the node to leave, and wait for its process to end."""
        await self.send({"leave": None})
        try:
            async with asyncio.timeout(within):
                code = await self._process.wait()
        except TimeoutError:
            await self.kill()
            raise Stuck(f"slot {self.slot} did not finish leaving within {within:.0f}s; see {self.log}") from None
        await self._reading
        if code != 0:
            raise Stuck(f"slot {self.slot} exited with {code} while leaving; see {self.log}")

    async def _read(self) -> None:
        stdout = self._process.stdout
        if stdout is not None:
            while line := await stdout.readline():
                try:
                    said = records.record(json.loads(line))
                except ValueError:
                    continue  # a line of something else the node printed
                if "ready" in said and not self._ready.done():
                    self._ready.set_result(known(said["ready"]))
                elif "members" in said:
                    view = View.decoded(said["members"])
                    answer = self._answers.get(view.question)
                    if answer is not None and not answer.done():
                        answer.set_result(view)
        gone = Stuck(f"the node on slot {self.slot} exited; see {self.log}")
        if not self._ready.done():
            self._ready.set_exception(gone)
        for answer in self._answers.values():
            if not answer.done():
                answer.set_exception(gone)


class Fleet:
    """The machines of a run: a process on each slot that is up, the network between them, and clients outside it.

    The clients reach every node directly, through no proxy: a partition is between nodes, and the traffic keeps
    reaching both sides of it, which is what makes the minority side refuse to write.
    """

    @classmethod
    @asynccontextmanager
    async def running(cls, *, slots: int, size: int, timing: Timing, output: Path) -> AsyncGenerator[Fleet]:
        fleet = cls(slots=slots, size=size, timing=timing, output=output)
        try:
            yield fleet
        finally:
            await fleet._close()

    def __init__(self, *, slots: int, size: int, timing: Timing, output: Path) -> None:
        self.addresses = _addresses(slots)
        self.store = Path(tempfile.mkdtemp(prefix="store-", dir=output)) / "records.db"
        """The SQLite file of the store every node is given, in a directory of its own: a run that reuses `output`
        inherits nothing of the one before."""
        self.timing = timing
        self.started = 0
        """Nodes started in all, restarts included."""
        self.peak = 0
        """The most machines that were up at once."""
        self._size = size
        self._output = output
        self._machines: dict[int, Machine] = {}
        self._versions: dict[int, Version] = {}
        self._lives: Tally[int] = Tally()
        self._sides: dict[int, int] = {}
        self._isolated: set[int] = set()
        self._slow: dict[int, float] = {}
        self._skew: dict[int, float] = {}
        self._clients = AsyncExitStack()

    @property
    def up(self) -> tuple[int, ...]:
        """The slots with a machine running, in order."""
        return tuple(sorted(self._machines))

    def reachable(self) -> tuple[str, ...]:
        """The addresses the machines up advertise."""
        return tuple(self.addresses[slot] for slot in self.up)

    async def boot(self, slots: Sequence[int], within: timedelta, /) -> None:
        """Start the machines of `slots` together, seeded with the first of them, and wait until they see each other.

        Each runs the code it ran last on its slot, `tests.app` on a slot never started.
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
        """Start `number` clients, seeded with the first machines up, closed with the fleet."""
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
        """Kill the process on every slot at once, and answer the slots that were up."""
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
        """Wait until every machine up sees exactly the machines up, all alive, and nothing else, and answer how long
        that took.

        Nothing else includes the dead: a member that crashed stays in the ring until it is removed, and only then does
        the ring replace its replicas, which the next fault must wait for.
        """
        loop = asyncio.get_running_loop()
        began = loop.time()

        async def every_machine_sees_exactly_the_others() -> None:
            # Each question costs every node a turn of its loop, so they are asked a few times a second, not a hundred.
            await asyncio.sleep(_POLL)
            self.check()
            machines = [self._machines[slot] for slot in self.up]
            views = await asyncio.gather(*(machine.members(_ASKING) for machine in machines))
            everyone = frozenset((view.me, "alive") for view in views)
            for machine, view in zip(machines, views, strict=True):
                assert view.table == everyone, f"slot {machine.slot} {_difference(view.table, everyone)}"

        try:
            await eventually(every_machine_sees_exactly_the_others, within)
        except (AssertionError, TimeoutError) as failure:
            raise Stuck(f"the cluster did not converge within {within.total_seconds():.0f}s: {failure}") from None
        return loop.time() - began

    def check(self) -> None:
        """Raise `Stuck` if a machine the runner did not end has gone away by itself."""
        for slot in self.up:
            if (machine := self._machines[slot]).ended:
                raise Stuck(f"the node on slot {slot} exited by itself; see {machine.log}")

    async def _start(self, slot: int, version: Version, seeds: tuple[str, ...]) -> None:
        if slot in self._machines:
            raise ValueError(f"slot {slot} is already up")
        self._lives[slot] += 1
        log = self._output / f"slot-{slot:03d}-{self._lives[slot]}.log"
        launch = Launch(self.addresses[slot], seeds, self._size, str(self.store), version)
        machine = await Machine.launch(slot, launch, log)
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
        """Send each machine its part of the network."""
        await asyncio.gather(
            *(self._machines[slot].send({"network": self._network(slot).encoded()}) for slot in self.up)
        )

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
        """The first few machines of `among`, as an operator would configure them, without `slot` itself."""
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


def _addresses(number: int) -> tuple[str, ...]:
    """Free ports on the loopback, below the ephemeral range where the proxies of the nodes bind theirs."""
    port = random.SystemRandom().randrange(20_000, 30_000)
    found: list[str] = []
    while len(found) < number:
        port += 1
        with socket.socket() as probe:
            try:
                probe.bind(("127.0.0.1", port))
            except OSError:
                continue
        found.append(f"127.0.0.1:{port}")
    return tuple(found)


def _sink(log: Path) -> int:
    return os.open(log, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)


_SEEDS = 3
_STARTING = 16
"""How many machines start at once, which bounds the load of a large boot."""
_JOIN = 60.0
"""Seconds a node or a client has to join."""
_ASKING = 5.0
"""Seconds a node has to answer for its member table."""
_POLL = 0.25
"""Seconds between two rounds of questions while the cluster converges."""
_GRACE = 15.0
"""Seconds past its `leave_timeout` a leaving node has to be gone."""
_LINE = 1 << 20
