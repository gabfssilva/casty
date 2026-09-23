"""The kinds of actor a chaos run writes to, each with the invariants it must keep through the faults.

`WORKLOADS` is what a run picks from by name. A kind is plugged in by adding an entry: a function from the `Stage` to
a `Workload`, which may be one of these with other settings — `Ledgers` over another ledger type is how a type with a
store, a write level or a placement of its own is put under the traffic.

Every check is phrased so that an ambiguous call never breaks it: whatever ended in `Unavailable` or `TimeoutError`
may or may not have been applied, and may still be, at any moment after it began. An outage takes with it everything
the cluster held in memory only, so after one each kind expects only what it confirmed since, but for the ledger whose
store keeps every write (`durable`), which must still hold everything it ever confirmed.
"""

from __future__ import annotations

import asyncio
import math
import random
from collections import Counter as Tally
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import timedelta
from itertools import count, pairwise

from casty import Collections, DefaultedActor, NodeId
from casty.collections import Lease, Missing
from tests.app import Append, Entries, Ledger, LedgerMsg, durable_ledger, ledger
from tests.chaos.actors import Stamp, stamped
from tests.chaos.journal import Operation, Stage, Verdict, Workload, ambiguous, settles
from tests.chaos.schedule import SKEW
from tests.traffic import kept


class Ledgers:
    """Appends of unique entries to the keys of a ledger type: the traffic of `tests.traffic`, carried over.

    Any type that takes `Append` and `Entries` fits, so a ledger with other settings is put under the traffic by
    passing it as `actor`, under a `name` of its own. A type whose store keeps every write (`durable="write"`) is held
    to every entry it confirmed across an outage too; any other only to those whose append began after the last one.
    """

    def __init__(
        self,
        stage: Stage,
        /,
        *,
        actor: DefaultedActor[Ledger, LedgerMsg] = ledger,
        keys: int = 50,
        name: str = "ledger",
    ) -> None:
        self.name = name
        self._stage = stage
        self._actor = actor
        self._keys = tuple(f"{name}-{index}" for index in range(keys))
        self._entries = count(1)
        self._attempted: dict[str, set[int]] = {key: set() for key in self._keys}
        self._confirmed: dict[str, set[int]] = {key: set() for key in self._keys}
        self._since = -math.inf
        """When the last outage had killed every machine: an append that began before may be gone with them."""

    async def operate(self, sender: int, rng: random.Random, /) -> None:
        key = rng.choice(self._keys)
        entry = next(self._entries)
        self._attempted[key].add(entry)
        journal = self._stage.journal
        operation = journal.begin(self.name, key, "append", entry)
        try:
            confirmed = await self._stage.senders[sender].ref(self._actor, key).ask(Append, entry)
        except Exception as error:
            if not ambiguous(error):
                raise
            journal.fail(operation, error)
            return
        journal.confirm(operation, confirmed)
        if confirmed and operation.started >= self._since:
            self._confirmed[key].add(entry)

    def outage(self, dead: float, /) -> None:
        if self._actor.durable == "write":
            return
        self._since = dead
        for confirmed in self._confirmed.values():
            confirmed.clear()

    async def audit(self, reader: int, within: timedelta, /) -> list[Verdict]:
        system = self._stage.senders[reader]

        async def kept_by(key: str) -> None:
            confirmed = frozenset(self._confirmed[key])
            listing = await system.ref(self._actor, key).ask(Entries)
            broken = kept(key, listing.entries, confirmed, self._attempted[key])
            assert not broken, "; ".join(broken)

        failures = await settles(self._keys, kept_by, within)
        return [Verdict(f"{self.name}: confirmed ⊆ applied ⊆ attempted, each applied once", len(self._keys), failures)]

    async def rest(self, reader: int, within: timedelta, /) -> list[Verdict]:
        return await self.audit(reader, within)


class Pinned:
    """Appends to keys pinned to the machines up, each answered by the node that applied it.

    A pinned key has one copy, on the node at the address its ref names, and loses it with that incarnation. Only a
    node at that address may apply or answer, and what a read must hold is what the incarnation answering it confirmed.
    """

    def __init__(self, stage: Stage, /, *, names: int = 2) -> None:
        self.name = "pinned"
        self._stage = stage
        self._names = tuple(f"pinned-{index}" for index in range(names))
        self._entries = count(1)
        self._attempted: dict[tuple[str, str], set[int]] = {}
        self._confirmed: dict[tuple[str, str], dict[NodeId, set[int]]] = {}
        self._stamped = 0
        self._broken: list[str] = []

    async def operate(self, sender: int, rng: random.Random, /) -> None:
        up = self._stage.up()
        # During an outage no machine is up, and there is no address to pin a call to.
        if not up:
            return
        address, name = rng.choice(up), rng.choice(self._names)
        entry = next(self._entries)
        self._attempted.setdefault((address, name), set()).add(entry)
        journal = self._stage.journal
        operation = journal.begin(self.name, f"@{address}/{name}", "stamp", entry)
        try:
            node = await self._stage.senders[sender].ref(stamped, name, at=address).ask(Stamp, entry)
        except Exception as error:
            if not ambiguous(error):
                raise
            journal.fail(operation, error)
            return
        journal.confirm(operation, node)
        self._stamped += 1
        if node.address != address:
            self._broken.append(f"{operation} was applied by {node}, away from its address")
        else:
            self._confirmed.setdefault((address, name), {}).setdefault(node, set()).add(entry)

    def outage(self, dead: float, /) -> None:
        """Nothing to forget: what a read must hold is already what the incarnation answering it confirmed."""

    async def audit(self, reader: int, within: timedelta, /) -> list[Verdict]:
        system = self._stage.senders[reader]
        up = set(self._stage.up())
        keys = [key for key in self._attempted if key[0] in up]

        async def kept_there(key: tuple[str, str]) -> None:
            address, name = key
            confirmed = {node: frozenset(entries) for node, entries in self._confirmed.get(key, {}).items()}
            listing = await system.ref(stamped, name, at=address).ask(Entries)
            assert listing.node.address == address, f"@{address}/{name} was answered by {listing.node}"
            mine = confirmed.get(listing.node, frozenset())
            broken = kept(f"@{address}/{name}", listing.entries, mine, self._attempted[key])
            assert not broken, "; ".join(broken)

        failures = await settles(keys, kept_there, within)
        return [
            Verdict("pinned: applied only by the node at its address", self._stamped, (*self._broken,)),
            Verdict("pinned: a read holds what the incarnation answering it confirmed, once each", len(keys), failures),
        ]

    async def rest(self, reader: int, within: timedelta, /) -> list[Verdict]:
        return await self.audit(reader, within)


class Counters:
    """Increments of striped counters: a read lies between the adds confirmed before it and those attempted."""

    def __init__(self, stage: Stage, /, *, counters: int = 4, stripes: int = 2) -> None:
        self.name = "counter"
        self._stage = stage
        self._names = tuple(f"counter-{index}" for index in range(counters))
        self._facades = [
            {name: Collections(system).counter(name, stripes=stripes) for name in self._names}
            for system in stage.senders
        ]
        self._attempted = dict.fromkeys(self._names, 0)
        self._confirmed = dict.fromkeys(self._names, 0)
        """The adds confirmed since the last outage, whose counters started again from zero."""
        self._since = -math.inf

    async def operate(self, sender: int, rng: random.Random, /) -> None:
        name = rng.choice(self._names)
        self._attempted[name] += 1
        journal = self._stage.journal
        operation = journal.begin(self.name, name, "add", 1)
        try:
            await self._facades[sender][name].add(1)
        except Exception as error:
            if not ambiguous(error):
                raise
            journal.fail(operation, error)
            return
        journal.confirm(operation)
        if operation.started >= self._since:
            self._confirmed[name] += 1

    def outage(self, dead: float, /) -> None:
        self._since = dead
        self._confirmed = dict.fromkeys(self._names, 0)

    async def audit(self, reader: int, within: timedelta, /) -> list[Verdict]:
        facades = self._facades[reader]

        async def bounded(name: str) -> None:
            floor = self._confirmed[name]
            value = await facades[name].get()
            ceiling = self._attempted[name]
            assert floor <= value <= ceiling, f"{name} reads {value}, outside [{floor} confirmed, {ceiling} attempted]"

        failures = await settles(self._names, bounded, within)
        return [Verdict("counter: a read lies between the adds confirmed and attempted", len(self._names), failures)]

    async def rest(self, reader: int, within: timedelta, /) -> list[Verdict]:
        return await self.audit(reader, within)


def impossible(value: int | None, writes: Sequence[Operation], started: float, ended: float) -> str | None:
    """Why no write in `writes` can be what a read over `[started, ended]` gave, or None when one can.

    `value` is what the read gave, None for absence; `writes` are puts, whose argument is the value, and removes,
    whose argument is None. A read may give any write of the key that began before the read ended, unless a confirmed
    write began after that one was confirmed and ended before the read began. A write never confirmed may land at any
    moment after it began, so nothing overwrites it. Absence is also what the key held before any write.
    """
    sources = [write.applied_by for write in writes if write.argument == value and write.started < ended]
    if value is None:
        sources.append(-math.inf)
    if not sources:
        return f"no write of {value} had begun"
    before = [write for write in writes if write.confirmed and write.finished < started]
    latest = max(before, key=lambda write: write.started, default=None)
    if latest is None or any(source >= latest.started for source in sources):
        return None
    return f"{latest} overwrote every write it could have come from"


def erased(workload: str, key: str, dead: float, /, *, surely: bool = True) -> Operation:
    """An outage as a remove of `key`, of a workload held in memory only: done by `dead`, when every machine was.

    It is confirmed when what it removes surely went with the machines, and ambiguous when it may not have: a call that
    was still under way may land on the cluster that came back.
    """
    return Operation(0, workload, key, "outage", None, dead, dead, "confirmed" if surely else "ambiguous")


class Dicts:
    """Puts of unique values, removes and gets on the keys of one dict, each key read back as a register."""

    def __init__(self, stage: Stage, /, *, keys: int = 64, shards: int = 8) -> None:
        self.name = "dict"
        self._stage = stage
        self._keys = tuple(f"k{index}" for index in range(keys))
        self._facades = [
            Collections(system).dict("chaos-dict", key=str, value=int, index_shards=shards) for system in stage.senders
        ]
        self._values = count(1)
        self._writes: dict[str, list[Operation]] = {key: [] for key in self._keys}

    async def operate(self, sender: int, rng: random.Random, /) -> None:
        key = rng.choice(self._keys)
        facade = self._facades[sender]
        journal = self._stage.journal
        call: Awaitable[object]
        roll = rng.random()
        if roll < 0.6:
            value = next(self._values)
            operation = journal.begin(self.name, key, "put", value)
            self._writes[key].append(operation)
            call = facade.put(key, value)
        elif roll < 0.75:
            operation = journal.begin(self.name, key, "remove")
            self._writes[key].append(operation)
            call = facade.remove(key)
        else:
            operation = journal.begin(self.name, key, "get")
            call = facade.get(key)
        try:
            result = await call
        except Exception as error:
            if not ambiguous(error):
                raise
            journal.fail(operation, error)
            return
        journal.confirm(operation, result)

    def outage(self, dead: float, /) -> None:
        # A put still under way at `dead` may have landed on the cluster that came back. It ends after this remove, so
        # `impossible` lets a read give it, and `_certain` never counts on it.
        for key, writes in self._writes.items():
            writes.append(erased(self.name, key, dead))

    async def audit(self, reader: int, within: timedelta, /) -> list[Verdict]:
        facade = self._facades[reader]
        now = self._stage.journal.now

        async def possible(key: str) -> None:
            started = now()
            value = await facade.get(key)
            ended = now()
            why = impossible(None if isinstance(value, Missing) else value, self._writes[key], started, ended)
            assert why is None, f"{key} reads {value!r} over [{started:.3f}, {ended:.3f}]: {why}"

        async def sized(_: str) -> None:
            started = now()
            size = await facade.size()
            ended = now()
            floor = sum(self._certain(key, started, ended) for key in self._keys)
            ceiling = sum(self._put(key, ended) for key in self._keys)
            assert floor <= size <= ceiling, (
                f"size() is {size} over [{started:.3f}, {ended:.3f}], outside [{floor}, {ceiling}]"
            )

        async def listed(_: str) -> None:
            started = now()
            items = dict(await facade.items())
            ended = now()
            broken = [
                f"{key} listed with {value}: {why}"
                for key, value in items.items()
                if (why := impossible(value, self._writes.get(key, ()), started, ended)) is not None
            ]
            broken += [
                f"{key} missing" for key in self._keys if key not in items and self._certain(key, started, ended)
            ]
            assert not broken, f"items() over [{started:.3f}, {ended:.3f}]: " + "; ".join(broken)

        reads = await settles(self._keys, possible, within)
        sizes = await settles(("size",), sized, within)
        listings = await settles(("items",), listed, within)
        return [
            Verdict("dict: get() gives a value no confirmed write overwrote", len(self._keys), reads),
            Verdict("dict: size() between the keys surely there and the keys ever put", 1, sizes),
            Verdict("dict: items() lists what get() could give, and every key surely there", 1, listings),
        ]

    async def rest(self, reader: int, within: timedelta, /) -> list[Verdict]:
        verdicts = await self.audit(reader, within)
        facade = self._facades[reader]

        async def agree(_: str) -> None:
            values = await asyncio.gather(*(facade.get(key) for key in self._keys))
            present = {
                key: value for key, value in zip(self._keys, values, strict=True) if not isinstance(value, Missing)
            }
            items = dict(await facade.items())
            size = await facade.size()
            assert items == present, f"items() differs from get() on {sorted(items.keys() ^ present.keys())}"
            assert size == len(present), f"size() is {size} with {len(present)} keys present"

        failures = await settles(("at rest",), agree, within)
        return [*verdicts, Verdict("dict at rest: size() and items() agree with get()", 1, failures)]

    def _certain(self, key: str, started: float, ended: float) -> bool:
        """Whether a read over `[started, ended]` must find `key`: the last put confirmed before the read began came
        after every remove that began before the read ended, each of those confirmed."""
        writes = self._writes[key]
        puts = [write for write in writes if write.argument is not None and write.applied_by < started]
        put = max(puts, key=lambda write: write.started, default=None)
        removes = [write for write in writes if write.argument is None and write.started < ended]
        return put is not None and all(remove.applied_by < put.started for remove in removes)

    def _put(self, key: str, ended: float) -> bool:
        """Whether a read that ended at `ended` may find `key`: a put of it had begun."""
        return any(write.argument is not None and write.started < ended for write in self._writes[key])


@dataclass(slots=True)
class _Member:
    added: Operation
    removed: Operation | None = None


class Sets:
    """Adds of unique values to one set, and removes of values whose add was confirmed."""

    def __init__(self, stage: Stage, /, *, shards: int = 8) -> None:
        self.name = "set"
        self._stage = stage
        self._facades = [Collections(system).set("chaos-set", value=int, shards=shards) for system in stage.senders]
        self._values = count(1)
        self._members: dict[int, _Member] = {}
        self._removable: list[int] = []
        self._since = -math.inf
        """When the last outage had killed every machine: a value whose add began before is never removed again."""

    async def operate(self, sender: int, rng: random.Random, /) -> None:
        facade = self._facades[sender]
        journal = self._stage.journal
        call: Awaitable[object]
        roll = rng.random()
        if roll < 0.4 and self._removable:
            at = rng.randrange(len(self._removable))
            value = self._removable[at]
            self._removable[at] = self._removable[-1]
            self._removable.pop()
            operation = journal.begin(self.name, _SET, "remove", value)
            self._members[value].removed = operation
            call = facade.remove(value)
        elif roll < 0.9 or not self._members:
            value = next(self._values)
            operation = journal.begin(self.name, _SET, "add", value)
            self._members[value] = _Member(operation)
            call = facade.add(value)
        else:
            value = rng.randint(1, len(self._members))
            operation = journal.begin(self.name, _SET, "contains", value)
            call = facade.contains(value)
        try:
            result = await call
        except Exception as error:
            if not ambiguous(error):
                raise
            journal.fail(operation, error)
            return
        journal.confirm(operation, result)
        if operation.action == "add" and operation.started >= self._since:
            self._removable.append(value)

    def outage(self, dead: float, /) -> None:
        # The outage stands for the remove of every value added before it; a remove made after would take its place.
        self._since = dead
        self._removable = [value for value in self._removable if self._members[value].added.started >= dead]
        for member in self._members.values():
            if member.added.started < dead:
                member.removed = erased(self.name, _SET, dead, surely=member.added.applied_by < dead)

    async def audit(self, reader: int, within: timedelta, /) -> list[Verdict]:
        facade = self._facades[reader]
        now = self._stage.journal.now

        async def listed(_: str) -> None:
            started = now()
            present = set(await facade.items())
            ended = now()
            members = self._members
            missing = sorted(
                value for value, member in members.items() if value not in present and _certain(member, started, ended)
            )
            undone = sorted(value for value in present if value in members and _gone(members[value], started))
            invented = sorted(
                value for value in present if value not in members or members[value].added.started >= ended
            )
            assert not (missing or undone or invented), (
                f"items() over [{started:.3f}, {ended:.3f}] misses confirmed adds {missing}, holds confirmed removes "
                f"{undone} and values never added {invented}"
            )

        async def sized(_: str) -> None:
            started = now()
            size = await facade.size()
            ended = now()
            members = self._members.values()
            floor = sum(_certain(member, started, ended) for member in members)
            ceiling = sum(member.added.started < ended and not _gone(member, started) for member in members)
            assert floor <= size <= ceiling, (
                f"size() is {size} over [{started:.3f}, {ended:.3f}], outside [{floor}, {ceiling}]"
            )

        listings = await settles(("items",), listed, within)
        sizes = await settles(("size",), sized, within)
        return [
            Verdict("set: items() holds every confirmed add, no confirmed remove, nothing never added", 1, listings),
            Verdict("set: size() between the values surely there and those that may be", 1, sizes),
        ]

    async def rest(self, reader: int, within: timedelta, /) -> list[Verdict]:
        return await self.audit(reader, within)


def _certain(member: _Member, started: float, ended: float) -> bool:
    """Whether a read of a set over `[started, ended]` must find the value: its add was confirmed before the read
    began, and no remove of it began before the read ended."""
    return member.added.applied_by < started and (member.removed is None or member.removed.started >= ended)


def _gone(member: _Member, started: float) -> bool:
    """Whether a read of a set that began at `started` must not find the value: its remove was confirmed before."""
    return member.removed is not None and member.removed.applied_by < started


class Queues:
    """Offers of unique values to one queue, and polls and drains of it.

    What comes out is checked as it comes: no value twice, none that was not offered first. At rest the queue is
    drained, and every confirmed offer must have come out once or still be there, but for as many as the polls and
    drains whose answer was lost could have taken, and for those offered before an outage; and nothing may be left
    behind a value offered after it that came out, which is the order of a queue.
    """

    def __init__(self, stage: Stage, /) -> None:
        self.name = "queue"
        self._stage = stage
        self._facades = [Collections(system).queue(_QUEUE, value=int) for system in stage.senders]
        self._values = count(1)
        self._offers: dict[int, Operation] = {}
        self._deliveries: dict[int, Operation] = {}
        self._lossy = 0
        """How many values the polls and drains whose answer was lost may have taken."""
        self._erased: set[int] = set()
        """The values offered before an outage, which may have gone with the machines."""
        self._broken: list[str] = []

    async def operate(self, sender: int, rng: random.Random, /) -> None:
        facade = self._facades[sender]
        journal = self._stage.journal
        roll = rng.random()
        # Offers come a little faster than polls and drains take, so the queue keeps a few values to keep in order.
        if roll < 0.6:
            value = next(self._values)
            operation = self._offers[value] = journal.begin(self.name, _QUEUE, "offer", value)
            await self._call(operation, facade.offer(value))
        elif roll < 0.85:
            operation = journal.begin(self.name, _QUEUE, "poll")
            taken = await self._call(operation, facade.poll(), lost=1)
            if taken is not None and not isinstance(taken, Missing):
                self._delivered([taken], operation)
        elif roll < 0.95:
            limit = rng.randint(1, 5)
            operation = journal.begin(self.name, _QUEUE, "drain", limit)
            if (drained := await self._call(operation, facade.drain(limit), lost=limit)) is not None:
                self._delivered(drained, operation)
        else:
            await self._call(journal.begin(self.name, _QUEUE, "size"), facade.size())

    def outage(self, dead: float, /) -> None:
        self._erased |= {value for value, offer in self._offers.items() if offer.started < dead}

    async def audit(self, reader: int, within: timedelta, /) -> list[Verdict]:
        facade = self._facades[reader]
        now = self._stage.journal.now

        async def sized(_: str) -> None:
            started = now()
            size = await facade.size()
            ended = now()
            offered = sum(offer.started < ended for offer in self._offers.values())
            ceiling = offered - sum(taken.finished < started for taken in self._deliveries.values())
            assert size <= ceiling, f"size() is {size} over [{started:.3f}, {ended:.3f}], above {ceiling} not yet taken"

        sizes = await settles(("size",), sized, within)
        taken = len(self._deliveries)
        return [
            Verdict("queue: nothing comes out twice, or before it was offered", taken, (*self._broken,)),
            Verdict("queue: size() at most what was offered and not yet taken", 1, sizes),
        ]

    async def rest(self, reader: int, within: timedelta, /) -> list[Verdict]:
        verdicts = await self.audit(reader, within)
        facade = self._facades[reader]
        remaining: list[int] = []

        async def drained(_: str) -> None:
            while True:
                try:
                    taken = await facade.drain(_DRAIN)
                except Exception as error:
                    if ambiguous(error):
                        self._lossy += _DRAIN
                    raise
                if not taken:
                    return
                remaining.extend(taken)

        unreadable = await settles(("drain",), drained, within)
        left = set(remaining)
        delivered = self._deliveries.keys()
        broken = list(unreadable)
        if twice := sorted(value for value, times in Tally(remaining).items() if times > 1 or value in delivered):
            broken.append(f"values that came out twice: {twice}")
        if invented := sorted(left - self._offers.keys()):
            broken.append(f"values left that were never offered: {invented}")
        confirmed = {value for value, offer in self._offers.items() if offer.confirmed and value not in self._erased}
        if len(lost := confirmed - delivered - left) > self._lossy:
            broken.append(
                f"{len(lost)} confirmed offers lost, more than the {self._lossy} that lost answers could have taken: "
                f"{sorted(lost)[:50]}"
            )
        newest = max((self._offers[value].started for value in delivered), default=-math.inf)
        behind = sorted(value for value in left & confirmed if self._offers[value].finished < newest)
        order = (f"left behind a value offered after them and taken, at {newest:.3f}: {behind[:50]}",) if behind else ()
        return [
            *verdicts,
            Verdict("queue at rest: each confirmed offer came out once or is still there", len(confirmed), (*broken,)),
            Verdict("queue at rest: nothing is left behind a value offered after it", len(left), order),
        ]

    async def _call[T](self, operation: Operation, call: Awaitable[T], /, *, lost: int = 0) -> T | None:
        """Make the call of `operation` and record how it ended: what it answered, or None when it is ambiguous."""
        journal = self._stage.journal
        try:
            result = await call
        except Exception as error:
            if not ambiguous(error):
                raise
            journal.fail(operation, error)
            self._lossy += lost
            return None
        journal.confirm(operation, result)
        return result

    def _delivered(self, values: Sequence[int], operation: Operation) -> None:
        for value in values:
            offer = self._offers.get(value)
            if (earlier := self._deliveries.get(value)) is not None:
                self._broken.append(f"{value} came out twice: from {earlier} and from {operation}")
            elif offer is None or offer.started >= operation.finished:
                self._broken.append(f"{value} came out from {operation}, before it was offered")
            else:
                self._deliveries[value] = operation


class Barriers:
    """Rounds of `parties` arrivals at one barrier, each party from a sender of its own and a moment after another.

    A party released before the last party of its round had even begun waiting was released by arrivals that were not
    its round's. A party that ended without being released may still be waiting at the barrier until its deadline, and
    counts towards the next round there, so a barrier is not judged until that deadline has passed.
    """

    def __init__(
        self, stage: Stage, /, *, barriers: int = 2, parties: int = 3, patience: timedelta = timedelta(seconds=20)
    ) -> None:
        self.name = "barrier"
        self._stage = stage
        self._names = tuple(f"barrier-{index}" for index in range(barriers))
        self._parties = parties
        self._patience = patience
        self._facades = [
            {name: Collections(system).barrier(name, parties=parties) for name in self._names}
            for system in stage.senders
        ]
        self._busy = {name: asyncio.Lock() for name in self._names}
        self._stale = dict.fromkeys(self._names, -math.inf)
        """Until when a party that was not released may still be waiting at each barrier."""
        self._judged = 0
        self._broken: list[str] = []

    async def operate(self, sender: int, rng: random.Random, /) -> None:
        free = [name for name in self._names if not self._busy[name].locked()]
        if not free:
            await asyncio.sleep(0.05)
            return
        name = rng.choice(free)
        async with self._busy[name]:
            await self._round(name, sender, rng)

    def outage(self, dead: float, /) -> None:
        """Nothing to forget: an outage only takes arrivals away, and a party it cut short already delays judging."""

    async def audit(self, reader: int, within: timedelta, /) -> list[Verdict]:
        invariant = "barrier: no party released before every party of its round began waiting"
        return [Verdict(invariant, self._judged, tuple(self._broken))]

    async def rest(self, reader: int, within: timedelta, /) -> list[Verdict]:
        facades = self._facades[reader]

        async def empty(name: str) -> None:
            waiting = await facades[name].waiting()
            assert waiting == 0, f"{name} still has {waiting} parties waiting"

        # A party nobody withdrew leaves only at its deadline.
        patience = max(within, timedelta(seconds=_DEADLINE + 2 * SKEW + 5.0))
        failures = await settles(self._names, empty, patience)
        verdicts = await self.audit(reader, within)
        return [*verdicts, Verdict("barrier at rest: no party is left waiting", len(self._names), failures)]

    async def _round(self, name: str, sender: int, rng: random.Random) -> None:
        journal = self._stage.journal
        judged = journal.now() >= self._stale[name]
        others = [rng.randrange(len(self._stage.senders)) for _ in range(self._parties - 1)]
        async with asyncio.TaskGroup() as arrivals:
            waits = [
                arrivals.create_task(self._wait(name, party, index * rng.uniform(0.02, 0.1)))
                for index, party in enumerate([sender, *others])
            ]
        parties = [wait.result() for wait in waits]
        last = max(party.started for party in parties)
        if judged and any(party.confirmed for party in parties):
            self._judged += 1
            if early := [party for party in parties if party.confirmed and party.finished < last]:
                self._broken.append(f"{name}: {early[0]} was released before the last party arrived at {last:.3f}")
        for party in parties:
            if not party.confirmed:
                self._stale[name] = max(self._stale[name], party.finished + _DEADLINE + 2 * SKEW + 1.0)

    async def _wait(self, name: str, sender: int, delay: float) -> Operation:
        await asyncio.sleep(delay)
        journal = self._stage.journal
        operation = journal.begin(self.name, name, "wait")
        try:
            async with asyncio.timeout(self._patience.total_seconds()):
                await self._facades[sender][name].wait()
        except Exception as error:
            if not ambiguous(error):
                raise
            journal.fail(operation, error)
        else:
            journal.confirm(operation, True)
        return operation


@dataclass(slots=True)
class _Grant:
    acquired: Operation
    token: int
    released: Operation | None = None


class Locks:
    """Leases of locks taken, held for a moment and released.

    A token granted after another was granted and answered must be larger, and no token is granted twice. A lease holds
    from its grant until its release began or its TTL ran out, and no other grant may fall inside that; the TTL is
    counted short by what two clocks may be apart, since each owner expires a lease by its own. An outage starts every
    lock over, so the grants on each side of one are judged apart, and a grant asked for across it on neither side.
    """

    def __init__(self, stage: Stage, /, *, locks: int = 2, ttl: timedelta = timedelta(seconds=5)) -> None:
        self.name = "lock"
        self._stage = stage
        self._names = tuple(f"lock-{index}" for index in range(locks))
        self._ttl = ttl
        self._facades = [
            {name: Collections(system).lock(name, ttl=ttl.total_seconds()) for name in self._names}
            for system in stage.senders
        ]
        self._grants: dict[str, list[_Grant]] = {name: [] for name in self._names}
        self._eras: list[dict[str, list[_Grant]]] = []
        """The grants of each cluster an outage ended."""
        self._since = -math.inf

    async def operate(self, sender: int, rng: random.Random, /) -> None:
        name = rng.choice(self._names)
        lock = self._facades[sender][name]
        journal = self._stage.journal
        waits = rng.random() < 0.3
        operation = journal.begin(self.name, name, "acquire" if waits else "try_lock")
        lease: Lease | None
        try:
            if waits:
                async with asyncio.timeout(2.0):
                    lease = await lock.acquire()
            else:
                lease = await lock.try_lock()
        except Exception as error:
            if not ambiguous(error):
                raise
            journal.fail(operation, error)
            return
        journal.confirm(operation, None if lease is None else lease.token)
        if lease is None:
            return
        grant = _Grant(operation, lease.token)
        if operation.started >= self._since:
            self._grants[name].append(grant)
        await asyncio.sleep(rng.uniform(0.0, 0.05))
        release = grant.released = journal.begin(self.name, name, "release", lease.token)
        try:
            released = await lease.release()
        except Exception as error:
            if not ambiguous(error):
                raise
            journal.fail(release, error)
            return
        journal.confirm(release, released)

    def outage(self, dead: float, /) -> None:
        self._eras.append(self._grants)
        self._grants = {name: [] for name in self._names}
        self._since = dead

    async def audit(self, reader: int, within: timedelta, /) -> list[Verdict]:
        eras = (*self._eras, self._grants)
        granted = sum(len(grants) for era in eras for grants in era.values())
        fenced = tuple(line for era in eras for name, grants in era.items() for line in _fenced(name, grants))
        alone = tuple(line for era in eras for name, grants in era.items() for line in self._alone(name, grants))
        return [
            Verdict("lock: fencing tokens are unique and grow with every grant", granted, fenced),
            Verdict("lock: no grant while another lease holds", granted, alone),
        ]

    async def rest(self, reader: int, within: timedelta, /) -> list[Verdict]:
        return await self.audit(reader, within)

    def _alone(self, name: str, grants: Sequence[_Grant]) -> list[str]:
        held = self._ttl.total_seconds() - 2 * SKEW - 0.5
        broken: list[str] = []
        for grant, later in pairwise(sorted(grants, key=lambda grant: grant.token)):
            released = math.inf if grant.released is None else grant.released.started
            until = min(released, grant.acquired.started + held)
            if later.token != grant.token and later.acquired.finished < until:
                broken.append(f"{name}: {later.acquired} took a lease {grant.acquired} held until {until:.3f}")
        return broken


def _fenced(name: str, grants: Sequence[_Grant]) -> list[str]:
    """What breaks the fencing of `grants`: a token granted twice, or one no larger than a token answered before the
    grant of it began."""
    tokens = Tally(grant.token for grant in grants)
    broken = [f"{name}: token {token} granted {times} times" for token, times in tokens.items() if times > 1]
    answered = sorted(grants, key=lambda grant: grant.acquired.finished)
    highest: _Grant | None = None
    seen = 0
    for grant in sorted(grants, key=lambda grant: grant.acquired.started):
        while seen < len(answered) and answered[seen].acquired.finished < grant.acquired.started:
            if highest is None or answered[seen].token > highest.token:
                highest = answered[seen]
            seen += 1
        if highest is not None and grant.token <= highest.token:
            broken.append(f"{name}: {grant.acquired} got token {grant.token}, not above that of {highest.acquired}")
    return broken


def durable(stage: Stage, /) -> Ledgers:
    """The ledger its store keeps too, on the SQLite file every node of the run shares: it outlives an outage."""
    return Ledgers(stage, actor=durable_ledger, name="durable")


WORKLOADS: Mapping[str, Callable[[Stage], Workload]] = {
    "ledger": Ledgers,
    "durable": durable,
    "pinned": Pinned,
    "counter": Counters,
    "dict": Dicts,
    "set": Sets,
    "queue": Queues,
    "barrier": Barriers,
    "lock": Locks,
}
"""Every kind a run can put under its traffic, by the name `CHAOS_WORKLOADS` picks it with."""

_SET = "chaos-set"
_QUEUE = "chaos-queue"
_DRAIN = 20
"""How many values one drain at rest takes: a drain whose answer is lost may have taken that many."""
_DEADLINE = 30.0
"""How long a barrier keeps a party after the last time it asked (`casty.collections._wait`)."""
