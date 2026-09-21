"""Concurrent appends to many keys, and the invariant they must satisfy when owners change under them.

The traffic is what a test runs while it breaks the cluster. What it proves is not a sequence of events but three
things about every key at the end:

    confirmed ⊆ applied ⊆ attempted, and applied without repetition

`confirmed ⊆ applied` is the durability: an `ask` that answered `True` never disappears. `applied ⊆ attempted` says no
state came from another key or from garbage. No repetition says no message was applied twice while the key moved.
"""

import asyncio
import random
from collections.abc import AsyncGenerator, Sequence
from contextlib import asynccontextmanager, suppress
from datetime import timedelta
from itertools import count

from casty import System, Unavailable
from tests.app import Append, Entries, Listing, ledger
from tests.cluster import Harness
from tests.support import eventually


class Traffic:
    """Tasks appending unique ids to `keys` ledger keys, each attempt from a random sender.

    A sender is any `System`: without `senders` every attempt goes out from one of the nodes still running, and with
    them, only from the ones the test named, which is how a test keeps the traffic off the node it is about to break.
    """

    @classmethod
    @asynccontextmanager
    async def running(
        cls,
        harness: Harness,
        /,
        *,
        senders: Sequence[System] | None = None,
        keys: int = 50,
        workers: int = 8,
        seed: int = 0,
    ) -> AsyncGenerator["Traffic"]:
        traffic = cls(harness, senders=senders, keys=keys, seed=seed)
        async with asyncio.TaskGroup() as tasks:
            for _ in range(workers):
                tasks.create_task(traffic._append_until_stopped())
            try:
                yield traffic
            finally:
                traffic._stop.set()

    def __init__(self, harness: Harness, /, *, senders: Sequence[System] | None, keys: int, seed: int) -> None:
        self.keys = tuple(f"key-{index}" for index in range(keys))
        self._harness = harness
        self._chosen = None if senders is None else tuple(senders)
        self._rng = random.Random(seed)
        self._ids = count(1)
        self._attempted: dict[str, set[int]] = {key: set() for key in self.keys}
        self._confirmed: dict[str, set[int]] = {key: set() for key in self.keys}
        self._refused: list[str] = []
        self._stop = asyncio.Event()

    async def settle(self, period: timedelta = timedelta(milliseconds=500)) -> None:
        """Keep the traffic going for a while longer, so that what a test just broke is exercised by it."""
        await asyncio.sleep(period.total_seconds())

    async def verify(self, within: timedelta = timedelta(seconds=30)) -> dict[str, Listing]:
        """Check the invariant on every key, and give back what each one answered, with the node that answered."""
        listings: dict[str, Listing] = {}

        async def every_key_kept_what_it_confirmed() -> None:
            for key in self.keys:
                listing = listings[key] = await self._entries(key)
                applied = listing.entries
                assert len(set(applied)) == len(applied), f"{key} applied an entry twice: {applied}"
                assert self._confirmed[key] <= set(applied), f"{key} lost {self._confirmed[key] - set(applied)}"
                assert set(applied) <= self._attempted[key], f"{key} invented {set(applied) - self._attempted[key]}"

        await eventually(every_key_kept_what_it_confirmed, within)
        return listings

    def confirmed(self, key: str, /) -> frozenset[int]:
        return frozenset(self._confirmed[key])

    @property
    def refused(self) -> tuple[str, ...]:
        """The attempts that answered neither yes nor no, each one with the key and what ended it."""
        return tuple(self._refused)

    def forget(self) -> None:
        """Drop what was refused so far, so that a test counts only from the event it is about."""
        self._refused.clear()

    async def _append_until_stopped(self) -> None:
        while not self._stop.is_set():
            await self._append_once()

    async def _append_once(self) -> None:
        senders = self._senders()
        if not senders:
            await asyncio.sleep(0.01)
            return
        sender = self._rng.choice(senders)
        key = self._rng.choice(self.keys)
        entry = next(self._ids)
        self._attempted[key].add(entry)
        try:
            confirmed = await sender.ref(ledger, key).ask(Append, entry)
        except (Unavailable, TimeoutError) as error:
            # Both mean "may or may not have been applied", which is why the invariant is containment, not equality.
            self._refused.append(f"{key}: {type(error).__name__}: {error}")
            return
        except RuntimeError:
            if sender in self._senders():
                raise
            return  # the machine of this node died while the attempt was in flight
        if confirmed:
            self._confirmed[key].add(entry)

    def _senders(self) -> tuple[System, ...]:
        """What an attempt goes out from: what the test named, or every node still running."""
        if self._chosen is not None:
            return self._chosen
        return tuple(node.system for node in self._harness.nodes)

    async def _entries(self, key: str, /) -> Listing:
        for sender in self._senders():
            with suppress(Unavailable, TimeoutError):
                return await sender.ref(ledger, key).ask(Entries)
        raise AssertionError(f"nothing answered for {key}")
