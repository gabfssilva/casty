"""casty.Barrier: a distributed cyclic barrier (spec 06). Single-owner like
Register/Queue — one owner actor counts arrivals, state replicated by quorum.
The Nth arrival releases the current generation and resets the count, so the
same barrier is reusable round after round.

Blocking is client-side, exactly as in Semaphore/Lock — the actor mailbox is
serial, so a waiter parked inside the handler would deadlock the arrival that
releases it. `wait(timeout=...)` arrives once, then polls the generation with
backoff; a timed-out waiter withdraws its arrival."""

from __future__ import annotations

import asyncio
import typing

from casty.actors.proxy import Caller
from casty.actors.registry import ActorInfo, Consistency, actor
from casty.collections import _sharded
from casty.errors import CastyError, CastyTimeoutError

_PREFIX = "casty.BarrierShard"


def _shard_class(
    wire: str, replicas: int, write: Consistency | int, read: Consistency | int
) -> None:
    @actor(name=wire, replicas=replicas, write=write, read=read)
    class BarrierShard:
        generation: int = 0
        arrived: int = 0

        async def arrive(self, parties: int) -> tuple[int, bool]:
            generation = self.generation
            self.arrived += 1
            if self.arrived >= parties:
                self.arrived = 0
                self.generation += 1
                return generation, True
            return generation, False

        async def released(self, generation: int) -> bool:
            return self.generation > generation

        async def leave(self, generation: int) -> bool:
            if generation != self.generation:
                return False  # that generation already released; nothing to withdraw
            if self.arrived > 0:
                self.arrived -= 1
            return True

        async def waiting(self) -> int:
            return self.arrived


_sharded.register(_PREFIX, _shard_class)


def shard_info(replicas: int, write: Consistency | int, read: Consistency | int) -> ActorInfo:
    return _sharded.materialize(_PREFIX, replicas, write, read)


class Barrier(_sharded.ShardRouter):
    """Distributed cyclic barrier, from `ActorSystem.barrier`. A single owner
    actor counts arrivals; the party that completes a generation releases every
    waiter of that generation, and the barrier resets for the next round."""

    def __init__(self, caller: Caller, name: str, info: ActorInfo, parties: int) -> None:
        super().__init__(caller, name, info, 1)
        self._parties = parties

    @property
    def parties(self) -> int:
        """Arrivals needed to release a generation. Client-supplied, not
        stored — callers of the same name must agree on it."""
        return self._parties

    async def wait(self, timeout: float | None = None) -> None:
        """Arrive at the barrier and block until `parties` have arrived.

        Blocking is client-side: after arriving, the caller polls the
        generation with backoff until it releases.

        Parameters
        ----------
        timeout : float | None
            Seconds to wait for the remaining parties. None waits forever.

        Raises
        ------
        CastyTimeoutError
            If `timeout` elapses first. The arrival is withdrawn, so the
            barrier is not left one party short.
        """
        arrival = await self._call(0, "arrive", [self._parties])
        generation, released = typing.cast(tuple[int, bool], arrival)
        if released:
            return
        loop = asyncio.get_running_loop()
        deadline = None if timeout is None else loop.time() + timeout
        delay = 0.01
        while True:
            try:
                if await self._call(0, "released", [generation]):
                    return
            except CastyError:
                pass  # failover in progress; the arrival is replicated state — keep polling
            if deadline is not None and loop.time() >= deadline:
                withdrawn = await self._call(0, "leave", [generation])
                if not withdrawn:
                    return  # released while timing out: the barrier tripped, honor it
                raise CastyTimeoutError(
                    f"{self._name!r}: wait timed out after {timeout}s"
                )
            wait = delay if deadline is None else min(delay, max(0.0, deadline - loop.time()))
            await asyncio.sleep(wait)
            delay = min(delay * 2, 0.25)

    async def waiting(self) -> int:
        """Parties currently arrived and waiting in this generation."""
        return typing.cast(int, await self._call(0, "waiting", []))
