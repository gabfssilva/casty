"""casty.Semaphore / casty.Lock: distributed counting semaphore and mutual-
exclusion lock (spec 06 §4.3-4.4). Single-owner like Register/Queue — one owner
actor serializes acquire/release, state replicated by quorum. A Lock is a
Semaphore of capacity 1.

Each held permit is a *lease*: `acquire` returns a monotonic fencing token with
a TTL; the holder must `renew` before it expires or the permit is reclaimed
(lazy expiry on access). Blocking is client-side — the actor mailbox is serial,
so a permit that waits inside the handler would deadlock the release that frees
it. `acquire(timeout=...)` polls with backoff."""

from __future__ import annotations

import asyncio
import dataclasses
import time
import typing

from casty.actors.proxy import Caller
from casty.actors.registry import ActorInfo, Consistency, actor
from casty.collections import _sharded
from casty.errors import CastyTimeoutError

_PREFIX = "casty.SemaphoreShard"


def _shard_class(
    wire: str, replicas: int, write: Consistency | int, read: Consistency | int
) -> None:
    @actor(name=wire, replicas=replicas, write=write, read=read)
    class SemaphoreShard:
        next_token: int = 0
        # token -> (permits held, wall-clock expiry)
        leases: dict[int, tuple[int, float]] = dataclasses.field(default_factory=dict)

        def _purge(self, now: float) -> None:
            for token in [t for t, (_, exp) in self.leases.items() if exp <= now]:
                del self.leases[token]

        def _used(self, now: float) -> int:
            return sum(n for n, exp in self.leases.values() if exp > now)

        async def acquire(self, n: int, ttl: float, capacity: int) -> int | None:
            now = time.time()
            self._purge(now)
            if capacity - self._used(now) < n:
                return None
            token = self.next_token
            self.next_token += 1
            self.leases[token] = (n, now + ttl)
            return token

        async def release(self, token: int) -> bool:
            self._purge(time.time())
            return self.leases.pop(token, None) is not None

        async def renew(self, token: int, ttl: float) -> bool:
            now = time.time()
            self._purge(now)
            held = self.leases.get(token)
            if held is None:
                return False
            self.leases[token] = (held[0], now + ttl)
            return True

        async def available(self, capacity: int) -> int:
            return capacity - self._used(time.time())


_sharded.register(_PREFIX, _shard_class)


def shard_info(replicas: int, write: Consistency | int, read: Consistency | int) -> ActorInfo:
    return _sharded.materialize(_PREFIX, replicas, write, read)


@dataclasses.dataclass(frozen=True, slots=True)
class Lease:
    """A held permit. `token` is a strictly increasing fencing token: pass it to
    the protected resource so a stale holder (expired lease) is rejected. Async
    context manager — releases on exit."""

    token: int
    _owner: _SemaphoreBase

    async def renew(self, ttl: float = 30.0) -> bool:
        """Extend the lease. False if it already expired (permit lost — the
        caller must reacquire); the token then no longer holds a permit."""
        return await self._owner._renew(self.token, ttl)

    async def release(self) -> bool:
        """Free the permit. False if it was already expired or released."""
        return await self._owner._release(self.token)

    async def __aenter__(self) -> Lease:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.release()


class _SemaphoreBase(_sharded.ShardRouter):
    def __init__(self, caller: Caller, name: str, info: ActorInfo, capacity: int) -> None:
        super().__init__(caller, name, info, 1)
        self._capacity = capacity

    async def _try(self, n: int, ttl: float) -> Lease | None:
        token = await self._call(0, "acquire", [n, ttl, self._capacity])
        if token is None:
            return None
        return Lease(typing.cast(int, token), self)

    async def _acquire(self, n: int, ttl: float, timeout: float | None) -> Lease:
        loop = asyncio.get_running_loop()
        deadline = None if timeout is None else loop.time() + timeout
        delay = 0.01
        while True:
            lease = await self._try(n, ttl)
            if lease is not None:
                return lease
            if deadline is not None and loop.time() >= deadline:
                raise CastyTimeoutError(
                    f"{self._name!r}: acquire timed out after {timeout}s"
                )
            wait = delay if deadline is None else min(delay, max(0.0, deadline - loop.time()))
            await asyncio.sleep(wait)
            delay = min(delay * 2, 0.25)

    async def _release(self, token: int) -> bool:
        return typing.cast(bool, await self._call(0, "release", [token]))

    async def _renew(self, token: int, ttl: float) -> bool:
        return typing.cast(bool, await self._call(0, "renew", [token, ttl]))

    async def _available(self) -> int:
        return typing.cast(int, await self._call(0, "available", [self._capacity]))


class Semaphore(_SemaphoreBase):
    """Distributed counting semaphore. `capacity` permits; single owner (shard
    0) serializes grants."""

    async def try_acquire(self, n: int = 1, *, ttl: float = 30.0) -> Lease | None:
        return await self._try(n, ttl)

    async def acquire(
        self, n: int = 1, *, ttl: float = 30.0, timeout: float | None = None
    ) -> Lease:
        return await self._acquire(n, ttl, timeout)

    async def available(self) -> int:
        return await self._available()


class Lock(_SemaphoreBase):
    """Distributed mutual-exclusion lock — a Semaphore of capacity 1. Async
    context manager: `async with node.lock(name):` acquires (blocking up to the
    factory's `timeout`) and releases on exit."""

    def __init__(
        self,
        caller: Caller,
        name: str,
        info: ActorInfo,
        *,
        ttl: float,
        timeout: float | None,
    ) -> None:
        super().__init__(caller, name, info, 1)
        self._ttl = ttl
        self._timeout = timeout
        self._held: Lease | None = None

    async def try_lock(self, *, ttl: float | None = None) -> Lease | None:
        return await self._try(1, ttl if ttl is not None else self._ttl)

    async def acquire(self, *, ttl: float | None = None, timeout: float | None = None) -> Lease:
        return await self._acquire(
            1,
            ttl if ttl is not None else self._ttl,
            timeout if timeout is not None else self._timeout,
        )

    async def locked(self) -> bool:
        return await self._available() == 0

    async def __aenter__(self) -> Lease:
        self._held = await self.acquire()
        return self._held

    async def __aexit__(self, *exc: object) -> None:
        if self._held is not None:
            await self._held.release()
            self._held = None
