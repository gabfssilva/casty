"""Shared by the node containers and the pytest driver: the replicated test
actor, cluster-wide timing config, and driver-side convergence helpers."""

from __future__ import annotations

import asyncio
import os
from collections.abc import Awaitable, Callable

import casty

WRITERS = int(os.environ.get("CHAOS_WRITERS", "20"))
RATE = float(os.environ.get("CHAOS_RATE", "0"))  # total ops/s; 0 = no cap

CHAOS_CONFIG = casty.Config(
    transport=casty.TransportConfig(
        connect_timeout=3.0,
        request_timeout=5.0,
        keepalive_interval=2.0,
        keepalive_timeout=2.0,
        reconnect_base=0.1,
        reconnect_max=2.0,
    ),
    membership=casty.MembershipConfig(
        shuffle_interval=1.0,
        suspicion_timeout=2.0,
        anti_entropy_interval=5.0,
        sweep_interval=0.25,
        join_timeout=30.0,
    ),
    call_timeout=10.0,
    client_sync_interval=1.0,
)


@casty.actor(name="chaos.Counter", replicas=3, write=casty.MAJORITY)
class Counter:
    value: int = 0

    async def add(self, n: int) -> int:
        self.value += n
        return self.value

    async def read(self) -> int:
        return self.value


async def view_size(addr: str) -> int:
    """How many alive members the node at `addr` sees, via a one-shot SYNC."""
    client = await casty.connect([addr], config=CHAOS_CONFIG)
    try:
        return len(client._addrs)
    finally:
        await client.close()


async def wait_views(addrs: list[str], expected: int, *, timeout: float = 180.0) -> None:
    """Poll every node until all report exactly `expected` alive members."""
    sem = asyncio.Semaphore(20)

    async def size(addr: str) -> int:
        async with sem:
            try:
                return await view_size(addr)
            except (casty.CastyError, OSError, TimeoutError):
                return -1

    deadline = asyncio.get_running_loop().time() + timeout
    sizes: list[int] = []
    while asyncio.get_running_loop().time() < deadline:
        sizes = list(await asyncio.gather(*(size(a) for a in addrs)))
        if all(s == expected for s in sizes):
            return
        await asyncio.sleep(1.0)
    raise AssertionError(f"views did not converge to {expected}: {sorted(sizes)}")


class Workload:
    """Concurrent writers incrementing replicated counters, with acked/attempted
    counts per key. `rate` caps the total ops/s via per-writer pacing
    (approximate: it does not subtract the time of the op itself)."""

    def __init__(
        self,
        client: casty.Client,
        keys: list[str],
        *,
        writers: int = WRITERS,
        rate: float = RATE,
    ) -> None:
        self.acked = dict.fromkeys(keys, 0)
        self.attempted = dict.fromkeys(keys, 0)
        self._client = client
        self._keys = keys
        self._writers = writers
        self._pace = writers / rate if rate > 0 else 0.0
        self._stop = asyncio.Event()
        self._tasks: list[asyncio.Task[None]] = []
        self._started = 0.0
        self._elapsed = 0.0
        self._next = 0
        self._marks: list[tuple[str, float, int]] = []

    def start(self) -> None:
        self._started = asyncio.get_running_loop().time()
        self._marks = [("baseline", self._started, 0)]
        self._tasks = [asyncio.create_task(self._run()) for _ in range(self._writers)]

    def mark(self, label: str) -> None:
        """Close the current phase and open a new one named `label`; the report shows
        ops/s per phase instead of a global average."""
        self._marks.append((label, asyncio.get_running_loop().time(), self.total_acked))

    async def stop(self) -> None:
        self._stop.set()
        await asyncio.gather(*self._tasks)
        now = asyncio.get_running_loop().time()
        self._elapsed = now - self._started
        self._marks.append(("", now, self.total_acked))

    def report(self) -> str:
        parts = [f"{self.total_acked} acks, {self.ops_per_sec:.0f} ops/s overall"]
        for (label, t0, a0), (_, t1, a1) in zip(self._marks, self._marks[1:], strict=False):
            if t1 - t0 > 0:
                parts.append(f"{label}: {(a1 - a0) / (t1 - t0):.0f} ops/s")
        return " | ".join(parts)

    @property
    def total_acked(self) -> int:
        return sum(self.acked.values())

    @property
    def ops_per_sec(self) -> float:
        return self.total_acked / self._elapsed if self._elapsed else 0.0

    async def _run(self) -> None:
        while not self._stop.is_set():
            key = self._keys[self._next % len(self._keys)]
            self._next += 1
            self.attempted[key] += 1
            try:
                await self._client.actor(Counter, key).add(1)
                self.acked[key] += 1
            except casty.CastyError:
                await asyncio.sleep(0.1)
            if self._pace:
                await asyncio.sleep(self._pace)


async def verify_counters(
    client: casty.Client, workload: Workload, *, timeout: float = 60.0
) -> None:
    """With the load stopped: no lost ack (value >= acked) and no phantom write
    beyond the attempts (value <= attempted)."""
    for key, lo in workload.acked.items():
        hi = workload.attempted[key]

        async def check(k: str = key, lo: int = lo, hi: int = hi) -> None:
            value = await client.actor(Counter, k).read()
            assert lo <= value <= hi, f"{k}: {value} outside [{lo}, {hi}]"

        await eventually(check, timeout=timeout)


async def eventually[T](
    fn: Callable[[], Awaitable[T]], *, timeout: float = 30.0, interval: float = 0.2
) -> T:
    """Retry `fn` through the transient errors of reactivation and ownership
    moves, the same loop the in-process integration tests use."""
    deadline = asyncio.get_running_loop().time() + timeout
    while True:
        try:
            return await fn()
        except (casty.CastyError, AssertionError):
            if asyncio.get_running_loop().time() > deadline:
                raise
            await asyncio.sleep(interval)
