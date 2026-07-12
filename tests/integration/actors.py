"""Actor fixtures shared by the etapa 4+ integration tests: fast configs, test
actor classes, and a helper to start a real cluster of Nodes on localhost."""

from __future__ import annotations

import asyncio
import dataclasses
from collections.abc import AsyncIterator, Callable

import casty
from casty.errors import ConnectionLostError
from casty.node import Config
from tests.integration.cluster import FAST_MEMBERSHIP, FAST_TRANSPORT, free_port

FAST_CONFIG = Config(
    transport=FAST_TRANSPORT,
    membership=FAST_MEMBERSHIP,
    call_timeout=5.0,
    default_idle_timeout=30.0,
    drain_timeout=5.0,
    client_sync_interval=0.5,
)


@casty.actor(name="itest.Counter")
class Counter:
    value: int = 0

    async def add(self, n: int) -> int:
        self.value += n
        return self.value

    async def read(self) -> int:
        return self.value


@casty.actor(name="itest.Lifecycle", idle_timeout=0.3)
class Lifecycle:
    activations: int = 0

    @casty.activate
    async def _up(self) -> None:
        self.activations += 1
        events.append(f"activate:{casty.context().key}")

    @casty.deactivate
    async def _down(self) -> None:
        events.append(f"deactivate:{casty.context().key}")

    async def touch(self) -> int:
        return self.activations

    async def retire(self) -> None:
        casty.context().deactivate()


events: list[str] = []


@casty.actor(name="itest.Flaky")
class Flaky:
    value: int = 0

    async def set(self, n: int) -> None:
        self.value = n

    async def get(self) -> int:
        return self.value

    async def boom(self) -> None:
        raise RuntimeError("kaboom")


@casty.actor(name="itest.Log")
class Log:
    entries: list[bytes] = dataclasses.field(default_factory=list)

    async def tail(self, since: int) -> AsyncIterator[bytes]:
        for e in self.entries[since:]:
            yield e

    async def ingest(self, items: AsyncIterator[bytes]) -> int:
        n = 0
        async for item in items:
            self.entries.append(item)
            n += 1
        return n

    async def transform(self, xs: AsyncIterator[bytes]) -> AsyncIterator[int]:
        async for x in xs:
            yield len(x)

    async def count_up(self, n: int) -> AsyncIterator[int]:
        for i in range(n):
            yield i

    async def size(self) -> int:
        return len(self.entries)

    async def boom(self) -> AsyncIterator[int]:
        yield 1
        raise RuntimeError("kaboom")


@casty.actor(name="itest.Chain")
class ChainActor:
    async def ping(self, hops: list[str]) -> str:
        if not hops:
            return casty.context().key
        head, *rest = hops
        peer = casty.context().actor(ChainActor, head)
        return await peer.ping(rest)


@casty.service(name="itest.Fetcher")
class Fetcher:
    async def slow(self, seconds: float, n: int) -> int:
        await asyncio.sleep(seconds)  # stands in for I/O: N calls must overlap
        return n * 2

    async def bump(self, key: str, n: int) -> int:
        return await casty.context().actor(Counter, key).add(n)

    async def boom(self) -> int:
        raise RuntimeError("kaboom")


async def start_nodes(
    n: int, *, config: Config = FAST_CONFIG, supervisor: casty.Supervisor | None = None
) -> list[casty.Node]:
    first = await casty.start(
        f"127.0.0.1:{free_port()}", config=config, supervisor=supervisor
    )
    seeds = [first.member.addr]
    systems = [first]
    for _ in range(n - 1):
        systems.append(
            await casty.start(
                f"127.0.0.1:{free_port()}", seeds=seeds, config=config, supervisor=supervisor
            )
        )
    await wait_view(systems, n)
    return systems


async def wait_view(systems: list[casty.Node], expected: int, timeout: float = 10.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if all(len(system.membership.alive_members()) == expected for system in systems):
            return
        await asyncio.sleep(0.05)
    sizes = [len(system.membership.alive_members()) for system in systems]
    raise AssertionError(f"views did not reach {expected} members: {sizes}")


async def stop_all(systems: list[casty.Node]) -> None:
    await asyncio.gather(*(system.close() for system in systems), return_exceptions=True)


async def kill_node(system: casty.Node) -> None:
    """Abrupt death: no drain, no leave broadcast."""
    await system.membership.stop()
    server = system._server
    assert server is not None
    server.stop_accepting()
    await system._pool.close()
    await server.close()


def owner_of(systems: list[casty.Node], wire: str, key: str) -> casty.Node:
    ring = systems[0]._ring
    assert ring is not None
    owner_id = ring.owner(f"{wire}/{key}")
    return next(n for n in systems if n.node_id == owner_id)


async def partition(
    group_a: list[casty.Node], group_b: list[casty.Node]
) -> Callable[[], None]:
    """In-process network partition: dials across groups fail, existing
    connections drop. Returns the heal function."""
    restores: list[Callable[[], None]] = []

    def block(system: casty.Node, blocked: set[str]) -> None:
        pool = system._pool
        original = pool.get

        async def blocked_get(addr: str, *, max_attempts: int = 1) -> object:
            if addr in blocked:
                raise ConnectionLostError(f"partitioned from {addr}")
            return await original(addr, max_attempts=max_attempts)

        setattr(pool, "get", blocked_get)  # noqa: B010 - test harness
        restores.append(lambda: setattr(pool, "get", original))

    addrs_a = {n.member.addr for n in group_a}
    addrs_b = {n.member.addr for n in group_b}
    for system in group_a:
        block(system, addrs_b)
    for system in group_b:
        block(system, addrs_a)

    for system in group_a:
        for other in group_b:
            conn = system._pool.by_node(other.node_id)
            if conn is not None:
                await conn.close()
            back = other._pool.by_node(system.node_id)
            if back is not None:
                await back.close()

    def heal() -> None:
        for restore in restores:
            restore()

    return heal
