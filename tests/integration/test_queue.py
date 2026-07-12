from __future__ import annotations

import asyncio
from collections.abc import Callable

import pytest

import casty
from casty.errors import QuorumUnavailableError
from tests.integration.actors import partition, start_nodes, stop_all, wait_view

pytestmark = pytest.mark.asyncio


@casty.message(name="itest.QueueTicket")
class Ticket:
    code: str
    priority: int


async def test_queue_full_api_across_nodes_and_client() -> None:
    nodes = await start_nodes(3)
    client: casty.Client | None = None
    try:
        jobs: casty.Queue[str] = nodes[0].queue("jobs")
        assert await jobs.poll() is None
        assert await jobs.size() == 0

        for i in range(5):
            await jobs.offer(f"job-{i}")
        assert await jobs.size() == 5
        assert await jobs.peek() == "job-0"
        assert await jobs.size() == 5

        assert await jobs.poll() == "job-0"
        assert await jobs.poll() == "job-1"
        assert await jobs.size() == 3

        # same queue seen from another node
        jobs_b: casty.Queue[str] = nodes[1].queue("jobs")
        assert await jobs_b.peek() == "job-2"
        await jobs_b.offer("job-5")

        # and from a lite member
        from tests.integration.actors import FAST_CONFIG

        client = await casty.connect([nodes[0].member.addr], config=FAST_CONFIG)
        jobs_c: casty.Queue[str] = client.queue("jobs")
        await jobs_c.offer("job-6")
        assert await jobs_c.size() == 5

        drained = await jobs.drain(3)
        assert drained == ["job-2", "job-3", "job-4"]
        assert await jobs.size() == 2

        remaining = await jobs.drain(10)
        assert remaining == ["job-5", "job-6"]
        assert await jobs.size() == 0

        for i in range(3):
            await jobs.offer(f"final-{i}")
        await jobs.clear()
        assert await jobs.size() == 0
        assert await jobs.poll() is None
    finally:
        if client is not None:
            await client.close()
        await stop_all(nodes)


async def test_queue_message_values_and_order() -> None:
    nodes = await start_nodes(3)
    try:
        tickets: casty.Queue[Ticket] = nodes[0].queue("tickets")
        for i in range(6):
            await tickets.offer(Ticket(code=f"t-{i}", priority=i))
        polled = [await tickets.poll() for _ in range(6)]
        assert polled == [Ticket(code=f"t-{i}", priority=i) for i in range(6)]
        assert await tickets.poll() is None
    finally:
        await stop_all(nodes)


async def test_queue_order_preserved_across_interleaved_offers_from_distinct_nodes() -> None:
    nodes = await start_nodes(3)
    try:
        q_a: casty.Queue[str] = nodes[0].queue("interleaved")
        q_b: casty.Queue[str] = nodes[1].queue("interleaved")
        q_c: casty.Queue[str] = nodes[2].queue("interleaved")

        expected: list[str] = []
        for i in range(9):
            queue = [q_a, q_b, q_c][i % 3]
            item = f"item-{i}"
            await queue.offer(item)
            expected.append(item)

        polled = [await q_a.poll() for _ in range(9)]
        assert polled == expected
    finally:
        await stop_all(nodes)


async def test_queue_survives_owner_death() -> None:
    nodes = await start_nodes(3)
    try:
        jobs: casty.Queue[str] = nodes[0].queue("durable-queue")
        for i in range(10):
            await jobs.offer(f"job-{i}")
        victim = nodes[0]
        survivors = nodes[1:]
        from tests.integration.actors import kill_node

        await kill_node(victim)
        await wait_view(survivors, 2)
        jobs_b: casty.Queue[str] = survivors[0].queue("durable-queue")
        deadline = asyncio.get_running_loop().time() + 10.0
        drained: list[str | None] = []
        while True:
            try:
                drained = [await jobs_b.poll() for _ in range(10)]
                break
            except casty.CastyError:
                if asyncio.get_running_loop().time() > deadline:
                    raise
                await asyncio.sleep(0.1)
        assert drained == [f"job-{i}" for i in range(10)]
    finally:
        await stop_all(nodes)


async def test_queue_offer_fenced_in_minority() -> None:
    nodes = await start_nodes(3)
    heal: Callable[[], None] | None = None
    try:
        jobs: casty.Queue[str] = nodes[0].queue("fenced-queue")
        await jobs.offer("k")
        info = nodes[0].queue("fenced-queue")._info
        ring = nodes[0]._ring
        assert ring is not None
        owner_id = ring.owner(f"{info.wire_name}/fenced-queue:0")
        owner = next(n for n in nodes if n.node_id == owner_id)
        majority = [n for n in nodes if n is not owner]

        heal = await partition([owner], majority)
        await wait_view(majority, 2)

        owner_queue: casty.Queue[str] = owner.queue("fenced-queue")
        with pytest.raises(QuorumUnavailableError):
            await owner_queue.offer("j")

        majority_queue: casty.Queue[str] = majority[0].queue("fenced-queue")
        await majority_queue.offer("m")
        assert await majority_queue.size() == 2
    finally:
        if heal is not None:
            heal()
        await stop_all(nodes)
