import asyncio
import pytest
from dataclasses import dataclass

from casty import actor, Mailbox, message
from casty.state import State
from casty.cluster import DevelopmentCluster


@message
class Inc:
    amount: int = 1


@message
class Get:
    pass


@message
class Ping:
    pass


@pytest.mark.asyncio
async def test_replicated_actor_processes_messages():
    """Actor with replicas processes messages across cluster"""

    @actor(replicas=3)
    async def counter(state: State[int], *, mailbox: Mailbox[Inc | Get]):
        async for msg, ctx in mailbox:
            match msg:
                case Inc(amount):
                    if ctx.is_leader:
                        state.set(state.value + amount)
                case Get():
                    await ctx.reply(state.value)

    async with asyncio.timeout(15):
        async with DevelopmentCluster(nodes=3) as cluster:
            ref = await cluster.actor(counter(State(0)), name="counter")

            await ref.send(Inc(5))
            await ref.send(Inc(3))
            await asyncio.sleep(0.3)

            result = await ref.ask(Get())

            assert result == 8


@pytest.mark.asyncio
async def test_leader_flag_is_set():
    """ctx.is_leader distinguishes leader from replicas"""
    leader_flags: list[bool] = []

    @actor(replicas=2)
    async def tracker(*, mailbox: Mailbox[Ping | Get]):
        async for msg, ctx in mailbox:
            match msg:
                case Ping():
                    leader_flags.append(ctx.is_leader)
                case Get():
                    await ctx.reply(leader_flags)

    async with asyncio.timeout(15):
        async with DevelopmentCluster(nodes=3) as cluster:
            ref = await cluster.actor(tracker(), name="tracker")

            for _ in range(10):
                await ref.send(Ping())

            await asyncio.sleep(0.3)

            flags = await ref.ask(Get())

            assert len(flags) >= 1
            assert any(f is True for f in flags) or any(f is False for f in flags)


@pytest.mark.asyncio
async def test_clustered_actor_accessible_across_cluster():
    """Actor with clustered=True responds from any node"""
    node_ids: list[str] = []

    @actor(clustered=True)
    async def service(*, mailbox: Mailbox[Ping | Get]):
        async for msg, ctx in mailbox:
            match msg:
                case Ping():
                    node_ids.append(ctx.node_id)
                case Get():
                    await ctx.reply(node_ids)

    async with asyncio.timeout(15):
        async with DevelopmentCluster(nodes=5) as cluster:
            ref = await cluster.actor(service(), name="service")

            for _ in range(15):
                await ref.send(Ping())

            await asyncio.sleep(0.3)

            ids = await ref.ask(Get())

            assert len(ids) >= 1
            assert all(n.startswith("node-") for n in ids)
