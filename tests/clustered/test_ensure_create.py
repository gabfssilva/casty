import asyncio
import pytest
from dataclasses import dataclass

from casty import actor, Mailbox
from casty.cluster import DevelopmentCluster
from casty.serializable import serializable


@serializable
@dataclass
class Ping:
    pass


@serializable
@dataclass
class Pong:
    node: str


@actor
async def ping_actor(*, mailbox: Mailbox[Ping]):
    async for msg, ctx in mailbox:
        match msg:
            case Ping():
                await ctx.reply(Pong(node=ctx.node_id))


@pytest.mark.asyncio
async def test_ensure_create_on_responsible_node():
    async with asyncio.timeout(15):
        async with DevelopmentCluster(3) as cluster:
            await cluster.wait_for(3)

            ref = await cluster[0].actor(ping_actor(), name="ensure-test")

            result = await ref.ask(Ping())
            assert isinstance(result, Pong)


@pytest.mark.asyncio
async def test_multiple_creates_same_actor():
    async with asyncio.timeout(15):
        async with DevelopmentCluster(2) as cluster:
            await cluster.wait_for(2)

            ref0 = await cluster[0].actor(ping_actor(), name="multi-create")
            ref1 = await cluster[1].actor(ping_actor(), name="multi-create")

            result0 = await ref0.ask(Ping())
            result1 = await ref1.ask(Ping())

            assert isinstance(result0, Pong)
            assert isinstance(result1, Pong)
