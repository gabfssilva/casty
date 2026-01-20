import pytest
import asyncio
from dataclasses import dataclass

from casty import actor, Mailbox
from casty.serializable import serializable
from casty.cluster import ClusteredActorSystem


@serializable
@dataclass
class Inc:
    amount: int


@serializable
@dataclass
class Get:
    pass


@actor
async def counter(initial: int, *, mailbox: Mailbox[Inc | Get]):
    count = initial
    async for msg, ctx in mailbox:
        match msg:
            case Inc(amount):
                count += amount
            case Get():
                await ctx.reply(count)


@pytest.mark.asyncio
async def test_clustered_system_basic():

    async with ClusteredActorSystem(
        node_id="node-1",
        host="127.0.0.1",
        port=0,
    ) as system:
        ref = await system.actor(counter(0), name="counter")

        await ref.send(Inc(5))
        await asyncio.sleep(0.05)
        result = await ref.ask(Get())

        assert result == 5


@pytest.mark.asyncio
async def test_clustered_system_multiple_actors():
    async with ClusteredActorSystem(
        node_id="node-1",
        host="127.0.0.1",
        port=0,
    ) as system:
        ref1 = await system.actor(counter(0), name="counter1")
        ref2 = await system.actor(counter(100), name="counter2")

        await ref1.send(Inc(10))
        await ref2.send(Inc(20))
        await asyncio.sleep(0.05)

        result1 = await ref1.ask(Get())
        result2 = await ref2.ask(Get())

        assert result1 == 10
        assert result2 == 120


@pytest.mark.asyncio
async def test_clustered_system_get_or_create():
    async with ClusteredActorSystem(
        node_id="node-1",
        host="127.0.0.1",
        port=0,
    ) as system:
        ref1 = await system.actor(counter(0), name="counter")
        await ref1.send(Inc(5))
        await asyncio.sleep(0.05)

        ref2 = await system.actor(counter(100), name="counter")

        assert ref1 is ref2

        result = await ref2.ask(Get())
        assert result == 5


@pytest.mark.asyncio
async def test_clustered_system_port_property():
    async with ClusteredActorSystem(
        node_id="node-1",
        host="127.0.0.1",
        port=0,
    ) as system:
        assert system.port > 0
        assert isinstance(system.port, int)


@pytest.mark.asyncio
async def test_clustered_system_node_id_property():
    async with ClusteredActorSystem(
        node_id="my-node",
        host="127.0.0.1",
        port=0,
    ) as system:
        assert system.node_id == "my-node"


@pytest.mark.asyncio
async def test_clustered_system_address_property():
    async with ClusteredActorSystem(
        node_id="node-1",
        host="127.0.0.1",
        port=0,
    ) as system:
        assert system.address.startswith("127.0.0.1:")
        assert int(system.address.split(":")[1]) > 0


@pytest.mark.asyncio
async def test_two_nodes_connect():
    async with ClusteredActorSystem(
        node_id="node-1",
        host="127.0.0.1",
        port=0,
    ) as system1:
        async with ClusteredActorSystem(
            node_id="node-2",
            host="127.0.0.1",
            port=0,
        ) as system2:
            await system2.connect_to(system1.address)
            await asyncio.sleep(0.1)


@pytest.mark.asyncio
async def test_clustered_system_with_seeds():
    async with ClusteredActorSystem(
        node_id="seed-node",
        host="127.0.0.1",
        port=0,
    ) as seed_system:
        async with ClusteredActorSystem(
            node_id="joining-node",
            host="127.0.0.1",
            port=0,
            seeds=[seed_system.address],
        ) as joining_system:
            await asyncio.sleep(0.1)
            assert joining_system.node_id == "joining-node"
