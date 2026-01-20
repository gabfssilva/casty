from __future__ import annotations

import pytest
import asyncio
from dataclasses import dataclass

from casty import actor, Mailbox
from casty.state import State
from casty.serializable import serializable
from casty.cluster.replication import replicated, Routing
from casty.cluster import DevelopmentCluster, ClusteredActorSystem


@serializable
@dataclass
class Get:
    pass


@serializable
@dataclass
class Set:
    value: int


@pytest.mark.asyncio
async def test_replicated_actor_spawns_with_state():
    @replicated(factor=2, write_quorum=1, routing=Routing.LEADER)
    @actor
    async def counter(state: State[int], *, mailbox: Mailbox[Get | Set]):
        async for msg, ctx in mailbox:
            match msg:
                case Get():
                    await ctx.reply(state.value)
                case Set(value):
                    state.set(value)

    async with DevelopmentCluster(nodes=1) as cluster:
        ref = await cluster.nodes[0].actor(counter(0), name="test")

        await ref.send(Set(42))
        await asyncio.sleep(0.05)
        result = await ref.ask(Get())

        assert result == 42


@pytest.mark.asyncio
async def test_replicated_actor_is_leader_on_single_node():
    @replicated(factor=2, write_quorum=1, routing=Routing.LEADER)
    @actor
    async def storage(state: State[str], *, mailbox: Mailbox[Get | Set]):
        async for msg, ctx in mailbox:
            match msg:
                case Get():
                    await ctx.reply(f"leader={ctx.is_leader}")
                case Set(value):
                    state.set(str(value))

    async with DevelopmentCluster(nodes=1) as cluster:
        ref = await cluster.nodes[0].actor(storage(""), name="test")

        result = await ref.ask(Get())
        assert result == "leader=True"


@pytest.mark.asyncio
async def test_replicated_actor_with_runtime_config():
    @actor
    async def counter(state: State[int], *, mailbox: Mailbox[Get | Set]):
        async for msg, ctx in mailbox:
            match msg:
                case Get():
                    await ctx.reply(state.value)
                case Set(value):
                    state.set(value)

    async with DevelopmentCluster(nodes=1) as cluster:
        ref = await cluster.nodes[0].actor(
            counter(10),
            name="runtime-config",
            replicas=2,
            write_quorum=1,
            routing=Routing.LEADER,
        )

        result = await ref.ask(Get())
        assert result == 10

        await ref.send(Set(100))
        await asyncio.sleep(0.05)
        result = await ref.ask(Get())
        assert result == 100


@pytest.mark.asyncio
async def test_replicated_actor_state_increments_version():
    @replicated(factor=1, write_quorum=1, routing=Routing.LEADER)
    @actor
    async def versioned(state: State[int], *, mailbox: Mailbox[Get | Set]):
        async for msg, ctx in mailbox:
            match msg:
                case Get():
                    await ctx.reply({"value": state.value, "version": state.version})
                case Set(value):
                    state.set(value)

    async with ClusteredActorSystem(
        node_id="test-node",
        host="127.0.0.1",
        port=0,
    ) as system:
        ref = await system.actor(versioned(0), name="versioned")

        result = await ref.ask(Get())
        assert result == {"value": 0, "version": 0}

        await ref.send(Set(1))
        await asyncio.sleep(0.05)
        result = await ref.ask(Get())
        assert result == {"value": 1, "version": 1}

        await ref.send(Set(2))
        await asyncio.sleep(0.05)
        result = await ref.ask(Get())
        assert result == {"value": 2, "version": 2}


@pytest.mark.asyncio
async def test_non_replicated_actor_still_works():
    @actor
    async def simple_counter(initial: int, *, mailbox: Mailbox[Get | Set]):
        count = initial
        async for msg, ctx in mailbox:
            match msg:
                case Get():
                    await ctx.reply(count)
                case Set(value):
                    count = value

    async with ClusteredActorSystem(
        node_id="test-node",
        host="127.0.0.1",
        port=0,
    ) as system:
        ref = await system.actor(simple_counter(5), name="simple")

        result = await ref.ask(Get())
        assert result == 5

        await ref.send(Set(20))
        await asyncio.sleep(0.05)
        result = await ref.ask(Get())
        assert result == 20


@pytest.mark.asyncio
async def test_get_or_create_replicated_actor():
    @replicated(factor=2, write_quorum=1, routing=Routing.LEADER)
    @actor
    async def counter(state: State[int], *, mailbox: Mailbox[Get | Set]):
        async for msg, ctx in mailbox:
            match msg:
                case Get():
                    await ctx.reply(state.value)
                case Set(value):
                    state.set(value)

    async with ClusteredActorSystem(
        node_id="test-node",
        host="127.0.0.1",
        port=0,
    ) as system:
        ref1 = await system.actor(counter(0), name="same")
        await ref1.send(Set(42))
        await asyncio.sleep(0.05)

        ref2 = await system.actor(counter(100), name="same")

        assert ref1 is ref2

        result = await ref2.ask(Get())
        assert result == 42


@pytest.mark.asyncio
async def test_replicated_actor_hash_ring_assigns_leader():
    @replicated(factor=3, write_quorum=2, routing=Routing.LEADER)
    @actor
    async def replicated_actor(state: State[str], *, mailbox: Mailbox[Get | Set]):
        async for msg, ctx in mailbox:
            match msg:
                case Get():
                    await ctx.reply(ctx.is_leader)
                case Set(value):
                    state.set(str(value))

    async with ClusteredActorSystem(
        node_id="node-0",
        host="127.0.0.1",
        port=0,
    ) as system:
        ref = await system.actor(replicated_actor(""), name="hash-test")
        is_leader = await ref.ask(Get())
        assert is_leader is True
