import pytest
import asyncio
from dataclasses import dataclass

from casty import actor, Mailbox, State
from casty.serializable import serializable
from casty.cluster.development import DevelopmentCluster
from casty.reply import Reply


@serializable
@dataclass
class Ping:
    value: int


@serializable
@dataclass
class Pong:
    value: int


@pytest.mark.asyncio
async def test_remote_ref_send():
    received = []

    @actor
    async def pong_actor(*, mailbox: Mailbox[Ping]):
        async for msg, ctx in mailbox:
            received.append(msg.value)

    async with DevelopmentCluster(nodes=2) as cluster:
        node_a, node_b = cluster.nodes

        pong_ref = await node_b.actor(pong_actor(), name="pong")

        # Get remote ref via system.actor lookup
        remote_pong = await node_a.actor(name=pong_ref.actor_id, node_id=node_b.node_id)

        await remote_pong.send(Ping(42))
        await asyncio.sleep(0.1)

        assert received == [42]


@pytest.mark.asyncio
async def test_remote_ref_send_with_sender():
    received_senders = []

    @actor
    async def echo_actor(*, mailbox: Mailbox[Ping]):
        async for msg, ctx in mailbox:
            received_senders.append(ctx.sender.actor_id if ctx.sender else None)

    @actor
    async def sender_actor(*, mailbox: Mailbox[Pong]):
        async for msg, ctx in mailbox:
            pass

    async with DevelopmentCluster(nodes=2) as cluster:
        node_a, node_b = cluster.nodes

        echo_ref = await node_b.actor(echo_actor(), name="echo")
        sender_ref = await node_a.actor(sender_actor(), name="sender")

        remote_echo = await node_a.actor(name=echo_ref.actor_id, node_id=node_b.node_id)

        await remote_echo.send(Ping(1), sender=sender_ref)
        await asyncio.sleep(0.1)

        assert len(received_senders) == 1


@pytest.mark.asyncio
async def test_remote_ref_multiple_sends():
    received = []

    @actor
    async def collector_actor(*, mailbox: Mailbox[Ping]):
        async for msg, ctx in mailbox:
            received.append(msg.value)

    async with DevelopmentCluster(nodes=2) as cluster:
        node_a, node_b = cluster.nodes

        collector_ref = await node_b.actor(collector_actor(), name="collector")
        remote_collector = await node_a.actor(name=collector_ref.actor_id, node_id=node_b.node_id)

        for i in range(5):
            await remote_collector.send(Ping(i))

        await asyncio.sleep(0.2)

        assert received == [0, 1, 2, 3, 4]


@pytest.mark.asyncio
async def test_remote_ref_ask():
    @actor
    async def echo_actor(state: State[int],*, mailbox: Mailbox[Ping | Reply]):
        async for msg, ctx in mailbox:
            state.set(state.value + 1)

            print(f'\nhello, who sent to me: {ctx.sender.actor_id}. i am {ctx.node_id}')

            match msg:
                case Ping(value):
                    await ctx.reply(Pong(value * state.value))

    async with DevelopmentCluster(nodes=2, debug=True) as cluster:
        node_a, node_b = cluster.nodes

        echo_ref = await node_a.actor(echo_actor(0), name="echo", write_quorum=2, replicas=2)
        echo_ref_b = await node_b.actor(echo_actor(0), name="echo", write_quorum=2, replicas=2)

        result_a = await echo_ref.ask(Ping(21))
        result_b = await echo_ref_b.ask(Ping(21))

        assert result_a == Pong(21)
        assert result_b == Pong(42)
