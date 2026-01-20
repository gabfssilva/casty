import pytest
import asyncio
from dataclasses import dataclass

from casty import actor, Mailbox, ActorSystem
from casty.serializable import serializable


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
    from casty.cluster.remote_ref import RemoteActorRef
    from casty.cluster.router import router_actor
    from casty.cluster.inbound import inbound_actor, GetPort
    from casty.cluster.outbound import outbound_actor
    from casty.cluster.transport_messages import Register, Connect

    received = []

    @actor
    async def pong_actor(*, mailbox: Mailbox[Ping]):
        async for msg, ctx in mailbox:
            received.append(msg.value)

    # Node B (receiver)
    async with ActorSystem(node_id="node-b") as system_b:
        router_b = await system_b.actor(router_actor(), name="router")
        pong_ref = await system_b.actor(pong_actor(), name="pong")
        await router_b.send(Register(ref=pong_ref))

        inbound_b = await system_b.actor(
            inbound_actor("127.0.0.1", 0, router_b),
            name="inbound"
        )
        port_b = await inbound_b.ask(GetPort())

        # Node A (sender)
        async with ActorSystem(node_id="node-a") as system_a:
            router_a = await system_a.actor(router_actor(), name="router")
            outbound_a = await system_a.actor(
                outbound_actor(router_a),
                name="outbound"
            )

            # Get connection to node-b
            conn_ref = await outbound_a.ask(Connect(
                node_id="node-b",
                address=f"127.0.0.1:{port_b}"
            ))

            # Create remote ref
            remote_pong = RemoteActorRef(
                actor_id=pong_ref.actor_id,
                node_id="node-b",
                connection=conn_ref,
            )

            # Send via remote ref
            await remote_pong.send(Ping(42))
            await asyncio.sleep(0.1)

            assert received == [42]


@pytest.mark.asyncio
async def test_remote_ref_send_with_sender():
    from casty.cluster.remote_ref import RemoteActorRef
    from casty.cluster.router import router_actor
    from casty.cluster.inbound import inbound_actor, GetPort
    from casty.cluster.outbound import outbound_actor
    from casty.cluster.transport_messages import Register, Connect

    received_senders = []

    @actor
    async def echo_actor(*, mailbox: Mailbox[Ping]):
        async for msg, ctx in mailbox:
            received_senders.append(ctx.sender_id)

    # Node B (receiver)
    async with ActorSystem(node_id="node-b") as system_b:
        router_b = await system_b.actor(router_actor(), name="router")
        echo_ref = await system_b.actor(echo_actor(), name="echo")
        await router_b.send(Register(ref=echo_ref))

        inbound_b = await system_b.actor(
            inbound_actor("127.0.0.1", 0, router_b),
            name="inbound"
        )
        port_b = await inbound_b.ask(GetPort())

        # Node A (sender)
        async with ActorSystem(node_id="node-a") as system_a:
            router_a = await system_a.actor(router_actor(), name="router")
            outbound_a = await system_a.actor(
                outbound_actor(router_a),
                name="outbound"
            )

            conn_ref = await outbound_a.ask(Connect(
                node_id="node-b",
                address=f"127.0.0.1:{port_b}"
            ))

            remote_echo = RemoteActorRef(
                actor_id=echo_ref.actor_id,
                node_id="node-b",
                connection=conn_ref,
            )

            # Send with explicit sender
            await remote_echo.send(Ping(1), sender="my-actor")
            await asyncio.sleep(0.1)

            assert received_senders == ["my-actor"]


@pytest.mark.asyncio
async def test_remote_ref_ask_without_pending_asks():
    from casty.cluster.remote_ref import RemoteActorRef
    from casty.cluster.router import router_actor
    from casty.cluster.inbound import inbound_actor, GetPort
    from casty.cluster.outbound import outbound_actor
    from casty.cluster.transport_messages import Connect

    # Node B (receiver)
    async with ActorSystem(node_id="node-b") as system_b:
        router_b = await system_b.actor(router_actor(), name="router")

        inbound_b = await system_b.actor(
            inbound_actor("127.0.0.1", 0, router_b),
            name="inbound"
        )
        port_b = await inbound_b.ask(GetPort())

        # Node A (sender)
        async with ActorSystem(node_id="node-a") as system_a:
            router_a = await system_a.actor(router_actor(), name="router")
            outbound_a = await system_a.actor(
                outbound_actor(router_a),
                name="outbound"
            )

            conn_ref = await outbound_a.ask(Connect(
                node_id="node-b",
                address=f"127.0.0.1:{port_b}"
            ))

            remote_ref = RemoteActorRef(
                actor_id="some-actor",
                node_id="node-b",
                connection=conn_ref,
            )

            with pytest.raises(RuntimeError, match="pending_asks not configured"):
                await remote_ref.ask(Ping(1))


@pytest.mark.asyncio
async def test_remote_ref_multiple_sends():
    from casty.cluster.remote_ref import RemoteActorRef
    from casty.cluster.router import router_actor
    from casty.cluster.inbound import inbound_actor, GetPort
    from casty.cluster.outbound import outbound_actor
    from casty.cluster.transport_messages import Register, Connect

    received = []

    @actor
    async def collector_actor(*, mailbox: Mailbox[Ping]):
        async for msg, ctx in mailbox:
            received.append(msg.value)

    # Node B (receiver)
    async with ActorSystem(node_id="node-b") as system_b:
        router_b = await system_b.actor(router_actor(), name="router")
        collector_ref = await system_b.actor(collector_actor(), name="collector")
        await router_b.send(Register(ref=collector_ref))

        inbound_b = await system_b.actor(
            inbound_actor("127.0.0.1", 0, router_b),
            name="inbound"
        )
        port_b = await inbound_b.ask(GetPort())

        # Node A (sender)
        async with ActorSystem(node_id="node-a") as system_a:
            router_a = await system_a.actor(router_actor(), name="router")
            outbound_a = await system_a.actor(
                outbound_actor(router_a),
                name="outbound"
            )

            conn_ref = await outbound_a.ask(Connect(
                node_id="node-b",
                address=f"127.0.0.1:{port_b}"
            ))

            remote_collector = RemoteActorRef(
                actor_id=collector_ref.actor_id,
                node_id="node-b",
                connection=conn_ref,
            )

            # Send multiple messages
            for i in range(5):
                await remote_collector.send(Ping(i))

            await asyncio.sleep(0.2)

            assert received == [0, 1, 2, 3, 4]


@pytest.mark.asyncio
async def test_remote_ref_ask():
    from casty.cluster.remote_ref import RemoteActorRef
    from casty.cluster.router import router_actor, RegisterPending
    from casty.cluster.inbound import inbound_actor, GetPort
    from casty.cluster.outbound import outbound_actor
    from casty.cluster.transport_messages import Register, Connect

    @actor
    async def echo_actor(*, mailbox: Mailbox[Ping]):
        async for msg, ctx in mailbox:
            await ctx.reply(Pong(msg.value * 2))

    # Node B (responder)
    async with ActorSystem(node_id="node-b") as system_b:
        router_b = await system_b.actor(router_actor(), name="router")
        echo_ref = await system_b.actor(echo_actor(), name="echo")
        await router_b.send(Register(ref=echo_ref))

        inbound_b = await system_b.actor(
            inbound_actor("127.0.0.1", 0, router_b),
            name="inbound"
        )
        port_b = await inbound_b.ask(GetPort())

        # Node A (requester)
        async with ActorSystem(node_id="node-a") as system_a:
            router_a = await system_a.actor(router_actor(), name="router")

            # Shared pending asks dict
            pending_asks: dict[str, asyncio.Future] = {}
            await router_a.send(RegisterPending(pending=pending_asks))

            outbound_a = await system_a.actor(
                outbound_actor(router_a),
                name="outbound"
            )

            conn_ref = await outbound_a.ask(Connect(
                node_id="node-b",
                address=f"127.0.0.1:{port_b}"
            ))

            remote_echo = RemoteActorRef(
                actor_id=echo_ref.actor_id,
                node_id="node-b",
                connection=conn_ref,
                pending_asks=pending_asks,
            )

            result = await remote_echo.ask(Ping(21))
            assert result == Pong(42)
