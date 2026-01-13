"""Tests for cross-node messaging."""

import asyncio
from dataclasses import dataclass

import pytest

from casty import Actor, Context


@dataclass
class Increment:
    amount: int = 1


@dataclass
class GetValue:
    pass


class Counter(Actor[Increment | GetValue]):
    """Simple counter actor for testing."""

    def __init__(self, initial: int = 0) -> None:
        self.value = initial

    async def receive(self, msg: Increment | GetValue, ctx: Context) -> None:
        match msg:
            case Increment(amount):
                self.value += amount
            case GetValue():
                ctx.reply(self.value)


pytestmark = pytest.mark.slow


class TestRemoteMessaging:
    """Tests for cross-node messaging."""

    async def test_two_nodes_communicate(self, two_node_cluster) -> None:
        server, client = two_node_cluster

        await server.system.spawn(Counter, name="counter", initial=0)

        remote_counter = await client.system.lookup("counter")
        await remote_counter.send(Increment(10))
        await asyncio.sleep(0.1)

        local_counter = await server.system.lookup("counter")
        value = await local_counter.ask(GetValue())
        assert value == 10

    async def test_multiple_messages_remote(self, two_node_cluster) -> None:
        server, client = two_node_cluster

        await server.system.spawn(Counter, name="counter")

        remote = await client.system.lookup("counter")
        for _ in range(5):
            await remote.send(Increment(1))
        await asyncio.sleep(0.2)

        local = await server.system.lookup("counter")
        value = await local.ask(GetValue())
        assert value == 5

    async def test_third_node_sees_updated_state(self, distributed_node) -> None:
        """A third node connects later and can read the updated counter value."""
        server = await distributed_node(expected_cluster_size=3)
        client = await distributed_node(
            seeds=[f"127.0.0.1:{server.port}"],
            expected_cluster_size=3,
        )

        await server.system.spawn(Counter, name="counter", initial=0)

        remote_counter = await client.system.lookup("counter")
        await remote_counter.send(Increment(10))
        await asyncio.sleep(0.1)

        # Third node joins late
        observer = await distributed_node(
            seeds=[f"127.0.0.1:{server.port}"],
            expected_cluster_size=3,
        )

        remote = await observer.system.lookup("counter")
        value = await remote.ask(GetValue())
        assert value == 10
