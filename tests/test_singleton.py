"""Tests for singleton actors."""

import asyncio
from dataclasses import dataclass

import pytest

from casty import Actor, ActorSystem, Context
from casty.cluster import DevelopmentCluster
from casty.cluster.singleton_ref import SingletonRef


@dataclass(frozen=True)
class GetNodeId:
    pass


@dataclass(frozen=True)
class Increment:
    pass


@dataclass(frozen=True)
class GetCount:
    pass


class SingletonCounter(Actor[Increment | GetCount | GetNodeId]):
    """Simple counter for testing singletons."""

    def __init__(self):
        self.count = 0
        self.node_id = "unknown"

    async def on_start(self) -> None:
        if hasattr(self._ctx, "system") and hasattr(self._ctx.system, "node_id"):
            self.node_id = self._ctx.system.node_id or "unknown"

    async def receive(self, msg: Increment | GetCount | GetNodeId, ctx: Context) -> None:
        match msg:
            case Increment():
                self.count += 1
            case GetCount():
                ctx.reply(self.count)
            case GetNodeId():
                ctx.reply(self.node_id)


class TestSingletonSpawn:
    """Test spawning singleton actors."""

    @pytest.mark.asyncio
    async def test_spawn_singleton_returns_singleton_ref(self):
        """spawn with singleton=True should return SingletonRef."""
        async with ActorSystem.clustered("127.0.0.1", 0) as system:
            await asyncio.sleep(0.3)

            counter = await system.spawn(
                SingletonCounter, name="counter", singleton=True
            )

            assert isinstance(counter, SingletonRef)

    @pytest.mark.asyncio
    async def test_spawn_singleton_requires_name(self):
        """spawn with singleton=True should require a name."""
        async with ActorSystem.clustered("127.0.0.1", 0) as system:
            await asyncio.sleep(0.3)

            with pytest.raises(ValueError, match="require a name"):
                await system.spawn(SingletonCounter, singleton=True)

    @pytest.mark.asyncio
    async def test_spawn_singleton_excludes_sharded(self):
        """Cannot be both singleton and sharded."""
        async with ActorSystem.clustered("127.0.0.1", 0) as system:
            await asyncio.sleep(0.3)

            with pytest.raises(ValueError, match="both singleton and sharded"):
                await system.spawn(
                    SingletonCounter, name="x", singleton=True, sharded=True
                )


class TestSingletonMessaging:
    """Test sending messages to singletons."""

    @pytest.mark.asyncio
    async def test_singleton_send(self):
        """Should be able to send messages to singleton."""
        async with ActorSystem.clustered("127.0.0.1", 0) as system:
            await asyncio.sleep(0.3)

            counter = await system.spawn(
                SingletonCounter, name="counter", singleton=True
            )

            await counter.send(Increment())
            await counter.send(Increment())
            await asyncio.sleep(0.1)

            count = await counter.ask(GetCount())
            assert count == 2

    @pytest.mark.asyncio
    async def test_singleton_ask(self):
        """Should be able to ask singleton and get response."""
        async with ActorSystem.clustered("127.0.0.1", 0) as system:
            await asyncio.sleep(0.3)

            counter = await system.spawn(
                SingletonCounter, name="counter", singleton=True
            )

            count = await counter.ask(GetCount())
            assert count == 0


class TestSingletonMultiNode:
    """Test singletons across multiple nodes."""

    @pytest.mark.asyncio
    async def test_singleton_only_one_instance(self):
        """Only one singleton instance should exist across cluster."""
        async with DevelopmentCluster(2) as (system1, system2):
            await asyncio.sleep(0.5)

            # Both nodes spawn "same" singleton
            counter1 = await system1.spawn(
                SingletonCounter, name="global-counter", singleton=True
            )
            await asyncio.sleep(0.2)

            counter2 = await system2.spawn(
                SingletonCounter, name="global-counter", singleton=True
            )
            await asyncio.sleep(0.2)

            # Increment from both nodes
            await counter1.send(Increment())
            await counter2.send(Increment())
            await asyncio.sleep(0.3)

            # Both should see same count (messages went to same actor)
            count1 = await counter1.ask(GetCount())
            count2 = await counter2.ask(GetCount())

            assert count1 == 2
            assert count2 == 2


class TestSingletonRef:
    """Test SingletonRef interface."""

    @pytest.mark.asyncio
    async def test_singleton_ref_repr(self):
        """SingletonRef should have a nice repr."""
        async with ActorSystem.clustered("127.0.0.1", 0) as system:
            await asyncio.sleep(0.3)

            counter = await system.spawn(
                SingletonCounter, name="test-counter", singleton=True
            )

            assert "test-counter" in repr(counter)

    @pytest.mark.asyncio
    async def test_singleton_ref_name_property(self):
        """SingletonRef should expose name property."""
        async with ActorSystem.clustered("127.0.0.1", 0) as system:
            await asyncio.sleep(0.3)

            counter = await system.spawn(
                SingletonCounter, name="my-singleton", singleton=True
            )

            assert counter.name == "my-singleton"

    @pytest.mark.asyncio
    async def test_singleton_ref_equality(self):
        """SingletonRefs with same name should be equal."""
        async with ActorSystem.clustered("127.0.0.1", 0) as system:
            await asyncio.sleep(0.3)

            counter1 = await system.spawn(
                SingletonCounter, name="equality-test", singleton=True
            )
            counter2 = await system.spawn(
                SingletonCounter, name="equality-test", singleton=True
            )

            assert counter1 == counter2
            assert hash(counter1) == hash(counter2)

    @pytest.mark.asyncio
    async def test_singleton_ref_operators(self):
        """SingletonRef should support >> and << operators."""
        async with ActorSystem.clustered("127.0.0.1", 0) as system:
            await asyncio.sleep(0.3)

            counter = await system.spawn(
                SingletonCounter, name="op-test", singleton=True
            )

            # Use >> for send
            await (counter >> Increment())
            await (counter >> Increment())
            await asyncio.sleep(0.1)

            # Use << for ask
            count = await (counter << GetCount())
            assert count == 2
