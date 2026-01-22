import pytest
import asyncio
from dataclasses import dataclass

from casty import actor, Mailbox
from casty.cluster.snapshot import InMemory
from casty.actor_config import Routing


@dataclass
class Increment:
    amount: int


@dataclass
class Get:
    pass


@dataclass
class Put:
    pass


class TestActorReplicationConfig:
    def test_actor_default_no_replication(self):
        @actor
        async def simple(*, mailbox: Mailbox[str]):
            pass

        behavior = simple()
        assert not hasattr(behavior.func, "__replication_config__")

    def test_actor_clustered_only(self):
        @actor(clustered=True)
        async def clustered_actor(*, mailbox: Mailbox[str]):
            pass

        behavior = clustered_actor()
        config = behavior.func.__replication_config__
        assert config.clustered is True
        assert config.replicated is None

    def test_actor_replicated(self):
        @actor(replicated=3)
        async def replicated_actor(*, mailbox: Mailbox[str]):
            pass

        behavior = replicated_actor()
        config = behavior.func.__replication_config__
        assert config.replicated == 3
        assert config.clustered is True  # implied

    def test_actor_replicated_with_persistence(self):
        backend = InMemory()

        @actor(replicated=3, persistence=backend)
        async def persistent_actor(*, mailbox: Mailbox[str]):
            pass

        behavior = persistent_actor()
        config = behavior.func.__replication_config__
        assert config.replicated == 3
        assert config.persistence is backend

    def test_actor_replicated_with_routing(self):
        @actor(replicated=3, routing={Get: Routing.ANY, Put: Routing.LEADER})
        async def routed_actor(*, mailbox: Mailbox[Get | Put]):
            pass

        behavior = routed_actor()
        config = behavior.func.__replication_config__
        assert config.routing[Get] == Routing.ANY
        assert config.routing[Put] == Routing.LEADER

    def test_actor_persistence_without_replication(self):
        backend = InMemory()

        @actor(persistence=backend)
        async def local_persistent(*, mailbox: Mailbox[str]):
            pass

        behavior = local_persistent()
        config = behavior.func.__replication_config__
        assert config.persistence is backend
        assert config.replicated is None


def test_actor_decorator_creates_behavior():
    from casty.actor import actor, Behavior
    from casty.mailbox import Mailbox

    @actor
    async def counter(initial: int, *, mailbox: Mailbox[Increment | Get]):
        count = initial
        async for msg, ctx in mailbox:
            match msg:
                case Increment(amount):
                    count += amount
                case Get():
                    await ctx.reply(count)

    # Calling counter(0) returns a Behavior, not a coroutine
    behavior = counter(0)
    assert isinstance(behavior, Behavior)
    assert behavior.initial_args == (0,)
    assert behavior.initial_kwargs == {}


def test_behavior_stores_function():
    from casty.actor import actor, Behavior
    from casty.mailbox import Mailbox

    @actor
    async def simple(*, mailbox: Mailbox[str]):
        async for msg, ctx in mailbox:
            pass

    behavior = simple()
    assert behavior.func is not None
    assert callable(behavior.func)


@pytest.mark.asyncio
async def test_behavior_can_be_started():
    from casty.actor import actor
    from casty.mailbox import Mailbox, ActorMailbox, Stop
    from casty.envelope import Envelope

    results = []

    @actor
    async def collector(prefix: str, *, mailbox: Mailbox[str]):
        async for msg, ctx in mailbox:
            results.append(f"{prefix}:{msg}")

    behavior = collector("test")

    mailbox = ActorMailbox(self_id="collector/c1")

    task = asyncio.create_task(behavior.func("test", mailbox=mailbox))

    await mailbox.put(Envelope("hello"))
    await mailbox.put(Envelope("world"))
    await mailbox.put(Envelope(Stop()))

    await task

    assert results == ["test:hello", "test:world"]
