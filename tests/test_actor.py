import pytest
import asyncio
from dataclasses import dataclass

from casty import actor, Mailbox


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
        config = behavior.__replication_config__
        assert config.clustered is True
        assert config.replicas == 2  # defaults to 2 when clustered

    def test_actor_replicated(self):
        @actor(replicas=3)
        async def replicated_actor(*, mailbox: Mailbox[str]):
            pass

        behavior = replicated_actor()
        config = behavior.__replication_config__
        assert config.replicas == 3
        assert config.clustered is True  # implied

def test_actor_decorator_creates_behavior():
    from casty.actor import actor, Behavior
    from casty.mailbox import Mailbox

    @actor
    async def counter(count: int, *, mailbox: Mailbox[Increment | Get]):
        async for msg, ctx in mailbox:
            match msg:
                case Increment(amount):
                    count += amount
                case Get():
                    await ctx.reply(count)

    # Calling counter(0) returns a Behavior, not a coroutine
    behavior = counter(0)
    assert isinstance(behavior, Behavior)
    assert behavior.state_initials == [0]
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
    from types import SimpleNamespace
    from casty.actor import actor
    from casty.mailbox import Mailbox, ActorMailbox, Stop
    from casty.envelope import Envelope

    results = []

    @actor
    async def collector(count: int, *, mailbox: Mailbox[str]):
        async for msg, ctx in mailbox:
            count += 1
            results.append(f"{count}:{msg}")

    behavior = collector(0)

    mailbox = ActorMailbox(self_id="collector/c1")
    state_ns = SimpleNamespace(count=0)

    task = asyncio.create_task(behavior.func(state_ns, mailbox=mailbox))

    await mailbox.put(Envelope("hello"))
    await mailbox.put(Envelope("world"))
    await mailbox.put(Envelope(Stop()))

    await task

    assert results == ["1:hello", "2:world"]


