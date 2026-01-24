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
        assert config.replicas == 1  # defaults to 1 when clustered

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
    from casty.state import State

    @actor
    async def counter(state: State[int], *, mailbox: Mailbox[Increment | Get]):
        async for msg, ctx in mailbox:
            match msg:
                case Increment(amount):
                    state.value += amount
                case Get():
                    await ctx.reply(state.value)

    behavior = counter(State(0))
    assert isinstance(behavior, Behavior)
    assert behavior.state_initial is not None
    assert behavior.state_initial.value == 0
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
    from casty.state import State

    results = []

    @actor
    async def collector(state: State[int], *, mailbox: Mailbox[str]):
        async for msg, ctx in mailbox:
            state.value += 1
            results.append(f"{state.value}:{msg}")

    behavior = collector(State(0))

    mailbox = ActorMailbox(self_id="collector/c1")
    state = behavior.state_initial

    task = asyncio.create_task(behavior.func(state=state, mailbox=mailbox))

    await mailbox.put(Envelope("hello"))
    await mailbox.put(Envelope("world"))
    await mailbox.put(Envelope(Stop()))

    await task

    assert results == ["1:hello", "2:world"]


def test_actor_with_explicit_state():
    from casty import actor, Mailbox
    from casty.state import State

    @actor
    async def counter(state: State[int], *, mailbox: Mailbox[str]):
        pass

    behavior = counter(State(0))
    assert behavior.state_initial is not None
    assert behavior.state_initial.value == 0


