import asyncio
import pytest
from dataclasses import dataclass
from casty import Actor, Context
from casty.cluster import DevelopmentCluster
from casty.persistent_actor import GetState, GetCurrentVersion


@dataclass
class Increment:
    amount: int


@dataclass
class GetCount:
    pass


@dataclass
class NoOp:
    pass


class NoOpActor(Actor[NoOp | GetCount]):
    def __init__(self):
        self.value = 0

    async def receive(self, msg, ctx: Context) -> None:
        match msg:
            case GetCount():
                await ctx.reply(self.value)
            case NoOp():
                pass

    def get_state(self) -> dict:
        return {"value": self.value}

    def set_state(self, state: dict) -> None:
        self.value = state.get("value", 0)


class Counter(Actor[Increment | GetCount]):
    def __init__(self):
        self.count = 0

    async def receive(self, msg: Increment | GetCount, ctx: Context) -> None:
        match msg:
            case Increment(amount):
                self.count += amount
            case GetCount():
                await ctx.reply(self.count)

    def get_state(self) -> dict:
        return {"count": self.count}

    def set_state(self, state: dict) -> None:
        self.count = state.get("count", 0)


@pytest.mark.asyncio
async def test_persistent_actor_forwards_messages():
    async with DevelopmentCluster(1) as cluster:
        node = cluster[0]

        ref = await node.actor(Counter, name="counter-forwards", scope="cluster")
        await asyncio.sleep(0.1)

        await ref.send(Increment(5))
        await ref.send(Increment(3))
        await asyncio.sleep(0.1)

        count = await ref.ask(GetCount())
        assert count == 8


@pytest.mark.asyncio
async def test_persistent_actor_get_state():
    async with DevelopmentCluster(1) as cluster:
        node = cluster[0]

        ref = await node.actor(Counter, name="counter-state", scope="cluster")
        await asyncio.sleep(0.1)

        await ref.send(Increment(10))
        await asyncio.sleep(0.1)

        state = await ref.ask(GetState())
        assert state == {"count": 10}


@pytest.mark.asyncio
async def test_persistent_actor_get_version():
    async with DevelopmentCluster(1) as cluster:
        node = cluster[0]

        ref = await node.actor(Counter, name="counter-version", scope="cluster")
        await asyncio.sleep(0.1)

        v0 = await ref.ask(GetCurrentVersion())
        assert v0.clock == {}

        await ref.send(Increment(5))
        await asyncio.sleep(0.1)

        v1 = await ref.ask(GetCurrentVersion())
        assert "node-0" in v1.clock
        assert v1.clock["node-0"] == 1


@pytest.mark.asyncio
async def test_persistent_actor_multiple_state_changes():
    async with DevelopmentCluster(1) as cluster:
        node = cluster[0]

        ref = await node.actor(Counter, name="counter-multi", scope="cluster")
        await asyncio.sleep(0.1)

        await ref.send(Increment(10))
        await ref.send(Increment(5))
        await asyncio.sleep(0.1)

        state = await ref.ask(GetState())
        assert state == {"count": 15}

        version = await ref.ask(GetCurrentVersion())
        assert version.clock["node-0"] == 2


@pytest.mark.asyncio
async def test_persistent_actor_no_state_change_no_version_increment():
    async with DevelopmentCluster(1) as cluster:
        node = cluster[0]

        ref = await node.actor(NoOpActor, name="noop-actor", scope="cluster")
        await asyncio.sleep(0.1)

        v0 = await ref.ask(GetCurrentVersion())
        assert v0.clock == {}

        await ref.send(NoOp())
        await asyncio.sleep(0.1)

        v1 = await ref.ask(GetCurrentVersion())
        assert v1.clock == {}
