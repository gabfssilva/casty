import asyncio
import pytest
from dataclasses import dataclass
from casty import ActorSystem, Actor, Context
from casty.persistent_actor import (
    PersistentActor,
    GetState,
    GetCurrentVersion,
    StateChanged,
)
from casty.wal import InMemoryStoreBackend, VectorClock


@dataclass
class Increment:
    amount: int


@dataclass
class GetCount:
    pass


class Counter(Actor[Increment | GetCount]):
    def __init__(self):
        self.count = 0

    async def receive(self, msg: Increment | GetCount, ctx: Context) -> None:
        match msg:
            case Increment(amount):
                self.count += amount
            case GetCount():
                await ctx.reply(self.count)


@pytest.mark.asyncio
async def test_persistent_actor_forwards_messages():
    async with ActorSystem() as system:
        persistent = await system.spawn(
            PersistentActor,
            wrapped_actor_cls=Counter,
            actor_id="counter-1",
            node_id="node-a",
            backend=InMemoryStoreBackend(),
        )

        await persistent.send(Increment(5))
        await persistent.send(Increment(3))

        count = await persistent.ask(GetCount())

        assert count == 8


@pytest.mark.asyncio
async def test_persistent_actor_get_state():
    async with ActorSystem() as system:
        persistent = await system.spawn(
            PersistentActor,
            wrapped_actor_cls=Counter,
            actor_id="counter-1",
            node_id="node-a",
            backend=InMemoryStoreBackend(),
        )

        await persistent.send(Increment(10))

        state = await persistent.ask(GetState())
        assert state == {"count": 10}


@pytest.mark.asyncio
async def test_persistent_actor_get_version():
    async with ActorSystem() as system:
        persistent = await system.spawn(
            PersistentActor,
            wrapped_actor_cls=Counter,
            actor_id="counter-1",
            node_id="node-a",
            backend=InMemoryStoreBackend(),
        )

        v0 = await persistent.ask(GetCurrentVersion())
        assert v0.clock == {}

        await persistent.send(Increment(5))

        v1 = await persistent.ask(GetCurrentVersion())
        assert v1.clock == {"node-a": 1}


@pytest.mark.asyncio
async def test_persistent_actor_state_change_notification():
    async with ActorSystem() as system:
        notifications = []

        class NotificationCollector(Actor[StateChanged]):
            async def receive(self, msg: StateChanged, ctx: Context) -> None:
                notifications.append(msg)

        collector = await system.spawn(NotificationCollector)

        persistent = await system.spawn(
            PersistentActor,
            wrapped_actor_cls=Counter,
            actor_id="counter-1",
            node_id="node-a",
            backend=InMemoryStoreBackend(),
            on_state_change=collector,
        )

        await persistent.send(Increment(7))

        await asyncio.sleep(0.1)

        assert len(notifications) == 1
        assert notifications[0].actor_id == "counter-1"
        assert notifications[0].state == {"count": 7}
        assert notifications[0].version.clock == {"node-a": 1}


@pytest.mark.asyncio
async def test_persistent_actor_recovery():
    backend = InMemoryStoreBackend()

    async with ActorSystem() as system:
        persistent = await system.spawn(
            PersistentActor,
            wrapped_actor_cls=Counter,
            actor_id="counter-1",
            node_id="node-a",
            backend=backend,
        )

        await persistent.send(Increment(10))
        await persistent.send(Increment(5))

        state = await persistent.ask(GetState())
        assert state == {"count": 15}

    async with ActorSystem() as system:
        persistent = await system.spawn(
            PersistentActor,
            wrapped_actor_cls=Counter,
            actor_id="counter-1",
            node_id="node-a",
            backend=backend,
        )

        state = await persistent.ask(GetState())
        assert state == {"count": 15}

        version = await persistent.ask(GetCurrentVersion())
        assert version.clock == {"node-a": 2}


@pytest.mark.asyncio
async def test_persistent_actor_no_state_change_no_notification():
    async with ActorSystem() as system:
        notifications = []

        class NotificationCollector(Actor[StateChanged]):
            async def receive(self, msg: StateChanged, ctx: Context) -> None:
                notifications.append(msg)

        collector = await system.spawn(NotificationCollector)

        @dataclass
        class NoOp:
            pass

        class NoOpActor(Actor[NoOp]):
            def __init__(self):
                self.value = 0

            async def receive(self, msg: NoOp, ctx: Context) -> None:
                pass

        persistent = await system.spawn(
            PersistentActor,
            wrapped_actor_cls=NoOpActor,
            actor_id="noop-1",
            node_id="node-a",
            backend=InMemoryStoreBackend(),
            on_state_change=collector,
        )

        await persistent.send(NoOp())
        await asyncio.sleep(0.1)

        assert len(notifications) == 0
