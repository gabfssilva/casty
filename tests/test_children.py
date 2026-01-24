import asyncio
import pytest
from dataclasses import dataclass

from casty import ActorSystem, actor, Mailbox


@dataclass
class SpawnChild:
    name: str


@dataclass
class Ping:
    pass


@dataclass
class GetChildId:
    pass


@pytest.mark.asyncio
async def test_child_actor_has_hierarchical_id():
    """Child spawned via ctx.actor() has parent/child ID format"""
    child_id: str | None = None

    @actor
    async def child(*, mailbox: Mailbox[Ping]):
        async for msg, ctx in mailbox:
            pass

    @actor
    async def parent(*, mailbox: Mailbox[SpawnChild | GetChildId]):
        nonlocal child_id
        child_ref = None
        async for msg, ctx in mailbox:
            match msg:
                case SpawnChild(name):
                    child_ref = await ctx.actor(child(), name=name)
                    child_id = child_ref.actor_id
                case GetChildId():
                    await ctx.reply(child_id)

    async with asyncio.timeout(5):
        async with ActorSystem() as system:
            parent_ref = await system.actor(parent(), name="parent")

            await parent_ref.send(SpawnChild("worker"))
            await asyncio.sleep(0.05)

            result = await parent_ref.ask(GetChildId())

    assert result == "parent/worker"


@pytest.mark.asyncio
async def test_child_can_receive_messages():
    """Child actor can receive and process messages"""
    responses: list[str] = []

    @actor
    async def worker(*, mailbox: Mailbox[Ping]):
        async for msg, ctx in mailbox:
            await ctx.reply("worked")

    @actor
    async def manager(*, mailbox: Mailbox[SpawnChild | Ping]):
        child_ref = None
        async for msg, ctx in mailbox:
            match msg:
                case SpawnChild(name):
                    child_ref = await ctx.actor(worker(), name=name)
                case Ping():
                    if child_ref:
                        result = await child_ref.ask(Ping())
                        responses.append(result)

    async with asyncio.timeout(5):
        async with ActorSystem() as system:
            manager_ref = await system.actor(manager(), name="manager")

            await manager_ref.send(SpawnChild("w1"))
            await asyncio.sleep(0.05)

            await manager_ref.send(Ping())
            await asyncio.sleep(0.05)

    assert responses == ["worked"]


@pytest.mark.asyncio
async def test_multiple_children():
    """Parent can spawn multiple children"""
    child_ids: list[str] = []

    @actor
    async def child(*, mailbox: Mailbox[Ping]):
        async for msg, ctx in mailbox:
            pass

    @actor
    async def parent(*, mailbox: Mailbox[SpawnChild]):
        async for msg, ctx in mailbox:
            match msg:
                case SpawnChild(name):
                    ref = await ctx.actor(child(), name=name)
                    child_ids.append(ref.actor_id)

    async with asyncio.timeout(5):
        async with ActorSystem() as system:
            parent_ref = await system.actor(parent(), name="parent")

            await parent_ref.send(SpawnChild("child1"))
            await parent_ref.send(SpawnChild("child2"))
            await parent_ref.send(SpawnChild("child3"))

            await asyncio.sleep(0.1)

    assert child_ids == ["parent/child1", "parent/child2", "parent/child3"]
