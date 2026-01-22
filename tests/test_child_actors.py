import pytest
import asyncio
from dataclasses import dataclass

from casty import actor, Mailbox


@dataclass
class CreateChild:
    name: str


@dataclass
class GetChildCount:
    pass


@dataclass
class Ping:
    pass


@actor
async def child(*, mailbox: Mailbox[Ping]):
    async for msg, ctx in mailbox:
        match msg:
            case Ping():
                await ctx.reply("pong")


@actor
async def parent(*, mailbox: Mailbox[CreateChild | GetChildCount]):
    children: dict[str, any] = {}

    async for msg, ctx in mailbox:
        match msg:
            case CreateChild(name):
                ref = await ctx.actor(child(), name=name)
                children[name] = ref
                await ctx.reply(ref)
            case GetChildCount():
                await ctx.reply(len(children))


@pytest.mark.asyncio
async def test_create_child_actor():
    from casty.system import ActorSystem

    async with ActorSystem() as system:
        parent_ref = await system.actor(parent(), name="p1")

        child_ref = await parent_ref.ask(CreateChild("c1"))
        assert child_ref is not None
        assert "c1" in child_ref.actor_id

        result = await child_ref.ask(Ping())
        assert result == "pong"


@pytest.mark.asyncio
async def test_child_actor_hierarchy():
    from casty.system import ActorSystem

    async with ActorSystem() as system:
        parent_ref = await system.actor(parent(), name="p1")

        child_ref = await parent_ref.ask(CreateChild("c1"))

        # Child ID should include parent path
        assert child_ref.actor_id == "p1/c1"
