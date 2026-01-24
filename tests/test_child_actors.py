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


@dataclass
class Stop:
    pass


@actor
async def parent_that_stops(*, mailbox: Mailbox[CreateChild | Stop]):
    async for msg, ctx in mailbox:
        match msg:
            case CreateChild(name):
                ref = await ctx.actor(child(), name=name)
                await ctx.reply(ref)
            case Stop():
                return


@pytest.mark.asyncio
async def test_children_stopped_when_parent_stops():
    from casty.system import LocalActorSystem

    async with asyncio.timeout(5):
        system = LocalActorSystem()

        parent_ref = await system.actor(parent_that_stops(), name="p1")

        child_ref = await parent_ref.ask(CreateChild("c1"))
        assert child_ref.actor_id == "p1/c1"

        assert "p1" in system._actors
        assert "p1/c1" in system._actors
        assert "p1" in system._children
        assert "p1/c1" in system._children["p1"]

        await parent_ref.send(Stop())
        await asyncio.sleep(0.1)

        assert "p1" not in system._actors
        assert "p1/c1" not in system._actors
        assert "p1" not in system._children


@actor
async def parent_with_nested_children(*, mailbox: Mailbox[CreateChild | Stop]):
    async for msg, ctx in mailbox:
        match msg:
            case CreateChild(name):
                ref = await ctx.actor(parent_that_stops(), name=name)
                await ctx.reply(ref)
            case Stop():
                return


@pytest.mark.asyncio
async def test_nested_children_stopped_recursively():
    from casty.system import LocalActorSystem

    async with asyncio.timeout(5):
        system = LocalActorSystem()

        grandparent = await system.actor(parent_with_nested_children(), name="gp")
        parent_ref = await grandparent.ask(CreateChild("p"))
        child_ref = await parent_ref.ask(CreateChild("c"))

        assert grandparent.actor_id == "gp"
        assert parent_ref.actor_id == "gp/p"
        assert child_ref.actor_id == "gp/p/c"

        assert "gp" in system._actors
        assert "gp/p" in system._actors
        assert "gp/p/c" in system._actors

        await grandparent.send(Stop())
        await asyncio.sleep(0.2)

        assert "gp" not in system._actors
        assert "gp/p" not in system._actors
        assert "gp/p/c" not in system._actors
