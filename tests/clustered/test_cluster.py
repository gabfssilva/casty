"""Tests for global actor lookup across cluster nodes."""
import asyncio
import pytest
from casty import actor, Mailbox
from casty.cluster import DevelopmentCluster
from casty.serializable import serializable
from dataclasses import dataclass


@serializable
@dataclass
class Inc:
    amount: int


@serializable
@dataclass
class Get:
    pass


@serializable
@dataclass
class GetNodeId:
    pass


@actor
async def counter(initial: int = 0, *, mailbox: Mailbox[Inc | Get]):
    count = initial
    async for msg, ctx in mailbox:
        match msg:
            case Inc(amount):
                count += amount
            case Get():
                await ctx.reply(count)


@pytest.mark.asyncio
async def test_global_actor_lookup():
    """Actor created on node0 should be found by node1.

    Both refs should point to the same actor instance, meaning
    state changes via ref0 should be visible via ref1.
    """
    async with DevelopmentCluster(2, debug=True) as cluster:
        node0, node1 = cluster[0], cluster[1]

        # Create on node0
        ref0 = await node0.actor(counter(0), name="shared")

        # Increment via ref0
        await ref0.send(Inc(10))
        await asyncio.sleep(0.1)
        count0 = await ref0.ask(Get())
        assert count0 == 10, f"Expected count 10 via ref0, got {count0}"

        # Lookup from node1 - should find same actor, not create new one
        ref1 = await node1.actor(counter(0), name="shared")

        # If same actor instance, count should still be 10
        # If different actor, count would be 0 (initial value)
        count1 = await ref1.ask(Get())

        assert count1 == 10, (
            f"Expected count 10 via ref1 (same actor), got {count1}. "
            f"node1.actor() created a new actor instead of finding existing one."
        )


@actor
async def node_id_actor(*, mailbox: Mailbox[GetNodeId]):
    async for msg, ctx in mailbox:
        match msg:
            case GetNodeId():
                await ctx.reply(ctx.node_id)


@pytest.mark.asyncio
async def test_context_has_node_id():
    """Context should have node_id set for tracking which node handles messages."""
    async with DevelopmentCluster(1) as cluster:
        node = cluster[0]

        ref = await node.actor(node_id_actor(), name="stateful")
        node_id = await ref.ask(GetNodeId())

        assert node_id, "ctx.node_id should not be empty"
        assert node_id == node.node_id, f"Expected {node.node_id}, got {node_id}"


