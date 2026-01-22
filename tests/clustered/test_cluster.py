"""Tests for global actor lookup across cluster nodes."""
import asyncio
import pytest
from casty import actor, Mailbox
from casty.state import State
from casty.actor_config import Routing
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


@serializable
@dataclass
class WhoHandled:
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
    async with DevelopmentCluster(2) as cluster:
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
async def state_actor(*, mailbox: Mailbox[GetNodeId], state: State[int]):
    async for msg, ctx in mailbox:
        match msg:
            case GetNodeId():
                await ctx.reply(state.node_id)


@pytest.mark.asyncio
async def test_state_has_node_id():
    """State should have node_id set for VectorClock increments."""
    async with DevelopmentCluster(1) as cluster:
        node = cluster[0]

        ref = await node.actor(state_actor(state=0), name="stateful")
        node_id = await ref.ask(GetNodeId())

        assert node_id, "State.node_id should not be empty"
        assert node_id == node.node_id, f"Expected {node.node_id}, got {node_id}"


@actor(replicated=3, routing={WhoHandled: Routing.ANY})
async def routing_actor(*, mailbox: Mailbox[WhoHandled]):
    async for msg, ctx in mailbox:
        match msg:
            case WhoHandled():
                await ctx.reply(ctx.node_id)


@pytest.mark.asyncio
async def test_routing_any_distributes():
    """Routing.ANY should distribute messages across nodes."""
    async with asyncio.timeout(10):
        async with DevelopmentCluster(3) as cluster:
            node0 = cluster[0]

            ref = await node0.actor(routing_actor(), name="routed")

            handlers = set()
            for _ in range(6):
                node_id = await ref.ask(WhoHandled())
                handlers.add(node_id)

            assert len(handlers) > 1, f"Expected multiple handlers, got {handlers}"
