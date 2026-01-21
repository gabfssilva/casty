"""Tests for gossip key-value store using the Put/Get API."""
import pytest
import asyncio

from casty import ActorSystem
from casty.cluster.gossip import gossip_actor, Put, Get
from casty.cluster.membership import membership_actor
from casty.cluster.messages import SetLocalAddress
from casty.remote import remote, Listen


@pytest.mark.asyncio
async def test_gossip_stores_put():
    """Gossip should store Put messages."""
    async with ActorSystem(node_id="node-1") as system:
        remote_ref = await system.actor(remote(), name="remote")
        await remote_ref.ask(Listen(port=0, host="127.0.0.1"))

        membership_ref = await system.actor(
            membership_actor("node-1"),
            name="membership"
        )
        await membership_ref.send(SetLocalAddress("127.0.0.1:9001"))

        gossip_ref = await system.actor(
            gossip_actor("node-1", fanout=2),
            name="gossip"
        )

        await gossip_ref.send(Put("config/timeout", b"30"))
        await asyncio.sleep(0.05)

        result = await gossip_ref.ask(Get("config/timeout"))
        assert result == b"30"


@pytest.mark.asyncio
async def test_gossip_auto_increments_version():
    """Local Put with version=0 should auto-increment version."""
    async with ActorSystem(node_id="node-1") as system:
        remote_ref = await system.actor(remote(), name="remote")
        await remote_ref.ask(Listen(port=0, host="127.0.0.1"))

        membership_ref = await system.actor(
            membership_actor("node-1"),
            name="membership"
        )
        await membership_ref.send(SetLocalAddress("127.0.0.1:9001"))

        gossip_ref = await system.actor(
            gossip_actor("node-1", fanout=2),
            name="gossip"
        )

        await gossip_ref.send(Put("key", b"v1"))
        await gossip_ref.send(Put("key", b"v2"))
        await asyncio.sleep(0.05)

        result = await gossip_ref.ask(Get("key"))
        assert result == b"v2"


@pytest.mark.asyncio
async def test_gossip_get_returns_none_for_unknown():
    """Get should return None for unknown keys."""
    async with ActorSystem(node_id="node-1") as system:
        remote_ref = await system.actor(remote(), name="remote")
        await remote_ref.ask(Listen(port=0, host="127.0.0.1"))

        membership_ref = await system.actor(
            membership_actor("node-1"),
            name="membership"
        )
        await membership_ref.send(SetLocalAddress("127.0.0.1:9001"))

        gossip_ref = await system.actor(
            gossip_actor("node-1", fanout=2),
            name="gossip"
        )

        result = await gossip_ref.ask(Get("unknown/key"))
        assert result is None


@pytest.mark.asyncio
async def test_gossip_accepts_newer_version():
    """Gossip should accept Put with newer version."""
    async with ActorSystem(node_id="node-1") as system:
        remote_ref = await system.actor(remote(), name="remote")
        await remote_ref.ask(Listen(port=0, host="127.0.0.1"))

        membership_ref = await system.actor(
            membership_actor("node-1"),
            name="membership"
        )
        await membership_ref.send(SetLocalAddress("127.0.0.1:9001"))

        gossip_ref = await system.actor(
            gossip_actor("node-1", fanout=2),
            name="gossip"
        )

        await gossip_ref.send(Put("key", b"old", version=1))
        await gossip_ref.send(Put("key", b"new", version=5))
        await asyncio.sleep(0.05)

        result = await gossip_ref.ask(Get("key"))
        assert result == b"new"


@pytest.mark.asyncio
async def test_gossip_ignores_older_version():
    """Gossip should ignore Put with older version."""
    async with ActorSystem(node_id="node-1") as system:
        remote_ref = await system.actor(remote(), name="remote")
        await remote_ref.ask(Listen(port=0, host="127.0.0.1"))

        membership_ref = await system.actor(
            membership_actor("node-1"),
            name="membership"
        )
        await membership_ref.send(SetLocalAddress("127.0.0.1:9001"))

        gossip_ref = await system.actor(
            gossip_actor("node-1", fanout=2),
            name="gossip"
        )

        await gossip_ref.send(Put("key", b"new", version=10))
        await gossip_ref.send(Put("key", b"old", version=5))
        await asyncio.sleep(0.05)

        result = await gossip_ref.ask(Get("key"))
        assert result == b"new"
