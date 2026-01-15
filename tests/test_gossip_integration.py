"""Integration tests for gossip protocol with multiple nodes."""

import asyncio
import pytest
from dataclasses import dataclass

from casty import ActorSystem, Actor, Context
from casty.gossip import (
    GossipManager,
    GossipConfig,
    Publish,
    GetState,
    Subscribe,
    StateChanged,
    GossipStarted,
)

from .utils import retry_until, free_ports


@dataclass
class GetChanges:
    """Query to get collected changes."""
    pass


class StateCollector(Actor):
    """Actor that collects state changes for testing."""

    def __init__(self):
        self.changes: list[StateChanged] = []
        self.started_events: list[GossipStarted] = []

    async def receive(self, msg, ctx: Context) -> None:
        match msg:
            case StateChanged() as change:
                self.changes.append(change)
            case GossipStarted() as started:
                self.started_events.append(started)
            case GetChanges():
                ctx.reply(list(self.changes))


@pytest.fixture
def gossip_config():
    """Fast gossip config for testing."""
    return GossipConfig(
        gossip_interval=0.1,
        anti_entropy_interval=0.3,
        fanout=3,
        max_push_entries=20,
        reconnect_interval=0.1,
        max_reconnect_attempts=5,
    )


@pytest.mark.asyncio
async def test_single_node_publish_get(gossip_config):
    """Single node can publish and retrieve state."""
    async with ActorSystem() as system:
        gossip = await system.spawn(
            GossipManager,
            node_id="node-1",
            bind_address=("127.0.0.1", 0),  # Random port
            config=gossip_config,
        )

        # Publish state
        await gossip.send(Publish("test/key", b"test value"))

        # Retry until state is available
        async def check():
            result = await gossip.ask(GetState("test/key"))
            return result is not None and result.value == b"test value"

        await retry_until(check, message="State not published")


@pytest.mark.asyncio
async def test_single_node_subscribe(gossip_config):
    """Subscriptions receive state changes."""
    async with ActorSystem() as system:
        collector = await system.spawn(StateCollector)
        gossip = await system.spawn(
            GossipManager,
            node_id="node-1",
            bind_address=("127.0.0.1", 0),
            config=gossip_config,
        )

        # Wait for gossip to start
        await asyncio.sleep(0.1)

        # Subscribe to pattern
        await gossip.send(Subscribe("test/*", collector))

        # Publish matching keys
        await gossip.send(Publish("test/key1", b"value1"))
        await gossip.send(Publish("test/key2", b"value2"))

        # Publish non-matching key
        await gossip.send(Publish("other/key", b"other"))

        # Wait for changes
        async def check():
            changes = await collector.ask(GetChanges())
            matching_keys = {c.key for c in changes if c.key.startswith("test/")}
            return "test/key1" in matching_keys and "test/key2" in matching_keys

        await retry_until(check, message="Subscriptions not received")


@pytest.mark.asyncio
async def test_two_nodes_state_sync(gossip_config):
    """Two nodes sync state through gossip."""
    [port1] = free_ports(1)

    async with ActorSystem() as system:
        node1 = await system.spawn(
            GossipManager,
            node_id="node-1",
            bind_address=("127.0.0.1", port1),
            config=gossip_config,
        )

        await asyncio.sleep(0.1)

        # Start node 2, connecting to node 1
        node2 = await system.spawn(
            GossipManager,
            node_id="node-2",
            bind_address=("127.0.0.1", 0),
            seeds=[("127.0.0.1", port1)],
            config=gossip_config,
        )

        # Wait for connection
        await asyncio.sleep(0.3)

        # Publish on node 1
        await node1.send(Publish("shared/key", b"from node 1"))

        # Retry until node2 has the state
        async def check():
            result = await node2.ask(GetState("shared/key"))
            return result is not None and result.value == b"from node 1"

        await retry_until(check, timeout=10.0, message="State not synced to node 2")


@pytest.mark.asyncio
async def test_conflict_resolution_lww(gossip_config):
    """Concurrent updates resolved with LWW."""
    [port1] = free_ports(1)

    async with ActorSystem() as system:
        node1 = await system.spawn(
            GossipManager,
            node_id="node-1",
            bind_address=("127.0.0.1", port1),
            config=gossip_config,
        )

        await asyncio.sleep(0.1)

        node2 = await system.spawn(
            GossipManager,
            node_id="node-2",
            bind_address=("127.0.0.1", 0),
            seeds=[("127.0.0.1", port1)],
            config=gossip_config,
        )

        # Wait for connection
        await asyncio.sleep(0.5)

        # Both nodes update same key concurrently
        # Due to LWW semantics, the one with higher node_id wins when concurrent
        await node1.send(Publish("conflict/key", b"value from node-1"))
        await node2.send(Publish("conflict/key", b"value from node-2"))

        # Wait for convergence
        async def check():
            r1 = await node1.ask(GetState("conflict/key"))
            r2 = await node2.ask(GetState("conflict/key"))
            if r1 is None or r2 is None:
                return False
            # Both should converge to same value
            return r1.value == r2.value

        await retry_until(check, timeout=10.0, message="Values did not converge")


@pytest.mark.asyncio
async def test_three_nodes_eventual_consistency(gossip_config):
    """Three nodes eventually converge to same state."""
    [port1] = free_ports(1)

    async with ActorSystem() as system:
        node1 = await system.spawn(
            GossipManager,
            node_id="node-1",
            bind_address=("127.0.0.1", port1),
            config=gossip_config,
        )

        await asyncio.sleep(0.1)

        node2 = await system.spawn(
            GossipManager,
            node_id="node-2",
            bind_address=("127.0.0.1", 0),
            seeds=[("127.0.0.1", port1)],
            config=gossip_config,
        )

        await asyncio.sleep(0.1)

        node3 = await system.spawn(
            GossipManager,
            node_id="node-3",
            bind_address=("127.0.0.1", 0),
            seeds=[("127.0.0.1", port1)],
            config=gossip_config,
        )

        # Wait for mesh to form
        await asyncio.sleep(0.5)

        # Each node publishes different keys
        await node1.send(Publish("data/node1", b"from 1"))
        await node2.send(Publish("data/node2", b"from 2"))
        await node3.send(Publish("data/node3", b"from 3"))

        # Retry until all nodes have all keys
        async def check():
            for node in [node1, node2, node3]:
                r1 = await node.ask(GetState("data/node1"))
                r2 = await node.ask(GetState("data/node2"))
                r3 = await node.ask(GetState("data/node3"))

                if r1 is None or r1.value != b"from 1":
                    return False
                if r2 is None or r2.value != b"from 2":
                    return False
                if r3 is None or r3.value != b"from 3":
                    return False
            return True

        await retry_until(check, timeout=15.0, message="Three nodes did not converge")


@pytest.mark.asyncio
async def test_node_reconnection(gossip_config):
    """Node reconnects and syncs state."""
    [port1] = free_ports(1)

    async with ActorSystem() as system:
        node1 = await system.spawn(
            GossipManager,
            node_id="node-1",
            bind_address=("127.0.0.1", port1),
            config=gossip_config,
        )

        await asyncio.sleep(0.1)

        # Publish state before node2 joins
        await node1.send(Publish("existing/key", b"already here"))

        await asyncio.sleep(0.1)

        # Node 2 joins and should receive existing state via anti-entropy
        node2 = await system.spawn(
            GossipManager,
            node_id="node-2",
            bind_address=("127.0.0.1", 0),
            seeds=[("127.0.0.1", port1)],
            config=gossip_config,
        )

        # Retry until node2 has the existing state
        async def check():
            result = await node2.ask(GetState("existing/key"))
            return result is not None and result.value == b"already here"

        await retry_until(check, timeout=10.0, message="Existing state not synced")
