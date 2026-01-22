# tests/clustered/test_failover.py
import pytest
import asyncio
from casty import actor, Mailbox, message
from casty.state import State
from casty.actor_config import Routing
from casty.cluster import DevelopmentCluster
from casty.cluster.replica_manager import ReplicaManager


class TestHandleNodeDown:
    def test_handle_node_down_elects_new_leader(self):
        manager = ReplicaManager()
        manager.register("actor1", ["node-1", "node-2", "node-3"], "node-1")
        manager.register("actor2", ["node-1", "node-4"], "node-1")
        manager.register("actor3", ["node-2", "node-3"], "node-2")

        affected = manager.handle_node_down("node-1")

        # actor1 and actor2 should have new leaders
        assert "actor1" in [a[0] for a in affected]
        assert "actor2" in [a[0] for a in affected]
        assert "actor3" not in [a[0] for a in affected]

        # New leaders should be elected
        assert manager.get_leader("actor1") == "node-2"
        assert manager.get_leader("actor2") == "node-4"
        assert manager.get_leader("actor3") == "node-2"  # unchanged

    def test_handle_node_down_removes_from_replicas(self):
        manager = ReplicaManager()
        manager.register("actor1", ["node-1", "node-2", "node-3"], "node-2")

        manager.handle_node_down("node-1")

        assert manager.get_replicas("actor1") == ["node-2", "node-3"]

    def test_handle_node_down_returns_actors_needing_rebalance(self):
        manager = ReplicaManager()
        manager.register("actor1", ["node-1", "node-2", "node-3"], "node-1")

        affected = manager.handle_node_down("node-1")

        # Returns actors that need rebalancing (lost a replica)
        # Format: list of (actor_id, new_leader) tuples
        assert affected == [("actor1", "node-2")]

    def test_handle_node_down_unknown_node(self):
        manager = ReplicaManager()
        manager.register("actor1", ["node-1", "node-2"], "node-1")

        affected = manager.handle_node_down("node-999")

        assert affected == []


@message(readonly=True)
class Get:
    pass


@message
class Set:
    value: int


@pytest.mark.asyncio
async def test_failover_on_leader_down():
    @actor(replicated=2, routing={Get: Routing.LEADER, Set: Routing.LEADER})
    async def counter(state: State[int], mailbox: Mailbox[Get | Set]):
        async for msg, ctx in mailbox:
            match msg:
                case Get():
                    await ctx.reply(state.value)
                case Set(value):
                    state.set(value)

    async with DevelopmentCluster(nodes=3) as cluster:
        ref = await cluster.nodes[0].actor(counter(0), name="failover-test")

        # Set initial value
        await ref.send(Set(42))
        result = await ref.ask(Get())
        assert result == 42

        # TODO: Simulate leader failure
        # TODO: Verify replica takes over


@pytest.mark.asyncio
async def test_basic_replication_setup():
    @actor(replicated=2, routing={Get: Routing.LEADER, Set: Routing.LEADER})
    async def storage(state: State[int], mailbox: Mailbox[Get | Set]):
        async for msg, ctx in mailbox:
            match msg:
                case Get():
                    await ctx.reply(state.value)
                case Set(value):
                    state.set(value)

    async with DevelopmentCluster(nodes=2) as cluster:
        ref = await cluster.nodes[0].actor(storage(0), name="replicated")

        await ref.send(Set(100))
        await asyncio.sleep(0.1)

        result = await ref.ask(Get())
        assert result == 100
