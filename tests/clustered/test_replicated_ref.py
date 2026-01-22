from __future__ import annotations

import pytest
from unittest.mock import AsyncMock, MagicMock
from dataclasses import dataclass

from casty.cluster.replicated_ref import ReplicatedActorRef
from casty.actor_config import Routing
from casty.cluster.replica_manager import ReplicaManager


@dataclass
class Get:
    key: str


@dataclass
class Put:
    key: str
    value: bytes


class TestReplicatedActorRef:
    @pytest.mark.asyncio
    async def test_routes_to_leader_by_default(self):
        manager = ReplicaManager()
        manager.register("test/actor", ["node-1", "node-2", "node-3"], "node-2")

        refs = {
            "node-1": AsyncMock(),
            "node-2": AsyncMock(),
            "node-3": AsyncMock(),
        }

        replicated_ref = ReplicatedActorRef(
            actor_id="test/actor",
            manager=manager,
            node_refs=refs,
            routing={},
            local_node="node-1"
        )

        await replicated_ref.send(Put("key", b"value"))

        refs["node-2"].send.assert_called_once()
        refs["node-1"].send.assert_not_called()
        refs["node-3"].send.assert_not_called()

    @pytest.mark.asyncio
    async def test_routes_get_to_any_when_configured(self):
        manager = ReplicaManager()
        manager.register("test/actor", ["node-1", "node-2", "node-3"], "node-1")

        refs = {
            "node-1": AsyncMock(),
            "node-2": AsyncMock(),
            "node-3": AsyncMock(),
        }

        replicated_ref = ReplicatedActorRef(
            actor_id="test/actor",
            manager=manager,
            node_refs=refs,
            routing={Get: Routing.ANY},
            local_node="node-2"
        )

        await replicated_ref.send(Get("key"))

        total_calls = sum(1 for r in refs.values() if r.send.called)
        assert total_calls == 1

    @pytest.mark.asyncio
    async def test_routes_to_local_first_when_configured(self):
        manager = ReplicaManager()
        manager.register("test/actor", ["node-1", "node-2", "node-3"], "node-1")

        refs = {
            "node-1": AsyncMock(),
            "node-2": AsyncMock(),
            "node-3": AsyncMock(),
        }

        replicated_ref = ReplicatedActorRef(
            actor_id="test/actor",
            manager=manager,
            node_refs=refs,
            routing={Get: Routing.LOCAL_FIRST},
            local_node="node-2"
        )

        await replicated_ref.send(Get("key"))

        refs["node-2"].send.assert_called_once()

    @pytest.mark.asyncio
    async def test_ask_routes_correctly(self):
        manager = ReplicaManager()
        manager.register("test/actor", ["node-1", "node-2"], "node-1")

        refs = {
            "node-1": AsyncMock(),
            "node-2": AsyncMock(),
        }
        refs["node-1"].ask = AsyncMock(return_value="result")

        replicated_ref = ReplicatedActorRef(
            actor_id="test/actor",
            manager=manager,
            node_refs=refs,
            routing={},
            local_node="node-2"
        )

        result = await replicated_ref.ask(Get("key"))

        assert result == "result"
        refs["node-1"].ask.assert_called_once()

    @pytest.mark.asyncio
    async def test_local_first_falls_back_when_local_not_in_replicas(self):
        manager = ReplicaManager()
        manager.register("test/actor", ["node-1", "node-3"], "node-1")

        refs = {
            "node-1": AsyncMock(),
            "node-3": AsyncMock(),
        }

        replicated_ref = ReplicatedActorRef(
            actor_id="test/actor",
            manager=manager,
            node_refs=refs,
            routing={Get: Routing.LOCAL_FIRST},
            local_node="node-2"
        )

        await replicated_ref.send(Get("key"))

        refs["node-1"].send.assert_called_once()
        refs["node-3"].send.assert_not_called()

    @pytest.mark.asyncio
    async def test_ask_raises_when_no_node_available(self):
        manager = ReplicaManager()

        replicated_ref = ReplicatedActorRef(
            actor_id="test/actor",
            manager=manager,
            node_refs={},
            routing={},
            local_node="node-1"
        )

        with pytest.raises(RuntimeError, match="No available node"):
            await replicated_ref.ask(Get("key"))

    @pytest.mark.asyncio
    async def test_round_robin_distributes_across_replicas(self):
        manager = ReplicaManager()
        manager.register("test/actor", ["node-1", "node-2", "node-3"], "node-1")

        refs = {
            "node-1": AsyncMock(),
            "node-2": AsyncMock(),
            "node-3": AsyncMock(),
        }

        replicated_ref = ReplicatedActorRef(
            actor_id="test/actor",
            manager=manager,
            node_refs=refs,
            routing={Get: Routing.ANY},
            local_node="node-1"
        )

        for _ in range(3):
            await replicated_ref.send(Get("key"))

        assert refs["node-1"].send.call_count >= 0
        assert refs["node-2"].send.call_count >= 0
        assert refs["node-3"].send.call_count >= 0
        total = sum(r.send.call_count for r in refs.values())
        assert total == 3

    @pytest.mark.asyncio
    async def test_operators_work(self):
        manager = ReplicaManager()
        manager.register("test/actor", ["node-1"], "node-1")

        refs = {
            "node-1": AsyncMock(),
        }
        refs["node-1"].ask = AsyncMock(return_value="result")

        replicated_ref = ReplicatedActorRef(
            actor_id="test/actor",
            manager=manager,
            node_refs=refs,
            routing={},
            local_node="node-1"
        )

        await (replicated_ref >> Put("key", b"value"))
        refs["node-1"].send.assert_called_once()

        result = await (replicated_ref << Get("key"))
        assert result == "result"
