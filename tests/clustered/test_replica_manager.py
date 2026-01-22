# tests/clustered/test_replica_manager.py
import pytest
from casty.cluster.replica_manager import ReplicaManager, ReplicaInfo


class TestReplicaManager:
    def test_register_replicated_actor(self):
        manager = ReplicaManager()

        manager.register(
            actor_id="cache/user:123",
            nodes=["node-1", "node-2", "node-3"],
            leader="node-1"
        )

        info = manager.get("cache/user:123")
        assert info is not None
        assert info.nodes == ["node-1", "node-2", "node-3"]
        assert info.leader == "node-1"

    def test_get_leader(self):
        manager = ReplicaManager()
        manager.register("test/actor", ["a", "b", "c"], "b")

        assert manager.get_leader("test/actor") == "b"

    def test_get_leader_unknown_actor(self):
        manager = ReplicaManager()

        assert manager.get_leader("unknown") is None

    def test_get_replicas(self):
        manager = ReplicaManager()
        manager.register("test/actor", ["a", "b", "c"], "a")

        assert manager.get_replicas("test/actor") == ["a", "b", "c"]

    def test_is_leader(self):
        manager = ReplicaManager()
        manager.register("test/actor", ["a", "b", "c"], "b")

        assert manager.is_leader("test/actor", "b") is True
        assert manager.is_leader("test/actor", "a") is False

    def test_set_leader(self):
        manager = ReplicaManager()
        manager.register("test/actor", ["a", "b", "c"], "a")

        manager.set_leader("test/actor", "c")

        assert manager.get_leader("test/actor") == "c"

    def test_remove_node(self):
        manager = ReplicaManager()
        manager.register("test/actor", ["a", "b", "c"], "a")

        manager.remove_node("test/actor", "b")

        assert manager.get_replicas("test/actor") == ["a", "c"]

    def test_remove_leader_node_elects_new(self):
        manager = ReplicaManager()
        manager.register("test/actor", ["a", "b", "c"], "a")

        manager.remove_node("test/actor", "a")

        assert manager.get_leader("test/actor") == "b"
        assert manager.get_replicas("test/actor") == ["b", "c"]

    def test_add_node(self):
        manager = ReplicaManager()
        manager.register("test/actor", ["a", "b"], "a")

        manager.add_node("test/actor", "c")

        assert manager.get_replicas("test/actor") == ["a", "b", "c"]

    def test_get_all_actors_for_node(self):
        manager = ReplicaManager()
        manager.register("actor1", ["a", "b"], "a")
        manager.register("actor2", ["a", "c"], "a")
        manager.register("actor3", ["b", "c"], "b")

        actors = manager.get_actors_for_node("a")

        assert set(actors) == {"actor1", "actor2"}

    def test_get_actors_where_node_is_leader(self):
        manager = ReplicaManager()
        manager.register("actor1", ["a", "b"], "a")
        manager.register("actor2", ["a", "c"], "c")
        manager.register("actor3", ["a", "b"], "a")

        actors = manager.get_actors_where_leader("a")

        assert set(actors) == {"actor1", "actor3"}
