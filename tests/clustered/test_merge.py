import pytest

from casty.cluster import deserialize
from casty.cluster.merge import VectorClock, WAL, WALEntry


class TestVectorClock:
    def test_empty_clock(self):
        clock = VectorClock()
        assert clock.clock == {}

    def test_increment_creates_new_clock(self):
        clock = VectorClock()
        new_clock = clock.increment("node-a")
        assert clock.clock == {}
        assert new_clock.clock == {"node-a": 1}

    def test_increment_multiple_times(self):
        clock = VectorClock()
        clock = clock.increment("node-a")
        clock = clock.increment("node-a")
        clock = clock.increment("node-a")
        assert clock.clock == {"node-a": 3}

    def test_increment_different_nodes(self):
        clock = VectorClock()
        clock = clock.increment("node-a")
        clock = clock.increment("node-b")
        clock = clock.increment("node-a")
        assert clock.clock == {"node-a": 2, "node-b": 1}

    def test_merge_empty_clocks(self):
        clock1 = VectorClock()
        clock2 = VectorClock()
        merged = clock1.merge(clock2)
        assert merged.clock == {}

    def test_merge_takes_max(self):
        clock1 = VectorClock({"node-a": 2, "node-b": 1})
        clock2 = VectorClock({"node-a": 1, "node-b": 3})
        merged = clock1.merge(clock2)
        assert merged.clock == {"node-a": 2, "node-b": 3}

    def test_merge_with_different_nodes(self):
        clock1 = VectorClock({"node-a": 2})
        clock2 = VectorClock({"node-b": 3})
        merged = clock1.merge(clock2)
        assert merged.clock == {"node-a": 2, "node-b": 3}

    def test_merge_symmetric(self):
        clock1 = VectorClock({"node-a": 2, "node-b": 1})
        clock2 = VectorClock({"node-a": 1, "node-b": 3})
        assert clock1.merge(clock2) == clock2.merge(clock1)

    def test_dominates_true(self):
        older = VectorClock({"node-a": 1})
        newer = VectorClock({"node-a": 2})
        assert newer.dominates(older)
        assert not older.dominates(newer)

    def test_dominates_with_extra_node(self):
        older = VectorClock({"node-a": 1})
        newer = VectorClock({"node-a": 1, "node-b": 1})
        assert newer.dominates(older)
        assert not older.dominates(newer)

    def test_dominates_all_gte_but_none_gt(self):
        clock1 = VectorClock({"node-a": 1, "node-b": 1})
        clock2 = VectorClock({"node-a": 1, "node-b": 1})
        assert not clock1.dominates(clock2)
        assert not clock2.dominates(clock1)

    def test_dominates_requires_all_gte(self):
        clock1 = VectorClock({"node-a": 2, "node-b": 1})
        clock2 = VectorClock({"node-a": 1, "node-b": 2})
        assert not clock1.dominates(clock2)
        assert not clock2.dominates(clock1)

    def test_concurrent_true(self):
        clock1 = VectorClock({"node-a": 2, "node-b": 1})
        clock2 = VectorClock({"node-a": 1, "node-b": 2})
        assert clock1.concurrent(clock2)
        assert clock2.concurrent(clock1)

    def test_concurrent_false_when_dominates(self):
        older = VectorClock({"node-a": 1})
        newer = VectorClock({"node-a": 2})
        assert not older.concurrent(newer)
        assert not newer.concurrent(older)

    def test_concurrent_false_when_equal(self):
        clock1 = VectorClock({"node-a": 1})
        clock2 = VectorClock({"node-a": 1})
        assert not clock1.concurrent(clock2)

    def test_concurrent_divergent_modifications(self):
        base = VectorClock()
        clock1 = base.increment("node-a")
        clock2 = base.increment("node-b")
        assert clock1.concurrent(clock2)

    def test_equality_same_clocks(self):
        clock1 = VectorClock({"node-a": 1, "node-b": 2})
        clock2 = VectorClock({"node-a": 1, "node-b": 2})
        assert clock1 == clock2

    def test_equality_different_clocks(self):
        clock1 = VectorClock({"node-a": 1})
        clock2 = VectorClock({"node-a": 2})
        assert clock1 != clock2

    def test_equality_with_zero_values(self):
        clock1 = VectorClock({"node-a": 1})
        clock2 = VectorClock({"node-a": 1, "node-b": 0})
        assert clock1 == clock2

    def test_equality_non_vectorclock(self):
        clock = VectorClock({"node-a": 1})
        assert clock != {"node-a": 1}
        assert clock != "not a clock"
        assert clock != 42

    def test_hash_equal_clocks(self):
        clock1 = VectorClock({"node-a": 1, "node-b": 2})
        clock2 = VectorClock({"node-a": 1, "node-b": 2})
        assert hash(clock1) == hash(clock2)

    def test_hash_usable_in_set(self):
        clock1 = VectorClock({"node-a": 1})
        clock2 = VectorClock({"node-a": 1})
        clock3 = VectorClock({"node-a": 2})
        clock_set = {clock1, clock2, clock3}
        assert len(clock_set) == 2

    def test_repr(self):
        clock = VectorClock({"node-a": 1, "node-b": 2})
        repr_str = repr(clock)
        assert "VectorClock" in repr_str
        assert "node-a:1" in repr_str
        assert "node-b:2" in repr_str

    def test_to_dict(self):
        clock = VectorClock({"node-a": 1, "node-b": 2})
        d = clock.to_dict()
        assert d == {"node-a": 1, "node-b": 2}
        d["node-a"] = 99
        assert clock.clock["node-a"] == 1

    def test_from_dict(self):
        d = {"node-a": 1, "node-b": 2}
        clock = VectorClock.from_dict(d)
        assert clock.clock == {"node-a": 1, "node-b": 2}
        d["node-a"] = 99
        assert clock.clock["node-a"] == 1

    def test_serialization_roundtrip(self):
        clock = VectorClock({"node-a": 5, "node-b": 3})
        data = VectorClock.Codec.serialize(clock)
        result = deserialize(data)
        assert result == clock
        assert isinstance(result, VectorClock)

    def test_serialization_empty_clock(self):
        clock = VectorClock()
        data = VectorClock.Codec.serialize(clock)
        result = deserialize(data)
        assert result == clock


class TestWALEntry:
    def test_create_entry(self):
        version = VectorClock({"node-a": 1})
        delta = {"count": 10}
        entry = WALEntry(version=version, delta=delta)
        assert entry.version == version
        assert entry.delta == delta
        assert entry.timestamp > 0

    def test_entry_with_explicit_timestamp(self):
        version = VectorClock({"node-a": 1})
        delta = {"count": 10}
        entry = WALEntry(version=version, delta=delta, timestamp=12345.0)
        assert entry.timestamp == 12345.0


class TestWAL:
    def test_create_wal(self):
        wal = WAL(actor_id="counter-1", node_id="node-a")
        assert wal.actor_id == "counter-1"
        assert wal.node_id == "node-a"
        assert wal.entries == []
        assert wal.base_snapshot == {}
        assert wal.base_version == VectorClock()

    def test_current_version_empty_wal(self):
        wal = WAL(actor_id="counter-1", node_id="node-a")
        assert wal.current_version == VectorClock()

    def test_current_version_with_entries(self):
        wal = WAL(actor_id="counter-1", node_id="node-a")
        wal.append({"count": 10})
        assert wal.current_version == VectorClock({"node-a": 1})

    def test_append_returns_new_version(self):
        wal = WAL(actor_id="counter-1", node_id="node-a")
        v1 = wal.append({"count": 10})
        v2 = wal.append({"count": 20})
        assert v1 == VectorClock({"node-a": 1})
        assert v2 == VectorClock({"node-a": 2})

    def test_append_stores_delta(self):
        wal = WAL(actor_id="counter-1", node_id="node-a")
        wal.append({"count": 10})
        assert len(wal.entries) == 1
        assert wal.entries[0].delta == {"count": 10}

    def test_get_current_state(self):
        wal = WAL(actor_id="counter-1", node_id="node-a")
        wal.append({"count": 10})
        wal.append({"count": 20})
        wal.append({"name": "test"})
        state = wal.get_current_state()
        assert state == {"count": 20, "name": "test"}

    def test_get_current_state_with_base(self):
        wal = WAL(
            actor_id="counter-1",
            node_id="node-a",
            base_snapshot={"initial": True}
        )
        wal.append({"count": 10})
        state = wal.get_current_state()
        assert state == {"initial": True, "count": 10}

    def test_get_state_at_version(self):
        wal = WAL(actor_id="counter-1", node_id="node-a")
        v1 = wal.append({"count": 10})
        v2 = wal.append({"count": 20})

        state1 = wal.get_state_at(v1)
        state2 = wal.get_state_at(v2)

        assert state1 == {"count": 10}
        assert state2 == {"count": 20}

    def test_get_state_at_base_version(self):
        wal = WAL(
            actor_id="counter-1",
            node_id="node-a",
            base_snapshot={"initial": True}
        )
        wal.append({"count": 10})
        state = wal.get_state_at(VectorClock())
        assert state == {"initial": True}

    def test_get_state_at_nonexistent_version(self):
        wal = WAL(actor_id="counter-1", node_id="node-a")
        wal.append({"count": 10})
        state = wal.get_state_at(VectorClock({"node-b": 5}))
        assert state is None

    def test_find_base_with_common_ancestor(self):
        wal = WAL(actor_id="counter-1", node_id="node-a")
        v1 = wal.append({"count": 10})
        v2 = wal.append({"count": 20})

        their_version = v1.increment("node-b")

        result = wal.find_base(their_version)
        assert result is not None
        base_version, base_state = result
        assert base_version == v1
        assert base_state == {"count": 10}

    def test_find_base_empty_common_ancestor(self):
        wal = WAL(actor_id="counter-1", node_id="node-a")
        wal.append({"count": 10})

        their_version = VectorClock().increment("node-b")

        result = wal.find_base(their_version)
        assert result is not None
        base_version, base_state = result
        assert base_version == VectorClock()
        assert base_state == {}

    def test_find_base_no_common_ancestor(self):
        base_version = VectorClock({"node-c": 5})
        wal = WAL(
            actor_id="counter-1",
            node_id="node-a",
            base_version=base_version,
            base_snapshot={"old": True}
        )
        wal.append({"count": 10})

        their_version = VectorClock({"node-b": 1})

        result = wal.find_base(their_version)
        assert result is None

    def test_sync_to(self):
        wal = WAL(actor_id="counter-1", node_id="node-a")
        wal.append({"count": 10})
        wal.append({"count": 20})

        new_version = VectorClock({"node-b": 5})
        new_state = {"count": 100, "synced": True}

        wal.sync_to(new_version, new_state)

        assert wal.entries == []
        assert wal.base_version == new_version
        assert wal.base_snapshot == new_state
        assert wal.current_version == new_version
        assert wal.get_current_state() == new_state

    def test_append_merged(self):
        wal = WAL(actor_id="counter-1", node_id="node-a")
        wal.append({"count": 10})

        merged_version = VectorClock({"node-a": 1, "node-b": 1}).increment("node-a")
        merged_state = {"count": 25, "merged": True}

        wal.append_merged(merged_version, merged_state)

        assert wal.current_version == merged_version
        state = wal.get_current_state()
        assert state["count"] == 25
        assert state["merged"] is True

    def test_append_merged_tracks_delta(self):
        wal = WAL(actor_id="counter-1", node_id="node-a")
        wal.append({"count": 10, "name": "test"})

        merged_version = VectorClock({"node-a": 2})
        merged_state = {"count": 20, "name": "test"}

        wal.append_merged(merged_version, merged_state)

        last_entry = wal.entries[-1]
        assert last_entry.delta == {"count": 20}

    def test_append_merged_handles_removed_keys(self):
        wal = WAL(actor_id="counter-1", node_id="node-a")
        wal.append({"count": 10, "temp": "remove-me"})

        merged_version = VectorClock({"node-a": 2})
        merged_state = {"count": 10}

        wal.append_merged(merged_version, merged_state)

        last_entry = wal.entries[-1]
        assert last_entry.delta.get("temp") is None

    def test_compaction(self):
        wal = WAL(actor_id="counter-1", node_id="node-a", max_entries=10)

        for i in range(15):
            wal.append({"count": i})

        assert len(wal.entries) <= 10
        assert wal.get_current_state()["count"] == 14

    def test_compaction_preserves_state(self):
        wal = WAL(actor_id="counter-1", node_id="node-a", max_entries=4)

        wal.append({"a": 1})
        wal.append({"b": 2})
        wal.append({"c": 3})
        wal.append({"d": 4})
        wal.append({"e": 5})

        state = wal.get_current_state()
        assert state == {"a": 1, "b": 2, "c": 3, "d": 4, "e": 5}

    def test_three_way_merge_scenario(self):
        wal_a = WAL(actor_id="counter-1", node_id="node-a")
        base_state = {"count": 0}

        v1 = wal_a.append(base_state)

        wal_b = WAL(
            actor_id="counter-1",
            node_id="node-b",
            base_version=v1,
            base_snapshot=wal_a.get_state_at(v1)
        )

        v2_a = wal_a.append({"count": 10})
        v2_b = wal_b.append({"count": 5})

        assert v2_a.concurrent(v2_b)

        result = wal_a.find_base(v2_b)
        assert result is not None
        base_version, base_state = result
        assert base_version == v1
