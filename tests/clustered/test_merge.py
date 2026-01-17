import pytest

from casty.cluster import deserialize
from casty.wal import VectorClock, WALEntry


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
