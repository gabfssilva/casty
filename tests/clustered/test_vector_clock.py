import pytest
from casty.cluster.vector_clock import VectorClock


class TestVectorClockIncrement:
    def test_increment_new_node(self):
        clock = VectorClock()
        clock.increment("node-1")
        assert clock.versions == {"node-1": 1}

    def test_increment_existing_node(self):
        clock = VectorClock({"node-1": 5})
        clock.increment("node-1")
        assert clock.versions == {"node-1": 6}

    def test_increment_multiple_nodes(self):
        clock = VectorClock()
        clock.increment("node-1")
        clock.increment("node-2")
        clock.increment("node-1")
        assert clock.versions == {"node-1": 2, "node-2": 1}


class TestVectorClockMerge:
    def test_merge_disjoint(self):
        a = VectorClock({"A": 2})
        b = VectorClock({"B": 3})
        a.merge(b)
        assert a.versions == {"A": 2, "B": 3}

    def test_merge_overlapping(self):
        a = VectorClock({"A": 2, "B": 1})
        b = VectorClock({"A": 1, "B": 3})
        a.merge(b)
        assert a.versions == {"A": 2, "B": 3}

    def test_merge_does_not_modify_other(self):
        a = VectorClock({"A": 2})
        b = VectorClock({"B": 3})
        a.merge(b)
        assert b.versions == {"B": 3}


class TestVectorClockCompare:
    def test_compare_before(self):
        a = VectorClock({"A": 1, "B": 1})
        b = VectorClock({"A": 2, "B": 2})
        assert a.compare(b) == "before"

    def test_compare_after(self):
        a = VectorClock({"A": 2, "B": 2})
        b = VectorClock({"A": 1, "B": 1})
        assert a.compare(b) == "after"

    def test_compare_concurrent(self):
        a = VectorClock({"A": 2, "B": 1})
        b = VectorClock({"A": 1, "B": 2})
        assert a.compare(b) == "concurrent"

    def test_compare_equal(self):
        a = VectorClock({"A": 1, "B": 1})
        b = VectorClock({"A": 1, "B": 1})
        assert a.compare(b) == "concurrent"

    def test_compare_with_missing_keys(self):
        a = VectorClock({"A": 1})
        b = VectorClock({"A": 1, "B": 1})
        assert a.compare(b) == "before"


class TestVectorClockMeet:
    def test_meet_basic(self):
        a = VectorClock({"A": 3, "B": 1})
        b = VectorClock({"A": 2, "B": 2})
        meet = a.meet(b)
        assert meet.versions == {"A": 2, "B": 1}

    def test_meet_disjoint(self):
        a = VectorClock({"A": 3})
        b = VectorClock({"B": 2})
        meet = a.meet(b)
        assert meet.versions == {"A": 0, "B": 0}


class TestVectorClockCopy:
    def test_copy_is_independent(self):
        a = VectorClock({"A": 1})
        b = a.copy()
        b.increment("A")
        assert a.versions == {"A": 1}
        assert b.versions == {"A": 2}


class TestVectorClockIsBeforeOrEqual:
    def test_is_before(self):
        a = VectorClock({"A": 1})
        b = VectorClock({"A": 2})
        assert a.is_before_or_equal(b) is True

    def test_is_equal(self):
        a = VectorClock({"A": 1})
        b = VectorClock({"A": 1})
        assert a.is_before_or_equal(b) is True

    def test_is_after(self):
        a = VectorClock({"A": 2})
        b = VectorClock({"A": 1})
        assert a.is_before_or_equal(b) is False

    def test_is_concurrent(self):
        a = VectorClock({"A": 2, "B": 1})
        b = VectorClock({"A": 1, "B": 2})
        assert a.is_before_or_equal(b) is False
