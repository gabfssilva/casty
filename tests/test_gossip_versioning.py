"""Tests for VectorClock and versioning."""

import pytest

from casty.gossip.versioning import VectorClock, VersionedValue


class TestVectorClock:
    """Tests for VectorClock."""

    def test_empty_clock(self):
        """Empty clock has no entries."""
        clock = VectorClock()
        assert clock.entries == ()
        assert clock.get("node-1") == 0

    def test_increment(self):
        """Increment creates/updates entry."""
        clock = VectorClock()
        clock = clock.increment("node-1")
        assert clock.get("node-1") == 1

        clock = clock.increment("node-1")
        assert clock.get("node-1") == 2

        clock = clock.increment("node-2")
        assert clock.get("node-1") == 2
        assert clock.get("node-2") == 1

    def test_merge(self):
        """Merge takes maximum of each entry."""
        clock1 = VectorClock().increment("node-1").increment("node-1")
        clock2 = VectorClock().increment("node-2").increment("node-2").increment("node-2")

        merged = clock1.merge(clock2)
        assert merged.get("node-1") == 2
        assert merged.get("node-2") == 3

        # Merge with overlapping entries
        clock3 = VectorClock().increment("node-1")
        merged2 = clock1.merge(clock3)
        assert merged2.get("node-1") == 2

    def test_happens_before(self):
        """Test causal ordering."""
        clock1 = VectorClock().increment("node-1")
        clock2 = clock1.increment("node-1")

        # clock1 < clock2
        assert clock1.happens_before(clock2)
        assert not clock2.happens_before(clock1)

        # Same clock
        assert not clock1.happens_before(clock1)

    def test_concurrent(self):
        """Test concurrent detection."""
        # Two independent updates
        clock1 = VectorClock().increment("node-1")
        clock2 = VectorClock().increment("node-2")

        assert clock1.concurrent_with(clock2)
        assert clock2.concurrent_with(clock1)
        assert not clock1.happens_before(clock2)
        assert not clock2.happens_before(clock1)

    def test_not_concurrent_when_causal(self):
        """Causal clocks are not concurrent."""
        clock1 = VectorClock().increment("node-1")
        clock2 = clock1.increment("node-2")

        assert not clock1.concurrent_with(clock2)
        assert clock1.happens_before(clock2)

    def test_equality(self):
        """Test clock equality."""
        clock1 = VectorClock().increment("node-1")
        clock2 = VectorClock().increment("node-1")

        assert clock1 == clock2
        assert hash(clock1) == hash(clock2)

    def test_to_dict_from_dict(self):
        """Test serialization roundtrip."""
        clock = VectorClock().increment("node-1").increment("node-2")
        d = clock.to_dict()
        restored = VectorClock.from_dict(d)
        assert restored == clock

    def test_entries_sorted(self):
        """Entries are sorted by node_id."""
        clock = VectorClock()
        clock = clock.increment("node-2")
        clock = clock.increment("node-1")
        clock = clock.increment("node-3")

        keys = [k for k, _ in clock.entries]
        assert keys == sorted(keys)


class TestVersionedValue:
    """Tests for VersionedValue."""

    def test_create(self):
        """Create versioned value."""
        clock = VectorClock().increment("node-1")
        vv = VersionedValue(
            value="test",
            clock=clock,
            origin_node="node-1",
            timestamp=1234567890.0,
        )

        assert vv.value == "test"
        assert vv.clock == clock
        assert vv.origin_node == "node-1"
        assert vv.timestamp == 1234567890.0

    def test_immutable(self):
        """Versioned values are immutable."""
        clock = VectorClock()
        vv = VersionedValue(
            value="test",
            clock=clock,
            origin_node="node-1",
            timestamp=0.0,
        )

        with pytest.raises(AttributeError):
            vv.value = "changed"
