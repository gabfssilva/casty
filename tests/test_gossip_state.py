"""Tests for gossip state data structures."""

import pytest

from casty.gossip.state import StateEntry, StateDigest, DigestDiff, EntryStatus
from casty.gossip.versioning import VectorClock


class TestStateEntry:
    """Tests for StateEntry."""

    def test_create_entry(self):
        """Create a state entry."""
        clock = VectorClock().increment("node-1")
        entry = StateEntry(
            key="test/key",
            value=b"test value",
            version=clock,
            status=EntryStatus.ACTIVE,
            ttl=60.0,
            origin="node-1",
            timestamp=1234567890.0,
        )

        assert entry.key == "test/key"
        assert entry.value == b"test value"
        assert entry.version == clock
        assert entry.status == EntryStatus.ACTIVE
        assert entry.ttl == 60.0
        assert entry.origin == "node-1"
        assert not entry.is_deleted()

    def test_deleted_entry(self):
        """Deleted entry (tombstone)."""
        clock = VectorClock().increment("node-1")
        entry = StateEntry(
            key="test/key",
            value=b"",
            version=clock,
            status=EntryStatus.DELETED,
        )

        assert entry.is_deleted()

    def test_with_status(self):
        """Change entry status."""
        clock = VectorClock().increment("node-1")
        entry = StateEntry(
            key="test/key",
            value=b"test",
            version=clock,
            status=EntryStatus.ACTIVE,
        )

        deleted = entry.with_status(EntryStatus.DELETED)
        assert deleted.is_deleted()
        assert deleted.key == entry.key
        assert deleted.value == entry.value

    def test_immutable(self):
        """Entries are immutable."""
        clock = VectorClock()
        entry = StateEntry(
            key="test",
            value=b"test",
            version=clock,
        )

        with pytest.raises(AttributeError):
            entry.key = "changed"


class TestStateDigest:
    """Tests for StateDigest."""

    def test_create_digest(self):
        """Create a state digest."""
        clock1 = VectorClock().increment("node-1")
        clock2 = VectorClock().increment("node-2")

        digest = StateDigest(
            entries=(("key1", clock1), ("key2", clock2)),
            node_id="node-1",
            generation=5,
        )

        assert digest.node_id == "node-1"
        assert digest.generation == 5
        assert len(digest.entries) == 2

    def test_keys(self):
        """Get keys from digest."""
        clock = VectorClock().increment("node-1")
        digest = StateDigest(
            entries=(("key1", clock), ("key2", clock), ("key3", clock)),
            node_id="node-1",
        )

        keys = digest.keys()
        assert keys == {"key1", "key2", "key3"}

    def test_get_version(self):
        """Get version for specific key."""
        clock1 = VectorClock().increment("node-1")
        clock2 = clock1.increment("node-1")

        digest = StateDigest(
            entries=(("key1", clock1), ("key2", clock2)),
            node_id="node-1",
        )

        assert digest.get_version("key1") == clock1
        assert digest.get_version("key2") == clock2
        assert digest.get_version("missing") is None


class TestDigestDiff:
    """Tests for DigestDiff."""

    def test_create_diff(self):
        """Create a digest diff."""
        diff = DigestDiff(
            keys_i_need=("key1", "key2"),
            keys_peer_needs=("key3",),
        )

        assert diff.keys_i_need == ("key1", "key2")
        assert diff.keys_peer_needs == ("key3",)

    def test_is_empty(self):
        """Empty diff check."""
        empty_diff = DigestDiff(
            keys_i_need=(),
            keys_peer_needs=(),
        )
        assert empty_diff.is_empty()

        non_empty = DigestDiff(
            keys_i_need=("key1",),
            keys_peer_needs=(),
        )
        assert not non_empty.is_empty()
