import pytest
from casty.cluster.conflict import detect_conflict, ConflictResult
from casty.cluster.vector_clock import VectorClock


class TestConflictDetection:
    def test_no_conflict_local_behind(self):
        local = VectorClock({"A": 1})
        remote = VectorClock({"A": 2})

        result = detect_conflict(local, remote)

        assert result == ConflictResult.ACCEPT_REMOTE

    def test_no_conflict_local_ahead(self):
        local = VectorClock({"A": 2})
        remote = VectorClock({"A": 1})

        result = detect_conflict(local, remote)

        assert result == ConflictResult.IGNORE

    def test_conflict_concurrent(self):
        local = VectorClock({"A": 2, "B": 1})
        remote = VectorClock({"A": 1, "B": 2})

        result = detect_conflict(local, remote)

        assert result == ConflictResult.MERGE

    def test_equal_clocks_no_conflict(self):
        local = VectorClock({"A": 1})
        remote = VectorClock({"A": 1})

        result = detect_conflict(local, remote)

        assert result == ConflictResult.IGNORE  # already in sync
