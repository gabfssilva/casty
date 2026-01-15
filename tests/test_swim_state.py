"""Tests for SWIM+ state structures."""

import pytest
import time
from math import log

from casty.swim.state import (
    MemberState,
    MemberInfo,
    SuspicionState,
    ProbeList,
    calculate_min_suspicion_timeout,
    calculate_max_suspicion_timeout,
)


class TestMemberState:
    """Tests for MemberState enum."""

    def test_states_exist(self):
        assert MemberState.ALIVE.value == "alive"
        assert MemberState.SUSPECTED.value == "suspected"
        assert MemberState.DEAD.value == "dead"


class TestMemberInfo:
    """Tests for MemberInfo dataclass."""

    def test_create_member_info(self):
        info = MemberInfo(
            node_id="node-1",
            address=("127.0.0.1", 7946),
        )
        assert info.node_id == "node-1"
        assert info.address == ("127.0.0.1", 7946)
        assert info.state == MemberState.ALIVE
        assert info.incarnation == 0

    def test_is_alive(self):
        info = MemberInfo("node-1", ("127.0.0.1", 7946))
        assert info.is_alive()
        assert not info.is_suspected()
        assert not info.is_dead()

    def test_is_suspected(self):
        info = MemberInfo("node-1", ("127.0.0.1", 7946), state=MemberState.SUSPECTED)
        assert not info.is_alive()
        assert info.is_suspected()
        assert not info.is_dead()

    def test_is_dead(self):
        info = MemberInfo("node-1", ("127.0.0.1", 7946), state=MemberState.DEAD)
        assert not info.is_alive()
        assert not info.is_suspected()
        assert info.is_dead()

    def test_update_last_seen(self):
        info = MemberInfo("node-1", ("127.0.0.1", 7946))
        old_time = info.last_seen
        time.sleep(0.01)
        info.update_last_seen()
        assert info.last_seen > old_time


class TestSuspicionState:
    """Tests for SuspicionState (Lifeguard)."""

    def test_create_suspicion_state(self):
        state = SuspicionState(node_id="node-1", first_reporter="node-2")
        assert state.node_id == "node-1"
        assert state.first_reporter == "node-2"
        assert state.confirmation_count == 0
        assert len(state.confirmers) == 0

    def test_add_confirmation(self):
        state = SuspicionState(node_id="node-1", first_reporter="node-2")

        # First confirmation
        assert state.add_confirmation("node-2")
        assert state.confirmation_count == 1
        assert "node-2" in state.confirmers

        # Duplicate confirmation (should return False)
        assert not state.add_confirmation("node-2")
        assert state.confirmation_count == 1

        # Second unique confirmation
        assert state.add_confirmation("node-3")
        assert state.confirmation_count == 2

    def test_calculate_timeout_no_confirmations(self):
        state = SuspicionState(
            node_id="node-1",
            first_reporter="node-2",
            min_timeout=1.0,
            max_timeout=10.0,
            confirmation_threshold=5,
        )
        # With 0 confirmations, timeout should be max
        timeout = state.calculate_timeout()
        assert timeout == 10.0

    def test_calculate_timeout_with_confirmations(self):
        state = SuspicionState(
            node_id="node-1",
            first_reporter="node-2",
            min_timeout=1.0,
            max_timeout=10.0,
            confirmation_threshold=5,
        )

        # Add confirmations and check timeout decreases
        state.add_confirmation("node-2")
        timeout_1 = state.calculate_timeout()

        state.add_confirmation("node-3")
        timeout_2 = state.calculate_timeout()

        state.add_confirmation("node-4")
        timeout_3 = state.calculate_timeout()

        # Timeout should decrease with more confirmations
        assert timeout_1 > timeout_2 > timeout_3
        assert timeout_3 > state.min_timeout

    def test_calculate_timeout_at_threshold(self):
        state = SuspicionState(
            node_id="node-1",
            first_reporter="node-2",
            min_timeout=1.0,
            max_timeout=10.0,
            confirmation_threshold=5,
        )

        # Add threshold confirmations
        for i in range(5):
            state.add_confirmation(f"node-{i+2}")

        # At threshold, timeout should be min
        timeout = state.calculate_timeout()
        assert timeout == 1.0

    def test_is_expired(self):
        state = SuspicionState(
            node_id="node-1",
            first_reporter="node-2",
            min_timeout=0.01,  # Very short for testing
            max_timeout=0.01,
        )

        assert not state.is_expired()
        time.sleep(0.02)
        assert state.is_expired()


class TestProbeList:
    """Tests for ProbeList (round-robin)."""

    def test_empty_list(self):
        plist = ProbeList()
        assert len(plist) == 0
        assert plist.next_target() is None

    def test_add_member(self):
        plist = ProbeList()
        plist.add_member("node-1")
        plist.add_member("node-2")
        assert len(plist) == 2
        assert "node-1" in plist.members
        assert "node-2" in plist.members

    def test_add_duplicate_member(self):
        plist = ProbeList()
        plist.add_member("node-1")
        plist.add_member("node-1")
        assert len(plist) == 1

    def test_remove_member(self):
        plist = ProbeList()
        plist.add_member("node-1")
        plist.add_member("node-2")
        plist.remove_member("node-1")
        assert len(plist) == 1
        assert "node-1" not in plist.members

    def test_remove_nonexistent_member(self):
        plist = ProbeList()
        plist.add_member("node-1")
        plist.remove_member("node-2")  # Should not raise
        assert len(plist) == 1

    def test_round_robin(self):
        plist = ProbeList()
        plist.add_member("node-1")
        plist.add_member("node-2")
        plist.add_member("node-3")

        # Get all members in order
        seen = set()
        for _ in range(3):
            target = plist.next_target()
            assert target is not None
            seen.add(target)

        # Should have seen all members
        assert seen == {"node-1", "node-2", "node-3"}

    def test_shuffle_after_complete_round(self):
        plist = ProbeList()
        plist.add_member("node-1")
        plist.add_member("node-2")
        plist.add_member("node-3")

        gen_0 = plist.generation

        # Complete one round
        for _ in range(3):
            plist.next_target()

        # Generation should have incremented
        assert plist.generation == gen_0 + 1


class TestSuspicionTimeoutCalculation:
    """Tests for suspicion timeout helper functions."""

    def test_calculate_min_timeout(self):
        # Small cluster
        min_t = calculate_min_suspicion_timeout(
            cluster_size=3,
            protocol_period=1.0,
            alpha=5.0,
        )
        # Should be α × log₁₀(3) × 1.0 ≈ 5 × 0.477 ≈ 2.38
        assert 2.0 < min_t < 3.0

    def test_calculate_min_timeout_large_cluster(self):
        # Large cluster
        min_t = calculate_min_suspicion_timeout(
            cluster_size=1000,
            protocol_period=1.0,
            alpha=5.0,
        )
        # Should be α × log₁₀(1000) × 1.0 = 5 × 3 = 15
        assert min_t == 15.0

    def test_calculate_min_timeout_single_node(self):
        # Edge case: single node
        min_t = calculate_min_suspicion_timeout(
            cluster_size=1,
            protocol_period=1.0,
            alpha=5.0,
        )
        # Should be α × protocol_period for size <= 1
        assert min_t == 5.0

    def test_calculate_max_timeout(self):
        max_t = calculate_max_suspicion_timeout(min_timeout=2.0, beta=6.0)
        assert max_t == 12.0
