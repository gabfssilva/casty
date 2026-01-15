"""Tests for three-way merge functionality.

Tests the Mergeable protocol, MergeableActor wrapper,
version tracking, and merge helpers.
"""

import pytest
from dataclasses import dataclass

from casty import Actor, Context
from casty.merge import (
    Mergeable,
    MergeableActor,
    ActorVersion,
    is_mergeable,
    merge_sum,
    merge_max,
    merge_min,
    merge_lww,
    merge_union,
    merge_intersection,
    merge_set_add_remove,
    merge_list_append,
    merge_dict_shallow,
)


# =============================================================================
# Test Actor (implements Mergeable)
# =============================================================================


@dataclass
class Deposit:
    amount: int


@dataclass
class GetBalance:
    pass


class Account(Actor[Deposit | GetBalance]):
    """Test actor that implements Mergeable protocol."""

    def __init__(self) -> None:
        self.balance = 0
        self.name = ""
        self.last_updated = 0.0

    async def receive(self, msg: Deposit | GetBalance, ctx: Context) -> None:
        match msg:
            case Deposit(amount):
                self.balance += amount
            case GetBalance():
                ctx.reply(self.balance)

    def __casty_merge__(self, base: "Account", other: "Account") -> None:
        """Merge by summing balance deltas."""
        self.balance = int(merge_sum(base.balance, self.balance, other.balance))
        self.name = merge_lww(self.name, other.name, self.last_updated, other.last_updated)
        self.last_updated = max(self.last_updated, other.last_updated)


class NonMergeableActor(Actor[Deposit]):
    """Actor without __casty_merge__."""

    def __init__(self) -> None:
        self.value = 0

    async def receive(self, msg: Deposit, ctx: Context) -> None:
        self.value += msg.amount


# =============================================================================
# Protocol Tests
# =============================================================================


class TestMergeableProtocol:
    """Test the Mergeable protocol detection."""

    def test_mergeable_actor_is_detected(self):
        """Actor with __casty_merge__ is detected as Mergeable."""
        account = Account()
        assert is_mergeable(account)
        assert isinstance(account, Mergeable)

    def test_non_mergeable_actor_is_not_detected(self):
        """Actor without __casty_merge__ is not Mergeable."""
        actor = NonMergeableActor()
        assert not is_mergeable(actor)
        assert not isinstance(actor, Mergeable)


# =============================================================================
# Version Tests
# =============================================================================


class TestActorVersion:
    """Test ActorVersion behavior."""

    def test_happens_before(self):
        """Test happens_before ordering."""
        v1 = ActorVersion(1, "node-1")
        v2 = ActorVersion(2, "node-1")
        v3 = ActorVersion(2, "node-2")

        assert v1.happens_before(v2)
        assert v1.happens_before(v3)
        assert not v2.happens_before(v1)
        assert not v2.happens_before(v3)  # Same version

    def test_concurrent_with(self):
        """Test concurrent detection."""
        v1 = ActorVersion(2, "node-1")
        v2 = ActorVersion(2, "node-2")
        v3 = ActorVersion(2, "node-1")

        assert v1.concurrent_with(v2)
        assert v2.concurrent_with(v1)
        assert not v1.concurrent_with(v3)  # Same node

    def test_needs_merge(self):
        """Test merge detection."""
        v1 = ActorVersion(2, "node-1")
        v2 = ActorVersion(2, "node-2")
        v3 = ActorVersion(3, "node-1")
        v4 = ActorVersion(2, "node-1")

        # Concurrent versions need merge
        assert v1.needs_merge(v2)
        assert v2.needs_merge(v1)

        # Clear ordering doesn't need merge
        assert not v1.needs_merge(v3)  # v1 < v3
        assert not v3.needs_merge(v1)  # v3 > v1

        # Identical versions don't need merge
        assert not v1.needs_merge(v4)

    def test_merge_version(self):
        """Test merged version creation."""
        v1 = ActorVersion(3, "node-1")
        v2 = ActorVersion(5, "node-2")

        merged = v1.merge_version(v2, "node-1")
        assert merged.version == 6  # max(3, 5) + 1
        assert merged.node_id == "node-1"

    def test_increment(self):
        """Test version increment."""
        v1 = ActorVersion(1, "node-1")
        v2 = v1.increment()
        v3 = v1.increment("node-2")

        assert v2.version == 2
        assert v2.node_id == "node-1"
        assert v3.version == 2
        assert v3.node_id == "node-2"


# =============================================================================
# MergeableActor Wrapper Tests
# =============================================================================


class TestMergeableActorWrapper:
    """Test the MergeableActor wrapper."""

    def test_wrapping_mergeable_actor(self):
        """Wrapper accepts Mergeable actors."""
        account = Account()
        wrapper = MergeableActor(account)

        assert wrapper.actor is account
        assert wrapper.version == 0
        assert wrapper.base_snapshot is None

    def test_wrapping_non_mergeable_raises(self):
        """Wrapper rejects non-Mergeable actors."""
        actor = NonMergeableActor()

        with pytest.raises(TypeError) as exc_info:
            MergeableActor(actor)

        assert "must implement __casty_merge__" in str(exc_info.value)

    def test_version_increment(self):
        """Version increments correctly."""
        account = Account()
        wrapper = MergeableActor(account)

        assert wrapper.version == 0
        wrapper.increment_version()
        assert wrapper.version == 1
        wrapper.increment_version()
        assert wrapper.version == 2

    def test_take_snapshot(self):
        """Snapshot captures current state."""
        account = Account()
        account.balance = 100
        account.name = "Test"
        wrapper = MergeableActor(account)
        wrapper.increment_version()
        wrapper.increment_version()

        wrapper.take_snapshot()

        assert wrapper.base_snapshot == {"balance": 100, "name": "Test", "last_updated": 0.0}
        assert wrapper.base_version == 2

    def test_execute_merge(self):
        """Merge executes correctly."""
        account = Account()
        account.balance = 120  # Started at 100, added 20
        wrapper = MergeableActor(account)
        wrapper._state.version = 2

        # Base state (before divergence)
        base_state = {"balance": 100, "name": "", "last_updated": 0.0}

        # Other node's state (also added 30)
        other_state = {"balance": 130, "name": "", "last_updated": 0.0}
        other_version = 2

        wrapper.execute_merge(base_state, other_state, other_version)

        # Should be 100 + 20 + 30 = 150
        assert account.balance == 150
        # Version should be max(2, 2) + 1 = 3
        assert wrapper.version == 3

    def test_get_merge_info(self):
        """Get all merge info at once."""
        account = Account()
        account.balance = 50
        wrapper = MergeableActor(account)
        wrapper._state.version = 3
        wrapper.take_snapshot()
        account.balance = 75
        wrapper.increment_version()

        version, base_version, state, base_snapshot = wrapper.get_merge_info()

        assert version == 4
        assert base_version == 3
        assert state == {"balance": 75, "name": "", "last_updated": 0.0}
        assert base_snapshot == {"balance": 50, "name": "", "last_updated": 0.0}


# =============================================================================
# Merge Helper Tests
# =============================================================================


class TestMergeSum:
    """Test merge_sum helper."""

    def test_basic_sum(self):
        """Sum deltas from base."""
        base = 100
        mine = 120  # +20
        theirs = 130  # +30
        assert merge_sum(base, mine, theirs) == 150

    def test_negative_deltas(self):
        """Handle negative deltas (withdrawals)."""
        base = 100
        mine = 80  # -20
        theirs = 90  # -10
        assert merge_sum(base, mine, theirs) == 70

    def test_mixed_deltas(self):
        """Handle mixed positive and negative."""
        base = 100
        mine = 120  # +20
        theirs = 90  # -10
        assert merge_sum(base, mine, theirs) == 110

    def test_floats(self):
        """Works with floats."""
        assert merge_sum(10.0, 15.5, 12.3) == 17.8


class TestMergeMaxMin:
    """Test merge_max and merge_min helpers."""

    def test_max(self):
        """Take maximum value."""
        assert merge_max(0, 10, 20) == 20
        assert merge_max(0, 20, 10) == 20
        assert merge_max(0, 15, 15) == 15

    def test_min(self):
        """Take minimum value."""
        assert merge_min(0, 10, 20) == 10
        assert merge_min(0, 20, 10) == 10
        assert merge_min(0, 15, 15) == 15


class TestMergeLWW:
    """Test Last-Writer-Wins merge."""

    def test_theirs_wins(self):
        """Higher timestamp wins."""
        assert merge_lww("mine", "theirs", 1.0, 2.0) == "theirs"

    def test_mine_wins(self):
        """Lower timestamp loses."""
        assert merge_lww("mine", "theirs", 2.0, 1.0) == "mine"

    def test_equal_timestamps(self):
        """Equal timestamps: mine wins (tie-breaker)."""
        assert merge_lww("mine", "theirs", 1.0, 1.0) == "mine"


class TestMergeUnion:
    """Test set union merge."""

    def test_basic_union(self):
        """Union combines all elements."""
        mine = {1, 2, 3}
        theirs = {3, 4, 5}
        assert merge_union(mine, theirs) == {1, 2, 3, 4, 5}

    def test_disjoint_sets(self):
        """Disjoint sets are combined."""
        assert merge_union({1, 2}, {3, 4}) == {1, 2, 3, 4}

    def test_identical_sets(self):
        """Identical sets stay the same."""
        assert merge_union({1, 2}, {1, 2}) == {1, 2}


class TestMergeIntersection:
    """Test set intersection merge."""

    def test_basic_intersection(self):
        """Intersection keeps common elements."""
        mine = {1, 2, 3}
        theirs = {2, 3, 4}
        assert merge_intersection(mine, theirs) == {2, 3}

    def test_disjoint_sets(self):
        """Disjoint sets become empty."""
        assert merge_intersection({1, 2}, {3, 4}) == set()


class TestMergeSetAddRemove:
    """Test set add/remove merge."""

    def test_adds_from_both(self):
        """Adds from both sides are kept."""
        base = {1, 2}
        mine = {1, 2, 3}  # Added 3
        theirs = {1, 2, 4}  # Added 4
        assert merge_set_add_remove(base, mine, theirs) == {1, 2, 3, 4}

    def test_removes_only_if_both(self):
        """Removal only if both removed."""
        base = {1, 2, 3}
        mine = {1, 3}  # Removed 2
        theirs = {1, 2}  # Removed 3
        # Only common removes are applied
        assert merge_set_add_remove(base, mine, theirs) == {1, 2, 3}

    def test_both_remove_same(self):
        """Both remove same element."""
        base = {1, 2, 3}
        mine = {1, 2}  # Removed 3
        theirs = {1, 2}  # Also removed 3
        assert merge_set_add_remove(base, mine, theirs) == {1, 2}


class TestMergeListAppend:
    """Test list append merge."""

    def test_append_new_from_both(self):
        """Append new items from both sides."""
        base = [1, 2]
        mine = [1, 2, 3]
        theirs = [1, 2, 4]
        assert merge_list_append(base, mine, theirs) == [1, 2, 3, 4]

    def test_deduplicate_new(self):
        """Deduplicate items added by both."""
        base = [1]
        mine = [1, 2, 3]
        theirs = [1, 2, 4]  # Both added 2
        assert merge_list_append(base, mine, theirs) == [1, 2, 3, 4]


class TestMergeDictShallow:
    """Test shallow dictionary merge."""

    def test_different_keys_added(self):
        """Different keys are combined."""
        base = {"a": 1}
        mine = {"a": 1, "b": 2}
        theirs = {"a": 1, "c": 3}
        assert merge_dict_shallow(base, mine, theirs) == {"a": 1, "b": 2, "c": 3}

    def test_both_modify_same_key(self):
        """Both modify same key: theirs wins."""
        base = {"a": 1}
        mine = {"a": 2}
        theirs = {"a": 3}
        assert merge_dict_shallow(base, mine, theirs) == {"a": 3}

    def test_one_modifies_one_keeps(self):
        """One modifies, one keeps original."""
        base = {"a": 1}
        mine = {"a": 2}
        theirs = {"a": 1}
        assert merge_dict_shallow(base, mine, theirs) == {"a": 2}

    def test_modification_wins_over_delete(self):
        """Modification wins over deletion."""
        base = {"a": 1, "b": 2}
        mine = {"a": 1}  # Deleted b
        theirs = {"a": 1, "b": 3}  # Modified b
        assert merge_dict_shallow(base, mine, theirs) == {"a": 1, "b": 3}

    def test_both_delete(self):
        """Both delete same key."""
        base = {"a": 1, "b": 2}
        mine = {"a": 1}  # Deleted b
        theirs = {"a": 1}  # Also deleted b
        assert merge_dict_shallow(base, mine, theirs) == {"a": 1}
