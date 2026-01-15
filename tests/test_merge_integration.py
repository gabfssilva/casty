"""Integration tests for three-way merge in clustered environment.

Tests conflict detection, merge triggering, and state reconciliation
across distributed nodes.
"""

import asyncio
import pytest
from dataclasses import dataclass

from casty import Actor, ActorSystem, Context
from casty.merge import merge_sum, merge_lww, is_mergeable, MergeableActor


# =============================================================================
# Test Messages
# =============================================================================


@dataclass
class Deposit:
    amount: int


@dataclass
class Withdraw:
    amount: int


@dataclass
class GetBalance:
    pass


@dataclass
class SetName:
    name: str


@dataclass
class GetName:
    pass


# =============================================================================
# Test Actors
# =============================================================================


class MergeableAccount(Actor[Deposit | Withdraw | GetBalance | SetName | GetName]):
    """Account actor that implements Mergeable protocol."""

    def __init__(self, entity_id: str = "") -> None:
        self.entity_id = entity_id
        self.balance = 0
        self.name = ""
        self.last_updated = 0.0

    async def receive(
        self, msg: Deposit | Withdraw | GetBalance | SetName | GetName, ctx: Context
    ) -> None:
        match msg:
            case Deposit(amount):
                self.balance += amount
            case Withdraw(amount):
                self.balance -= amount
            case GetBalance():
                ctx.reply(self.balance)
            case SetName(name):
                import time
                self.name = name
                self.last_updated = time.time()
            case GetName():
                ctx.reply(self.name)

    def __casty_merge__(self, base: "MergeableAccount", other: "MergeableAccount") -> None:
        """Merge concurrent modifications."""
        # Sum balances using three-way merge
        self.balance = int(merge_sum(base.balance, self.balance, other.balance))
        # Last-writer-wins for name
        self.name = merge_lww(self.name, other.name, self.last_updated, other.last_updated)
        self.last_updated = max(self.last_updated, other.last_updated)


class NonMergeableCounter(Actor[Deposit | GetBalance]):
    """Counter that doesn't implement Mergeable."""

    def __init__(self, entity_id: str = "") -> None:
        self.entity_id = entity_id
        self.count = 0

    async def receive(self, msg: Deposit | GetBalance, ctx: Context) -> None:
        match msg:
            case Deposit(amount):
                self.count += amount
            case GetBalance():
                ctx.reply(self.count)


# =============================================================================
# Protocol Tests
# =============================================================================


class TestMergeableDetection:
    """Test that Mergeable actors are detected correctly."""

    def test_mergeable_actor_detected(self):
        """Actor with __casty_merge__ is detected as Mergeable."""
        account = MergeableAccount()
        assert is_mergeable(account)

    def test_non_mergeable_actor_not_detected(self):
        """Actor without __casty_merge__ is not Mergeable."""
        counter = NonMergeableCounter()
        assert not is_mergeable(counter)


# =============================================================================
# Wrapper Tests
# =============================================================================


class TestMergeableActorWrapper:
    """Test MergeableActor wrapper functionality."""

    def test_wrap_mergeable_actor(self):
        """Wrapping a Mergeable actor succeeds."""
        account = MergeableAccount()
        wrapper = MergeableActor(account)

        assert wrapper.actor is account
        assert wrapper.version == 0
        assert wrapper.base_snapshot is None

    def test_wrap_non_mergeable_raises(self):
        """Wrapping a non-Mergeable actor raises TypeError."""
        counter = NonMergeableCounter()

        with pytest.raises(TypeError) as exc_info:
            MergeableActor(counter)

        assert "must implement __casty_merge__" in str(exc_info.value)

    def test_version_increment(self):
        """Version increments correctly."""
        account = MergeableAccount()
        wrapper = MergeableActor(account)

        assert wrapper.version == 0
        wrapper.increment_version()
        assert wrapper.version == 1
        wrapper.increment_version()
        assert wrapper.version == 2

    def test_snapshot_captures_state(self):
        """Snapshot captures current state."""
        account = MergeableAccount()
        account.balance = 100
        account.name = "Test"

        wrapper = MergeableActor(account)
        wrapper.increment_version()
        wrapper.take_snapshot()

        assert wrapper.base_snapshot == {"entity_id": "", "balance": 100, "name": "Test", "last_updated": 0.0}
        assert wrapper.base_version == 1

    def test_merge_execution(self):
        """Merge executes correctly with delta calculation."""
        account = MergeableAccount()
        account.balance = 120  # Started at 100, added 20

        wrapper = MergeableActor(account)
        wrapper._state.version = 2

        # Base state (before divergence)
        base_state = {"entity_id": "", "balance": 100, "name": "", "last_updated": 0.0}

        # Other node's state (also added 30 from base of 100)
        other_state = {"entity_id": "", "balance": 130, "name": "", "last_updated": 0.0}
        other_version = 2

        wrapper.execute_merge(base_state, other_state, other_version)

        # Should be: base(100) + delta_mine(20) + delta_other(30) = 150
        assert account.balance == 150
        # Version should be max(2, 2) + 1 = 3
        assert wrapper.version == 3


# =============================================================================
# Single Node Integration Tests
# =============================================================================


@pytest.mark.asyncio
async def test_mergeable_entity_single_node():
    """Test that Mergeable entities work on a single clustered node."""
    async with ActorSystem.clustered() as system:
        await asyncio.sleep(0.3)  # Wait for cluster to initialize

        # Register the sharded type
        accounts = await system.spawn(MergeableAccount, name="accounts", sharded=True)

        # Send deposit
        await accounts["acc-1"].send(Deposit(100))
        await asyncio.sleep(0.1)

        # Check balance
        balance = await accounts["acc-1"].ask(GetBalance())
        assert balance == 100

        # Send more
        await accounts["acc-1"].send(Deposit(50))
        await asyncio.sleep(0.1)

        balance = await accounts["acc-1"].ask(GetBalance())
        assert balance == 150


@pytest.mark.asyncio
async def test_non_mergeable_entity_single_node():
    """Test that non-Mergeable entities still work."""
    async with ActorSystem.clustered() as system:
        await asyncio.sleep(0.3)  # Wait for cluster to initialize

        # Register the sharded type
        counters = await system.spawn(NonMergeableCounter, name="counters", sharded=True)

        # Send increment
        await counters["counter-1"].send(Deposit(10))
        await asyncio.sleep(0.1)

        # Check value
        value = await counters["counter-1"].ask(GetBalance())
        assert value == 10


# =============================================================================
# Merge Logic Tests
# =============================================================================


class TestMergeLogic:
    """Test the merge logic itself."""

    def test_concurrent_deposits_merge_correctly(self):
        """Concurrent deposits should sum correctly."""
        # Setup: Two actors starting from same base state
        account1 = MergeableAccount()
        account1.balance = 120  # Base was 100, deposited 20

        account2 = MergeableAccount()
        account2.balance = 130  # Base was 100, deposited 30

        base = MergeableAccount()
        base.balance = 100

        # Execute merge on account1
        account1.__casty_merge__(base, account2)

        # Result should be: 100 + 20 + 30 = 150
        assert account1.balance == 150

    def test_concurrent_deposits_and_withdrawals(self):
        """Mixed deposits and withdrawals should merge correctly."""
        account1 = MergeableAccount()
        account1.balance = 80  # Base was 100, withdrew 20

        account2 = MergeableAccount()
        account2.balance = 130  # Base was 100, deposited 30

        base = MergeableAccount()
        base.balance = 100

        account1.__casty_merge__(base, account2)

        # Result: 100 + (-20) + 30 = 110
        assert account1.balance == 110

    def test_lww_merge_for_names(self):
        """Last-writer-wins for name field."""
        import time

        account1 = MergeableAccount()
        account1.name = "Alice"
        account1.last_updated = time.time()

        account2 = MergeableAccount()
        account2.name = "Bob"
        account2.last_updated = time.time() + 1  # Later timestamp

        base = MergeableAccount()
        base.name = ""

        account1.__casty_merge__(base, account2)

        # Bob should win (later timestamp)
        assert account1.name == "Bob"


# =============================================================================
# Version Tracking Tests
# =============================================================================


class TestVersionTracking:
    """Test version tracking for conflict detection."""

    def test_version_needs_merge_concurrent(self):
        """Concurrent versions need merge."""
        from casty.merge import ActorVersion

        v1 = ActorVersion(2, "node-1")
        v2 = ActorVersion(2, "node-2")

        assert v1.needs_merge(v2)
        assert v2.needs_merge(v1)

    def test_version_no_merge_ordered(self):
        """Ordered versions don't need merge."""
        from casty.merge import ActorVersion

        v1 = ActorVersion(1, "node-1")
        v2 = ActorVersion(2, "node-1")

        assert not v1.needs_merge(v2)
        assert not v2.needs_merge(v1)

    def test_merge_version_creation(self):
        """Merged version is max + 1."""
        from casty.merge import ActorVersion

        v1 = ActorVersion(3, "node-1")
        v2 = ActorVersion(5, "node-2")

        merged = v1.merge_version(v2, "node-1")
        assert merged.version == 6  # max(3, 5) + 1
        assert merged.node_id == "node-1"
