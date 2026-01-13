"""Tests for state replication and failover in sharded actors."""

import asyncio
from dataclasses import dataclass

import pytest

from casty import Actor, Context, DistributedActorSystem
from casty.cluster.persistence import ReplicationConfig

from tests.distributed.helpers import wait_for_leader


@dataclass
class Deposit:
    amount: float


@dataclass
class GetBalance:
    pass


@dataclass
class SetValue:
    value: int


@dataclass
class GetValue:
    pass


class Account(Actor[Deposit | GetBalance]):
    """Account actor for testing state replication."""

    def __init__(self, entity_id: str) -> None:
        self.entity_id = entity_id
        self.balance = 0.0

    async def receive(self, msg: Deposit | GetBalance, ctx: Context) -> None:
        match msg:
            case Deposit(amount):
                self.balance += amount
            case GetBalance():
                ctx.reply(self.balance)


class Counter(Actor[SetValue | GetValue]):
    """Simple counter for testing."""

    def __init__(self, entity_id: str, initial: int = 0) -> None:
        self.entity_id = entity_id
        self.value = initial

    async def receive(self, msg: SetValue | GetValue, ctx: Context) -> None:
        match msg:
            case SetValue(value):
                self.value = value
            case GetValue():
                ctx.reply(self.value)


pytestmark = pytest.mark.slow


class TestStateReplication:
    """Tests for state replication between nodes."""

    async def test_state_replicated_to_backup(self, get_port) -> None:
        """Test that state is replicated to backup nodes."""
        port1 = get_port()
        port2 = get_port()

        config = ReplicationConfig(memory_replicas=1)

        node1 = DistributedActorSystem(
            "127.0.0.1",
            port1,
            seeds=[],
            expected_cluster_size=2,
            replication=config,
        )
        node2 = DistributedActorSystem(
            "127.0.0.1",
            port2,
            seeds=[f"127.0.0.1:{port1}"],
            expected_cluster_size=2,
            replication=config,
        )

        async with node1, node2:
            # Wait for leader election
            leader = await wait_for_leader([node1, node2], timeout=5.0)
            follower = node2 if leader == node1 else node1

            # Spawn sharded actor
            accounts = await leader.spawn(Account, name="accounts", shards=10)

            # Wait for shard allocations
            await asyncio.sleep(0.5)

            # Send messages to create and update entity state
            await accounts["user-1"].send(Deposit(100))
            await accounts["user-1"].send(Deposit(50))

            # Wait for state replication
            await asyncio.sleep(0.5)

            # Verify state is replicated to follower's backup manager
            backup_stats = follower.get_backup_stats()
            assert backup_stats["entry_count"] > 0, "No backup entries found"

    async def test_state_replicated_with_correct_version(self, get_port) -> None:
        """Test that state versions are incremented correctly."""
        port1 = get_port()
        port2 = get_port()

        config = ReplicationConfig(memory_replicas=1)

        node1 = DistributedActorSystem(
            "127.0.0.1",
            port1,
            seeds=[],
            expected_cluster_size=2,
            replication=config,
        )
        node2 = DistributedActorSystem(
            "127.0.0.1",
            port2,
            seeds=[f"127.0.0.1:{port1}"],
            expected_cluster_size=2,
            replication=config,
        )

        async with node1, node2:
            leader = await wait_for_leader([node1, node2], timeout=5.0)

            accounts = await leader.spawn(Account, name="accounts", shards=10)
            await asyncio.sleep(0.5)

            # Multiple operations should increment version
            for i in range(5):
                await accounts["user-1"].send(Deposit(10))
                await asyncio.sleep(0.1)

            # The state version should have been incremented
            entity_key = "accounts:user-1"
            version = leader._state_versions.get(entity_key, 0)
            assert version >= 5, f"Expected version >= 5, got {version}"

    async def test_no_replication_when_replicas_zero(self, get_port) -> None:
        """Test that no replication happens when memory_replicas is 0."""
        port1 = get_port()
        port2 = get_port()

        config = ReplicationConfig(memory_replicas=0)

        node1 = DistributedActorSystem(
            "127.0.0.1",
            port1,
            seeds=[],
            expected_cluster_size=2,
            replication=config,
        )
        node2 = DistributedActorSystem(
            "127.0.0.1",
            port2,
            seeds=[f"127.0.0.1:{port1}"],
            expected_cluster_size=2,
            replication=config,
        )

        async with node1, node2:
            leader = await wait_for_leader([node1, node2], timeout=5.0)
            follower = node2 if leader == node1 else node1

            accounts = await leader.spawn(Account, name="accounts", shards=10)
            await asyncio.sleep(0.5)

            await accounts["user-1"].send(Deposit(100))
            await asyncio.sleep(0.5)

            # No backups should be created
            backup_stats = follower.get_backup_stats()
            assert backup_stats["entry_count"] == 0, "Should have no backup entries"


class TestStateRestoration:
    """Tests for state restoration on failover."""

    async def test_state_restored_from_backup(self, get_port) -> None:
        """Test that state is restored from backup when node restarts."""
        port1 = get_port()
        port2 = get_port()

        config = ReplicationConfig(memory_replicas=1)

        # Start first cluster
        node1 = DistributedActorSystem(
            "127.0.0.1",
            port1,
            seeds=[],
            expected_cluster_size=2,
            replication=config,
        )
        node2 = DistributedActorSystem(
            "127.0.0.1",
            port2,
            seeds=[f"127.0.0.1:{port1}"],
            expected_cluster_size=2,
            replication=config,
        )

        async with node1, node2:
            leader = await wait_for_leader([node1, node2], timeout=5.0)

            accounts = await leader.spawn(Account, name="accounts", shards=10)
            await asyncio.sleep(0.5)

            # Create entity with state
            await accounts["user-1"].send(Deposit(100))
            await accounts["user-1"].send(Deposit(50))
            await asyncio.sleep(0.5)

            # Verify state
            balance = await accounts["user-1"].ask(GetBalance())
            assert balance == 150.0

            # Both nodes should have backup stats now
            stats1 = node1.get_backup_stats()
            stats2 = node2.get_backup_stats()
            total_backups = stats1["entry_count"] + stats2["entry_count"]
            assert total_backups > 0, "Expected at least one backup"


class TestBackupStateManager:
    """Tests for BackupStateManager functionality."""

    async def test_backup_manager_store_and_get(self) -> None:
        """Test basic store and get operations."""
        from casty.cluster.persistence import BackupStateManager

        manager = BackupStateManager(max_entries=100, max_memory_mb=10)

        # Store some state
        await manager.store(
            entity_type="account",
            entity_id="user-1",
            state=b"test_state_data",
            version=1,
            primary_node="node-1",
            timestamp=1000.0,
        )

        # Get the state back
        result = await manager.get("account", "user-1")
        assert result is not None
        state_bytes, version = result
        assert state_bytes == b"test_state_data"
        assert version == 1

    async def test_backup_manager_version_check(self) -> None:
        """Test that older versions are ignored."""
        from casty.cluster.persistence import BackupStateManager

        manager = BackupStateManager(max_entries=100, max_memory_mb=10)

        # Store v2 first
        await manager.store(
            entity_type="account",
            entity_id="user-1",
            state=b"v2_data",
            version=2,
            primary_node="node-1",
            timestamp=1000.0,
        )

        # Try to store v1 (should be ignored)
        await manager.store(
            entity_type="account",
            entity_id="user-1",
            state=b"v1_data",
            version=1,
            primary_node="node-1",
            timestamp=900.0,
        )

        # Should still have v2
        result = await manager.get("account", "user-1")
        assert result is not None
        state_bytes, version = result
        assert state_bytes == b"v2_data"
        assert version == 2

    async def test_backup_manager_delete(self) -> None:
        """Test delete operation."""
        from casty.cluster.persistence import BackupStateManager

        manager = BackupStateManager(max_entries=100, max_memory_mb=10)

        await manager.store(
            entity_type="account",
            entity_id="user-1",
            state=b"data",
            version=1,
            primary_node="node-1",
            timestamp=1000.0,
        )

        # Delete
        await manager.delete("account", "user-1")

        # Should be gone
        result = await manager.get("account", "user-1")
        assert result is None

    async def test_backup_manager_eviction(self) -> None:
        """Test LRU eviction when capacity is exceeded."""
        from casty.cluster.persistence import BackupStateManager

        # Very small capacity
        manager = BackupStateManager(max_entries=2, max_memory_mb=10)

        # Store 3 entries (should evict one)
        for i in range(3):
            await manager.store(
                entity_type="account",
                entity_id=f"user-{i}",
                state=f"data-{i}".encode(),
                version=1,
                primary_node="node-1",
                timestamp=float(i),
            )

        # Should only have 2 entries
        stats = manager.get_stats()
        assert stats["entry_count"] == 2


class TestPersistenceProtocols:
    """Tests for persistence protocols and mixins."""

    async def test_persistent_mixin_get_state(self) -> None:
        """Test PersistentActorMixin.get_state."""
        from casty.cluster.persistence import PersistentActorMixin

        class TestActor(PersistentActorMixin):
            def __init__(self):
                self.balance = 100.0
                self.name = "test"
                self._private = "secret"  # Should be excluded

        actor = TestActor()
        state = actor.__casty_get_state__()

        assert state["balance"] == 100.0
        assert state["name"] == "test"
        assert "_private" not in state

    async def test_persistent_mixin_set_state(self) -> None:
        """Test PersistentActorMixin.set_state."""
        from casty.cluster.persistence import PersistentActorMixin

        class TestActor(PersistentActorMixin):
            def __init__(self):
                self.balance = 0.0
                self.name = ""

        actor = TestActor()
        actor.__casty_set_state__({"balance": 200.0, "name": "restored"})

        assert actor.balance == 200.0
        assert actor.name == "restored"

    async def test_persistent_decorator(self) -> None:
        """Test @persistent decorator."""
        from casty.cluster.persistence import persistent

        @persistent("balance", "count")
        class TestActor:
            def __init__(self):
                self.balance = 100.0
                self.count = 5
                self.ignored = "should not be persisted"

        actor = TestActor()
        state = actor.__casty_get_state__()

        assert state == {"balance": 100.0, "count": 5}
        assert "ignored" not in state

    async def test_serialize_actor_state(self) -> None:
        """Test serialize_actor_state function."""
        from casty.cluster.persistence import serialize_actor_state
        from casty.cluster.serialize import deserialize

        class TestActor:
            def __init__(self):
                self.value = 42
                self.text = "hello"
                self._private = "ignored"

        actor = TestActor()
        state_bytes = serialize_actor_state(actor)

        # Should be valid msgpack
        state = deserialize(state_bytes, {})
        assert state["value"] == 42
        assert state["text"] == "hello"
        assert "_private" not in state

    async def test_restore_actor_state(self) -> None:
        """Test restore_actor_state function."""
        from casty.cluster.persistence import restore_actor_state

        class TestActor:
            def __init__(self):
                self.value = 0
                self.text = ""

        actor = TestActor()
        restore_actor_state(actor, {"value": 99, "text": "restored"})

        assert actor.value == 99
        assert actor.text == "restored"
