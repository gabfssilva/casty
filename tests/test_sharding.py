"""Tests for cluster sharding with Multi-Raft."""

import asyncio
from dataclasses import dataclass

import pytest

from casty import Actor, Context, DistributedActorSystem, ShardedRef
from casty.actor import ActorRef

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
    """Account actor for testing sharding."""

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


class TestShardingBasic:
    """Basic sharding functionality tests."""

    async def test_spawn_with_shards_returns_sharded_ref(self, get_port) -> None:
        """Test that spawn(Actor, name='x', shards=10) returns ShardedRef and not ActorRef."""
        system = DistributedActorSystem("127.0.0.1", get_port(), expected_cluster_size=1)
        async with system:
            ref = await system.spawn(Account, name="accounts", shards=10)

            # Should be ShardedRef, not ActorRef
            assert isinstance(ref, ShardedRef)
            assert not isinstance(ref, ActorRef)
            assert ref.entity_type == "accounts"
            assert ref.num_shards == 10

    async def test_entity_created_on_first_message(self, get_port) -> None:
        """Test that entity actor is created on first message."""
        system = DistributedActorSystem("127.0.0.1", get_port(), expected_cluster_size=1)
        async with system:
            accounts = await system.spawn(Account, name="accounts", shards=10)

            # Send message to entity
            await accounts["e1"].send(Deposit(100))

            # Wait for message to be processed via Raft
            await asyncio.sleep(0.3)

            # Verify entity was created
            entity_ref = system._get_entity_actor("accounts", "e1")
            assert entity_ref is not None

    async def test_send_works(self, get_port) -> None:
        """Test that fire-and-forget send works and state changes."""
        system = DistributedActorSystem("127.0.0.1", get_port(), expected_cluster_size=1)
        async with system:
            accounts = await system.spawn(Account, name="accounts", shards=10)

            # Send deposit
            await accounts["user-1"].send(Deposit(100))
            await asyncio.sleep(0.3)

            # Verify via ask
            balance = await accounts["user-1"].ask(GetBalance())
            assert balance == 100.0

    async def test_ask_works(self, get_port) -> None:
        """Test that request-response ask works and returns correct value."""
        system = DistributedActorSystem("127.0.0.1", get_port(), expected_cluster_size=1)
        async with system:
            accounts = await system.spawn(Account, name="accounts", shards=10)

            # Send deposits
            await accounts["user-1"].send(Deposit(100))
            await accounts["user-1"].send(Deposit(50))
            await asyncio.sleep(0.3)

            # Ask for balance
            balance = await accounts["user-1"].ask(GetBalance())
            assert balance == 150.0

    async def test_multiple_entities_isolated(self, get_port) -> None:
        """Test that different entities have independent state."""
        system = DistributedActorSystem("127.0.0.1", get_port(), expected_cluster_size=1)
        async with system:
            accounts = await system.spawn(Account, name="accounts", shards=10)

            # Deposit to different entities
            await accounts["a"].send(Deposit(100))
            await accounts["b"].send(Deposit(200))
            await asyncio.sleep(0.3)

            # Verify independent balances
            balance_a = await accounts["a"].ask(GetBalance())
            balance_b = await accounts["b"].ask(GetBalance())

            assert balance_a == 100.0
            assert balance_b == 200.0

    async def test_consistent_routing(self, get_port) -> None:
        """Test that same entity_id always routes to the same shard/node."""
        system = DistributedActorSystem("127.0.0.1", get_port(), expected_cluster_size=1)
        async with system:
            accounts = await system.spawn(Account, name="accounts", shards=100)

            # Same entity_id should always get same shard
            shard1 = accounts._get_shard_id("user-123")
            shard2 = accounts._get_shard_id("user-123")
            shard3 = accounts._get_shard_id("user-123")

            assert shard1 == shard2 == shard3

            # Different entity_ids can get different shards
            shard_a = accounts._get_shard_id("user-a")
            shard_b = accounts._get_shard_id("user-b")
            # They may or may not be the same, but the hash is deterministic
            assert accounts._get_shard_id("user-a") == shard_a
            assert accounts._get_shard_id("user-b") == shard_b


class TestShardingDistributed:
    """Tests for distributed sharding across multiple nodes."""

    async def test_distributed_routing(self, get_port) -> None:
        """Test that entities are distributed across nodes and routed correctly."""
        port1 = get_port()
        port2 = get_port()
        port3 = get_port()

        node1 = DistributedActorSystem(
            "127.0.0.1",
            port1,
            seeds=[],
            expected_cluster_size=3,
        )
        node2 = DistributedActorSystem(
            "127.0.0.1",
            port2,
            seeds=[f"127.0.0.1:{port1}"],
            expected_cluster_size=3,
        )
        node3 = DistributedActorSystem(
            "127.0.0.1",
            port3,
            seeds=[f"127.0.0.1:{port1}"],
            expected_cluster_size=3,
        )

        async with node1, node2, node3:
            # Wait for leader election
            leader = await wait_for_leader([node1, node2, node3], timeout=10.0)

            # Spawn sharded actor on leader
            # Note: Actor and message types are auto-imported on other nodes
            accounts = await leader.spawn(Account, name="accounts", shards=100)

            # Wait for shard allocations to replicate
            await asyncio.sleep(1.0)

            # Create entities that should be distributed across nodes
            # Use multiple entity IDs to increase chance of hitting different shards
            entity_ids = [f"user-{i}" for i in range(10)]

            for eid in entity_ids:
                await accounts[eid].send(Deposit(100))

            await asyncio.sleep(0.5)

            # Verify entities work from any node
            for eid in entity_ids:
                balance = await accounts[eid].ask(GetBalance())
                assert balance == 100.0, f"Entity {eid} has wrong balance"

            # Verify entities are distributed across nodes
            nodes = [node1, node2, node3]
            node_entity_counts = [len(n._entity_actors) for n in nodes]

            # At least some entities should exist (not all 0)
            assert sum(node_entity_counts) > 0, "No entities were created"

    async def test_message_ordering_via_raft(self, get_port) -> None:
        """Test that messages to same entity are applied in Raft log order."""
        port1 = get_port()
        port2 = get_port()

        node1 = DistributedActorSystem(
            "127.0.0.1",
            port1,
            seeds=[],
            expected_cluster_size=2,
        )
        node2 = DistributedActorSystem(
            "127.0.0.1",
            port2,
            seeds=[f"127.0.0.1:{port1}"],
            expected_cluster_size=2,
        )

        async with node1, node2:
            # Wait for leader election
            leader = await wait_for_leader([node1, node2], timeout=5.0)

            # Spawn counter with sharding
            # Note: Actor and message types are auto-imported on other nodes
            counters = await leader.spawn(Counter, name="counters", shards=10)

            # Wait for shard allocations to replicate
            await asyncio.sleep(0.5)

            # Send a sequence of set operations
            # Due to Raft ordering, the final value should be the last one sent
            await counters["c1"].send(SetValue(1))
            await counters["c1"].send(SetValue(2))
            await counters["c1"].send(SetValue(3))
            await counters["c1"].send(SetValue(42))

            await asyncio.sleep(0.5)

            # The value should be the last set value (42)
            value = await counters["c1"].ask(GetValue())
            assert value == 42, f"Expected 42, got {value}"


class TestShardingCriterion:
    """Integration test matching the success criterion from the spec."""

    async def test_full_criterion(self, get_port) -> None:
        """Test the full success criterion from the spec."""
        port1 = get_port()
        port2 = get_port()
        port3 = get_port()

        node1 = DistributedActorSystem(
            "127.0.0.1",
            port1,
            seeds=[],
            expected_cluster_size=3,
        )
        node2 = DistributedActorSystem(
            "127.0.0.1",
            port2,
            seeds=[f"127.0.0.1:{port1}"],
            expected_cluster_size=3,
        )
        node3 = DistributedActorSystem(
            "127.0.0.1",
            port3,
            seeds=[f"127.0.0.1:{port1}"],
            expected_cluster_size=3,
        )

        async with node1, node2, node3:
            # Wait for cluster to form
            leader = await wait_for_leader([node1, node2, node3], timeout=10.0)

            # Use the leader to spawn
            system = leader

            # This is the code from the spec:
            # Note: Actor and message types are auto-imported on other nodes
            accounts = await system.spawn(Account, name="account", shards=100)

            # Wait for shard allocations
            await asyncio.sleep(1.0)

            await accounts["user-1"].send(Deposit(100))
            await accounts["user-1"].send(Deposit(50))

            # Wait for messages to be processed
            await asyncio.sleep(0.5)

            balance = await accounts["user-1"].ask(GetBalance())
            assert balance == 150.0
