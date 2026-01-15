"""Tests for sharded actors with deterministic hashing."""

import asyncio
from dataclasses import dataclass

import pytest

from casty import ActorSystem, Actor, Context


# Test messages
@dataclass(frozen=True)
class UpdateBalance:
    amount: int


@dataclass(frozen=True)
class GetBalance:
    pass


@dataclass(frozen=True)
class GetEntityId:
    pass


# Test actor
class Account(Actor[UpdateBalance | GetBalance | GetEntityId]):
    """Simple account actor for testing sharding."""

    def __init__(self, entity_id: str) -> None:
        self.entity_id = entity_id
        self.balance = 0

    async def receive(self, msg: UpdateBalance | GetBalance | GetEntityId, ctx: Context) -> None:
        match msg:
            case UpdateBalance(amount):
                self.balance += amount
            case GetBalance():
                ctx.reply(self.balance)
            case GetEntityId():
                ctx.reply(self.entity_id)


class TestShardedSpawn:
    """Test spawning sharded actors."""

    @pytest.mark.asyncio
    async def test_spawn_sharded_returns_sharded_ref(self):
        """spawn with sharded=True should return a ShardedRef."""
        from casty.actor import ShardedRef

        async with ActorSystem.clustered() as system:
            await asyncio.sleep(0.2)

            accounts = await system.spawn(Account, name="accounts", sharded=True)

            assert isinstance(accounts, ShardedRef)
            assert accounts.entity_type == "accounts"

    @pytest.mark.asyncio
    async def test_spawn_sharded_requires_name(self):
        """spawn with sharded=True should require a name."""
        async with ActorSystem.clustered() as system:
            await asyncio.sleep(0.2)

            with pytest.raises(ValueError, match="require a name"):
                await system.spawn(Account, sharded=True)


class TestEntityAccess:
    """Test accessing entities via ShardedRef."""

    @pytest.mark.asyncio
    async def test_entity_subscript_returns_entity_ref(self):
        """Subscripting ShardedRef should return EntityRef."""
        from casty.actor import EntityRef

        async with ActorSystem.clustered() as system:
            await asyncio.sleep(0.2)

            accounts = await system.spawn(Account, name="accounts", sharded=True)

            entity = accounts["user-123"]
            assert isinstance(entity, EntityRef)
            assert entity.entity_id == "user-123"

    @pytest.mark.asyncio
    async def test_entity_send(self):
        """Should be able to send messages to entities."""
        async with ActorSystem.clustered() as system:
            await asyncio.sleep(0.2)

            accounts = await system.spawn(Account, name="accounts", sharded=True)

            # Send message to entity (fire-and-forget)
            await accounts["acc-1"].send(UpdateBalance(100))
            await asyncio.sleep(0.1)

            # Verify balance was updated
            balance = await accounts["acc-1"].ask(GetBalance())
            assert balance == 100

    @pytest.mark.asyncio
    async def test_entity_ask(self):
        """Should be able to ask entities and get responses."""
        async with ActorSystem.clustered() as system:
            await asyncio.sleep(0.2)

            accounts = await system.spawn(Account, name="accounts", sharded=True)

            # Send some updates
            await accounts["acc-2"].send(UpdateBalance(50))
            await accounts["acc-2"].send(UpdateBalance(25))
            await asyncio.sleep(0.1)

            # Ask for balance
            balance = await accounts["acc-2"].ask(GetBalance())
            assert balance == 75


class TestDeterministicRouting:
    """Test that entities are routed deterministically."""

    @pytest.mark.asyncio
    async def test_same_entity_same_routing(self):
        """Same entity ID should always route to same place."""
        async with ActorSystem.clustered() as system:
            await asyncio.sleep(0.2)

            accounts = await system.spawn(Account, name="accounts", sharded=True)

            # Multiple operations on same entity
            await accounts["user-abc"].send(UpdateBalance(10))
            await accounts["user-abc"].send(UpdateBalance(20))
            await accounts["user-abc"].send(UpdateBalance(30))
            await asyncio.sleep(0.1)

            # All updates should have gone to same actor
            balance = await accounts["user-abc"].ask(GetBalance())
            assert balance == 60

    @pytest.mark.asyncio
    async def test_entity_receives_correct_id(self):
        """Entity actor should receive its entity_id in constructor."""
        async with ActorSystem.clustered() as system:
            await asyncio.sleep(0.2)

            accounts = await system.spawn(Account, name="accounts", sharded=True)

            # Trigger entity creation by sending a message
            await accounts["entity-xyz"].send(UpdateBalance(1))
            await asyncio.sleep(0.1)

            # Ask entity for its ID
            entity_id = await accounts["entity-xyz"].ask(GetEntityId())
            assert entity_id == "entity-xyz"


class TestMultipleEntities:
    """Test handling multiple entities."""

    @pytest.mark.asyncio
    async def test_multiple_entities_independent(self):
        """Different entities should have independent state."""
        async with ActorSystem.clustered() as system:
            await asyncio.sleep(0.2)

            accounts = await system.spawn(Account, name="accounts", sharded=True)

            # Update different entities
            await accounts["alice"].send(UpdateBalance(100))
            await accounts["bob"].send(UpdateBalance(200))
            await accounts["charlie"].send(UpdateBalance(300))
            await asyncio.sleep(0.1)

            # Each should have its own balance
            alice_balance = await accounts["alice"].ask(GetBalance())
            bob_balance = await accounts["bob"].ask(GetBalance())
            charlie_balance = await accounts["charlie"].ask(GetBalance())

            assert alice_balance == 100
            assert bob_balance == 200
            assert charlie_balance == 300

    @pytest.mark.asyncio
    async def test_entity_lazy_spawn(self):
        """Entities should be spawned lazily on first message."""
        async with ActorSystem.clustered() as system:
            await asyncio.sleep(0.2)

            accounts = await system.spawn(Account, name="accounts", sharded=True)

            # Just creating the ShardedRef shouldn't spawn any entities
            # First message should trigger spawn
            await accounts["lazy-1"].send(UpdateBalance(1))
            await asyncio.sleep(0.1)

            # Should work
            balance = await accounts["lazy-1"].ask(GetBalance())
            assert balance == 1


class TestShardedRefOperators:
    """Test ShardedRef and EntityRef operators."""

    @pytest.mark.asyncio
    async def test_entity_rshift_operator(self):
        """EntityRef >> msg should send the message."""
        async with ActorSystem.clustered() as system:
            await asyncio.sleep(0.2)

            accounts = await system.spawn(Account, name="accounts", sharded=True)

            # Use >> operator
            await (accounts["op-test"] >> UpdateBalance(42))
            await asyncio.sleep(0.1)

            balance = await accounts["op-test"].ask(GetBalance())
            assert balance == 42

    @pytest.mark.asyncio
    async def test_entity_lshift_operator(self):
        """EntityRef << msg should ask and return result."""
        async with ActorSystem.clustered() as system:
            await asyncio.sleep(0.2)

            accounts = await system.spawn(Account, name="accounts", sharded=True)

            await accounts["op-ask"].send(UpdateBalance(99))
            await asyncio.sleep(0.1)

            # Use << operator
            balance = await (accounts["op-ask"] << GetBalance())
            assert balance == 99


class TestMultipleShardedTypes:
    """Test multiple sharded actor types."""

    @pytest.mark.asyncio
    async def test_multiple_sharded_types(self):
        """Should support multiple sharded actor types."""

        @dataclass(frozen=True)
        class SetValue:
            value: int

        @dataclass(frozen=True)
        class GetValue:
            pass

        class Counter(Actor[SetValue | GetValue]):
            def __init__(self, entity_id: str) -> None:
                self.entity_id = entity_id
                self.value = 0

            async def receive(self, msg: SetValue | GetValue, ctx: Context) -> None:
                match msg:
                    case SetValue(value):
                        self.value = value
                    case GetValue():
                        ctx.reply(self.value)

        async with ActorSystem.clustered() as system:
            await asyncio.sleep(0.2)

            # Spawn two different sharded types
            accounts = await system.spawn(Account, name="accounts", sharded=True)
            counters = await system.spawn(Counter, name="counters", sharded=True)

            # Use both
            await accounts["a1"].send(UpdateBalance(100))
            await counters["c1"].send(SetValue(42))
            await asyncio.sleep(0.1)

            acc_balance = await accounts["a1"].ask(GetBalance())
            counter_value = await counters["c1"].ask(GetValue())

            assert acc_balance == 100
            assert counter_value == 42


class TestMultiNodeSharding:
    """Test sharded actors across multiple cluster nodes."""

    @pytest.mark.asyncio
    async def test_two_node_cluster_forms(self):
        """Two nodes should form a cluster via seeds."""
        async with ActorSystem.clustered(
            host="127.0.0.1",
            port=17946,
            node_id="node-1",
        ) as system1:
            await asyncio.sleep(0.3)

            async with ActorSystem.clustered(
                host="127.0.0.1",
                port=17947,
                node_id="node-2",
                seeds=["127.0.0.1:17946"],
            ) as system2:
                # Wait for cluster to form
                await asyncio.sleep(0.5)

                # Both nodes should see each other
                assert system1.node_id == "node-1"
                assert system2.node_id == "node-2"

    @pytest.mark.asyncio
    async def test_sharded_entity_remote_send(self):
        """Entities should be routed to correct node based on hash."""
        async with ActorSystem.clustered(
            host="127.0.0.1",
            port=17948,
            node_id="node-1",
        ) as system1:
            await asyncio.sleep(0.3)

            async with ActorSystem.clustered(
                host="127.0.0.1",
                port=17949,
                node_id="node-2",
                seeds=["127.0.0.1:17948"],
            ) as system2:
                await asyncio.sleep(0.5)

                # Spawn sharded type on node 1
                accounts = await system1.spawn(Account, name="accounts", sharded=True)

                # Send to multiple entities - some will hash to node-2
                # We test many entities to ensure some route remotely
                for i in range(20):
                    await accounts[f"user-{i}"].send(UpdateBalance(10))

                await asyncio.sleep(0.3)

                # Verify all entities received their messages
                for i in range(20):
                    balance = await accounts[f"user-{i}"].ask(GetBalance())
                    assert balance == 10, f"Entity user-{i} has wrong balance: {balance}"

    @pytest.mark.asyncio
    async def test_sharded_entity_remote_ask(self):
        """Ask should work across nodes with proper response routing."""
        async with ActorSystem.clustered(
            host="127.0.0.1",
            port=17950,
            node_id="node-1",
        ) as system1:
            await asyncio.sleep(0.3)

            async with ActorSystem.clustered(
                host="127.0.0.1",
                port=17951,
                node_id="node-2",
                seeds=["127.0.0.1:17950"],
            ) as system2:
                await asyncio.sleep(0.5)

                accounts = await system1.spawn(Account, name="accounts", sharded=True)

                # Test multiple entities with ask
                for i in range(10):
                    entity_id = f"account-{i}"
                    await accounts[entity_id].send(UpdateBalance(i * 10))

                await asyncio.sleep(0.2)

                # Ask each entity for its balance
                for i in range(10):
                    entity_id = f"account-{i}"
                    balance = await accounts[entity_id].ask(GetBalance())
                    assert balance == i * 10

    @pytest.mark.asyncio
    async def test_entity_receives_correct_id_remote(self):
        """Entity should receive correct entity_id even when remote."""
        async with ActorSystem.clustered(
            host="127.0.0.1",
            port=17952,
            node_id="node-1",
        ) as system1:
            await asyncio.sleep(0.3)

            async with ActorSystem.clustered(
                host="127.0.0.1",
                port=17953,
                node_id="node-2",
                seeds=["127.0.0.1:17952"],
            ) as system2:
                await asyncio.sleep(0.5)

                accounts = await system1.spawn(Account, name="accounts", sharded=True)

                # Test multiple entities
                test_ids = ["remote-test-1", "remote-test-2", "remote-test-3"]
                for eid in test_ids:
                    # Trigger creation
                    await accounts[eid].send(UpdateBalance(1))

                await asyncio.sleep(0.2)

                # Verify each entity knows its ID
                for eid in test_ids:
                    received_id = await accounts[eid].ask(GetEntityId())
                    assert received_id == eid

    @pytest.mark.asyncio
    async def test_deterministic_routing_multi_node(self):
        """Same entity should always route to same node."""
        async with ActorSystem.clustered(
            host="127.0.0.1",
            port=17954,
            node_id="node-1",
        ) as system1:
            await asyncio.sleep(0.3)

            async with ActorSystem.clustered(
                host="127.0.0.1",
                port=17955,
                node_id="node-2",
                seeds=["127.0.0.1:17954"],
            ) as system2:
                await asyncio.sleep(0.5)

                accounts = await system1.spawn(Account, name="accounts", sharded=True)

                # Send multiple messages to same entity
                entity_id = "deterministic-test"
                await accounts[entity_id].send(UpdateBalance(100))
                await accounts[entity_id].send(UpdateBalance(200))
                await accounts[entity_id].send(UpdateBalance(300))

                await asyncio.sleep(0.2)

                accounts2 = await system2.spawn(Account, name="accounts", sharded=True)

                # All messages should have gone to same actor instance
                balance = await accounts2[entity_id].ask(GetBalance())
                assert balance == 600
