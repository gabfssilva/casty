"""Tests for sharded actors in distributed system."""

import asyncio
from dataclasses import dataclass

import pytest

from casty import Actor, ActorSystem, Context
from casty.actor import ShardedRef
from casty.cluster.control_plane import RaftConfig
from casty.persistence import ReplicationConfig


# Fast config for tests
FAST_RAFT_CONFIG = RaftConfig(
    heartbeat_interval=0.05,
    election_timeout_min=0.1,
    election_timeout_max=0.2,
)


@dataclass
class GetState:
    """Message to get entity state."""
    pass


@dataclass
class SetValue:
    """Message to set a value."""
    value: int


@dataclass
class Increment:
    """Message to increment value."""
    amount: int = 1


class ShardedCounter(Actor[GetState | SetValue | Increment]):
    """A sharded counter entity for testing."""

    def __init__(self, entity_id: str):
        self.entity_id = entity_id
        self.value = 0

    async def receive(
        self, msg: GetState | SetValue | Increment, ctx: Context
    ) -> None:
        match msg:
            case GetState():
                ctx.reply({
                    "entity_id": self.entity_id,
                    "value": self.value,
                })
            case SetValue(value):
                self.value = value
            case Increment(amount):
                self.value += amount


class TestShardedBasic:
    """Basic sharded actor tests."""

    @pytest.mark.asyncio
    async def test_spawn_sharded_returns_sharded_ref(self):
        """Test that spawning sharded returns ShardedRef."""
        async with ActorSystem.distributed(
            "127.0.0.1", 0, raft_config=FAST_RAFT_CONFIG
        ) as system:
            await asyncio.sleep(0.3)

            ref = await system.spawn(
                ShardedCounter,
                name="counters",
                sharded=True,
            )

            assert ref is not None
            assert isinstance(ref, ShardedRef)

    @pytest.mark.asyncio
    async def test_sharded_entity_indexing(self):
        """Test that ShardedRef supports indexing."""
        async with ActorSystem.distributed(
            "127.0.0.1", 0, raft_config=FAST_RAFT_CONFIG
        ) as system:
            await asyncio.sleep(0.3)

            counters = await system.spawn(
                ShardedCounter,
                name="indexed-counters",
                sharded=True,
            )

            # Access entity by ID
            entity = counters["entity-1"]
            assert entity is not None
            assert hasattr(entity, "send")
            assert hasattr(entity, "ask")


class TestShardedMessaging:
    """Tests for sending messages to sharded entities."""

    @pytest.mark.asyncio
    async def test_send_to_entity(self):
        """Test sending message to a specific entity."""
        async with ActorSystem.distributed(
            "127.0.0.1", 0, raft_config=FAST_RAFT_CONFIG
        ) as system:
            await asyncio.sleep(0.3)

            counters = await system.spawn(
                ShardedCounter,
                name="send-counters",
                sharded=True,
            )

            # Send to entity
            await counters["entity-1"].send(SetValue(42))
            await asyncio.sleep(0.1)

            # Ask for state
            state = await counters["entity-1"].ask(GetState())
            assert state["value"] == 42
            assert state["entity_id"] == "entity-1"

    @pytest.mark.asyncio
    async def test_multiple_entities_independent(self):
        """Test that different entities are independent."""
        async with ActorSystem.distributed(
            "127.0.0.1", 0, raft_config=FAST_RAFT_CONFIG
        ) as system:
            await asyncio.sleep(0.3)

            counters = await system.spawn(
                ShardedCounter,
                name="independent-counters",
                sharded=True,
            )

            # Set different values for different entities
            await counters["entity-a"].send(SetValue(100))
            await counters["entity-b"].send(SetValue(200))
            await counters["entity-c"].send(SetValue(300))
            await asyncio.sleep(0.15)

            # Verify independence
            state_a = await counters["entity-a"].ask(GetState())
            state_b = await counters["entity-b"].ask(GetState())
            state_c = await counters["entity-c"].ask(GetState())

            assert state_a["value"] == 100
            assert state_b["value"] == 200
            assert state_c["value"] == 300

    @pytest.mark.asyncio
    async def test_entity_state_persists_across_calls(self):
        """Test that entity state persists between messages."""
        async with ActorSystem.distributed(
            "127.0.0.1", 0, raft_config=FAST_RAFT_CONFIG
        ) as system:
            await asyncio.sleep(0.3)

            counters = await system.spawn(
                ShardedCounter,
                name="persistent-counters",
                sharded=True,
            )

            # Increment multiple times
            await counters["counter-1"].send(Increment(10))
            await counters["counter-1"].send(Increment(20))
            await counters["counter-1"].send(Increment(30))
            await asyncio.sleep(0.15)

            state = await counters["counter-1"].ask(GetState())
            assert state["value"] == 60


class TestShardedEntityCreation:
    """Tests for on-demand entity creation."""

    @pytest.mark.asyncio
    async def test_entity_created_on_first_message(self):
        """Test that entity is created when first messaged."""
        async with ActorSystem.distributed(
            "127.0.0.1", 0, raft_config=FAST_RAFT_CONFIG
        ) as system:
            await asyncio.sleep(0.3)

            counters = await system.spawn(
                ShardedCounter,
                name="lazy-counters",
                sharded=True,
            )

            # Entity doesn't exist yet - will be created on first message
            await counters["new-entity"].send(SetValue(99))
            await asyncio.sleep(0.1)

            state = await counters["new-entity"].ask(GetState())
            assert state["value"] == 99
            assert state["entity_id"] == "new-entity"

    @pytest.mark.asyncio
    async def test_many_entities_created(self):
        """Test creating many entities."""
        async with ActorSystem.distributed(
            "127.0.0.1", 0, raft_config=FAST_RAFT_CONFIG
        ) as system:
            await asyncio.sleep(0.3)

            counters = await system.spawn(
                ShardedCounter,
                name="many-counters",
                sharded=True,
            )

            # Create 20 entities
            for i in range(20):
                await counters[f"entity-{i}"].send(SetValue(i * 10))

            await asyncio.sleep(0.3)

            # Verify some entities
            for i in [0, 5, 10, 15, 19]:
                state = await counters[f"entity-{i}"].ask(GetState())
                assert state["value"] == i * 10


class TestShardedEntityTypes:
    """Tests for multiple sharded entity types."""

    @pytest.mark.asyncio
    async def test_different_entity_types(self):
        """Test multiple different sharded entity types."""

        class UserEntity(Actor[GetState | SetValue]):
            def __init__(self, entity_id: str):
                self.entity_id = entity_id
                self.data = {}

            async def receive(self, msg: GetState | SetValue, ctx: Context) -> None:
                match msg:
                    case GetState():
                        ctx.reply({"type": "user", "id": self.entity_id})
                    case SetValue(value):
                        self.data["value"] = value

        class OrderEntity(Actor[GetState | SetValue]):
            def __init__(self, entity_id: str):
                self.entity_id = entity_id
                self.items = []

            async def receive(self, msg: GetState | SetValue, ctx: Context) -> None:
                match msg:
                    case GetState():
                        ctx.reply({"type": "order", "id": self.entity_id})
                    case SetValue(value):
                        self.items.append(value)

        async with ActorSystem.distributed(
            "127.0.0.1", 0, raft_config=FAST_RAFT_CONFIG
        ) as system:
            await asyncio.sleep(0.3)

            users = await system.spawn(
                UserEntity,
                name="users",
                sharded=True,
            )
            orders = await system.spawn(
                OrderEntity,
                name="orders",
                sharded=True,
            )

            # Same ID but different types
            user_state = await users["123"].ask(GetState())
            order_state = await orders["123"].ask(GetState())

            assert user_state["type"] == "user"
            assert order_state["type"] == "order"


class TestShardedReplicationFactor:
    """Tests for sharded entity replication configuration."""

    @pytest.mark.asyncio
    async def test_custom_replication_factor(self):
        """Test spawning with custom replication factor."""
        async with ActorSystem.distributed(
            "127.0.0.1", 0, raft_config=FAST_RAFT_CONFIG
        ) as system:
            await asyncio.sleep(0.3)

            counters = await system.spawn(
                ShardedCounter,
                name="replicated-counters",
                sharded=True,
                replication=ReplicationConfig(factor=5),
            )

            assert counters.replication_factor == 5

    @pytest.mark.asyncio
    async def test_default_replication_factor(self):
        """Test default replication factor is 3."""
        async with ActorSystem.distributed(
            "127.0.0.1", 0, raft_config=FAST_RAFT_CONFIG
        ) as system:
            await asyncio.sleep(0.3)

            counters = await system.spawn(
                ShardedCounter,
                name="default-rf-counters",
                sharded=True,
            )

            assert counters.replication_factor == 3


class TestShardedEntityNaming:
    """Tests for sharded entity naming."""

    @pytest.mark.asyncio
    async def test_entity_type_uses_name(self):
        """Test that entity type uses provided name."""
        async with ActorSystem.distributed(
            "127.0.0.1", 0, raft_config=FAST_RAFT_CONFIG
        ) as system:
            await asyncio.sleep(0.3)

            counters = await system.spawn(
                ShardedCounter,
                name="my-counters",
                sharded=True,
            )

            assert counters.entity_type == "my-counters"

    @pytest.mark.asyncio
    async def test_entity_type_defaults_to_class_name(self):
        """Test that entity type defaults to class name."""
        async with ActorSystem.distributed(
            "127.0.0.1", 0, raft_config=FAST_RAFT_CONFIG
        ) as system:
            await asyncio.sleep(0.3)

            counters = await system.spawn(
                ShardedCounter,
                sharded=True,
            )

            assert counters.entity_type == "ShardedCounter"


class TestShardedEntityIdFormats:
    """Tests for various entity ID formats."""

    @pytest.mark.asyncio
    async def test_string_entity_ids(self):
        """Test string entity IDs."""
        async with ActorSystem.distributed(
            "127.0.0.1", 0, raft_config=FAST_RAFT_CONFIG
        ) as system:
            await asyncio.sleep(0.3)

            counters = await system.spawn(
                ShardedCounter,
                name="string-id-counters",
                sharded=True,
            )

            entity_ids = [
                "simple",
                "with-dashes",
                "with_underscores",
                "MixedCase",
                "uuid-like-12345-67890",
            ]

            for eid in entity_ids:
                await counters[eid].send(SetValue(len(eid)))

            await asyncio.sleep(0.2)

            for eid in entity_ids:
                state = await counters[eid].ask(GetState())
                assert state["entity_id"] == eid
                assert state["value"] == len(eid)


class TestShardedConcurrency:
    """Tests for concurrent access to sharded entities."""

    @pytest.mark.asyncio
    async def test_concurrent_sends_to_same_entity(self):
        """Test concurrent sends to the same entity."""
        async with ActorSystem.distributed(
            "127.0.0.1", 0, raft_config=FAST_RAFT_CONFIG
        ) as system:
            await asyncio.sleep(0.3)

            counters = await system.spawn(
                ShardedCounter,
                name="concurrent-counters",
                sharded=True,
            )

            # Send many increments concurrently
            async def increment():
                await counters["shared-entity"].send(Increment(1))

            await asyncio.gather(*[increment() for _ in range(50)])
            await asyncio.sleep(0.3)

            state = await counters["shared-entity"].ask(GetState())
            assert state["value"] == 50

    @pytest.mark.asyncio
    async def test_concurrent_sends_to_different_entities(self):
        """Test concurrent sends to different entities."""
        async with ActorSystem.distributed(
            "127.0.0.1", 0, raft_config=FAST_RAFT_CONFIG
        ) as system:
            await asyncio.sleep(0.3)

            counters = await system.spawn(
                ShardedCounter,
                name="parallel-counters",
                sharded=True,
            )

            # Send to many entities concurrently
            async def set_entity(idx: int):
                await counters[f"entity-{idx}"].send(SetValue(idx))

            await asyncio.gather(*[set_entity(i) for i in range(30)])
            await asyncio.sleep(0.3)

            # Verify all entities
            for i in range(30):
                state = await counters[f"entity-{i}"].ask(GetState())
                assert state["value"] == i
