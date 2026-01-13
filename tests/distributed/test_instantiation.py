"""Tests for distributed system instantiation and configuration."""

from dataclasses import dataclass

import pytest

from casty import ActorSystem, DistributedActorSystem
from casty.cluster.serialize import deserialize, serialize


@dataclass
class Increment:
    amount: int = 1


@dataclass
class GetValue:
    pass

pytestmark = pytest.mark.slow


class TestDistributedFactory:
    """Tests for ActorSystem.distributed() factory."""

    async def test_factory_returns_distributed_system(self, get_port) -> None:
        async with ActorSystem.distributed("127.0.0.1", get_port()) as system:
            assert isinstance(system, DistributedActorSystem)

    async def test_direct_instantiation(self, get_port) -> None:
        async with DistributedActorSystem("127.0.0.1", get_port()) as system:
            assert isinstance(system, DistributedActorSystem)


class TestTypeRegistry:
    """Tests for message type handling."""

    async def test_explicit_register_type(self, get_port) -> None:
        """Explicit registration still works for pre-caching types."""
        async with DistributedActorSystem("127.0.0.1", get_port()) as system:
            system.register_type(Increment)
            system.register_type(GetValue)

            assert "distributed.test_instantiation.Increment" in system._cluster.type_registry
            assert "distributed.test_instantiation.GetValue" in system._cluster.type_registry

    def test_auto_import_on_deserialize(self) -> None:
        """Types are automatically imported during deserialization."""
        registry: dict[str, type] = {}  # Empty registry

        # Serialize a message
        msg = Increment(amount=42)
        data = serialize(msg)

        # Deserialize without pre-registering - should auto-import
        result = deserialize(data, registry)

        assert isinstance(result, Increment)
        assert result.amount == 42
        # Type should now be cached in registry
        assert "distributed.test_instantiation.Increment" in registry
