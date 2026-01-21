import pytest
from unittest.mock import MagicMock

from casty import ActorSystem
from casty.remote.registry import registry_actor, SessionConnected, SessionDisconnected
from casty.remote.messages import Expose, Unexpose, Lookup, Exposed, Unexposed, LookupResult


@pytest.mark.asyncio
async def test_registry_expose_and_lookup():
    async with ActorSystem() as system:
        registry = await system.actor(
            registry_actor(),
            name="registry"
        )

        mock_ref = MagicMock()
        mock_ref.actor_id = "test/counter"

        result = await registry.ask(Expose(ref=mock_ref, name="counter"))
        assert isinstance(result, Exposed)
        assert result.name == "counter"

        result = await registry.ask(Lookup("counter"))
        assert isinstance(result, LookupResult)
        assert result.ref is mock_ref


@pytest.mark.asyncio
async def test_registry_lookup_not_found():
    async with ActorSystem() as system:
        registry = await system.actor(
            registry_actor(),
            name="registry"
        )

        result = await registry.ask(Lookup("nonexistent"))
        assert isinstance(result, LookupResult)
        assert result.ref is None


@pytest.mark.asyncio
async def test_registry_unexpose():
    async with ActorSystem() as system:
        registry = await system.actor(
            registry_actor(),
            name="registry"
        )

        mock_ref = MagicMock()
        mock_ref.actor_id = "test/counter"

        await registry.ask(Expose(ref=mock_ref, name="counter"))
        result = await registry.ask(Unexpose("counter"))
        assert isinstance(result, Unexposed)

        result = await registry.ask(Lookup("counter"))
        assert result.ref is None


@pytest.mark.asyncio
async def test_registry_session_management():
    async with ActorSystem() as system:
        registry = await system.actor(
            registry_actor(),
            name="registry"
        )

        mock_session = MagicMock()
        await registry.send(SessionConnected(mock_session))
        await registry.send(SessionDisconnected(mock_session))
