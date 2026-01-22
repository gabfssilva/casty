import pytest
from casty import actor, Mailbox
from casty.actor import get_registered_actor, clear_actor_registry


def test_actor_decorator_registers_function():
    clear_actor_registry()

    @actor
    async def my_test_actor(*, mailbox: Mailbox[str]):
        async for msg, ctx in mailbox:
            pass

    registered = get_registered_actor("my_test_actor")
    assert registered is not None
    assert registered.__name__ == "my_test_actor"


def test_get_registered_actor_returns_none_for_unknown():
    result = get_registered_actor("nonexistent_actor_xyz")
    assert result is None


def test_clear_actor_registry_removes_registered_actors():
    clear_actor_registry()

    @actor
    async def actor_to_clear(*, mailbox: Mailbox[str]):
        async for msg, ctx in mailbox:
            pass

    assert get_registered_actor("actor_to_clear") is not None

    clear_actor_registry()

    assert get_registered_actor("actor_to_clear") is None
