from __future__ import annotations

import pytest

from casty.core.address import ActorAddress


async def test_local_address_creation() -> None:
    addr = ActorAddress(system="my-system", path="/greeter/worker-1")
    assert addr.system == "my-system"
    assert addr.path == "/greeter/worker-1"
    assert addr.host is None
    assert addr.port is None
    assert addr.is_local is True


async def test_remote_address_creation() -> None:
    addr = ActorAddress(
        system="my-system",
        path="/greeter/worker-1",
        host="10.0.0.1",
        port=25520,
    )
    assert addr.system == "my-system"
    assert addr.path == "/greeter/worker-1"
    assert addr.host == "10.0.0.1"
    assert addr.port == 25520
    assert addr.is_local is False


async def test_local_address_to_uri() -> None:
    addr = ActorAddress(system="my-system", path="/greeter/worker-1")
    assert addr.to_uri() == "casty://my-system/greeter/worker-1"


async def test_remote_address_to_uri() -> None:
    addr = ActorAddress(
        system="my-system",
        path="/greeter/worker-1",
        host="10.0.0.1",
        port=25520,
    )
    assert addr.to_uri() == "casty://my-system@10.0.0.1:25520/greeter/worker-1"


async def test_local_address_from_uri() -> None:
    addr = ActorAddress.from_uri("casty://my-system/greeter/worker-1")
    assert addr.system == "my-system"
    assert addr.path == "/greeter/worker-1"
    assert addr.host is None
    assert addr.port is None


async def test_remote_address_from_uri() -> None:
    addr = ActorAddress.from_uri("casty://my-system@10.0.0.1:25520/greeter/worker-1")
    assert addr.system == "my-system"
    assert addr.path == "/greeter/worker-1"
    assert addr.host == "10.0.0.1"
    assert addr.port == 25520


async def test_address_roundtrip() -> None:
    original = ActorAddress(
        system="cluster",
        path="/user/greeter",
        host="192.168.1.100",
        port=9000,
    )
    roundtripped = ActorAddress.from_uri(original.to_uri())
    assert roundtripped == original


async def test_local_address_roundtrip() -> None:
    original = ActorAddress(system="local-sys", path="/user/greeter")
    roundtripped = ActorAddress.from_uri(original.to_uri())
    assert roundtripped == original


async def test_invalid_uri_raises() -> None:
    with pytest.raises(ValueError, match="casty://"):
        ActorAddress.from_uri("http://not-casty/path")

    with pytest.raises(ValueError, match="casty://"):
        ActorAddress.from_uri("garbage")


async def test_address_is_frozen() -> None:
    addr = ActorAddress(system="sys", path="/actor")
    with pytest.raises(AttributeError):
        addr.system = "other"  # type: ignore[misc]
