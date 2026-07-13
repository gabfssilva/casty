"""`connect(address_map=...)`: a lite member reaching a cluster whose members
announce addresses the client cannot route to (private IPs behind an SSH
tunnel). The node advertises an unroutable address; the map rewrites every
outbound dial — seeds included — to the local endpoint."""

from __future__ import annotations

import pytest

import casty
from tests.integration.actors import FAST_CONFIG, Counter, Fetcher, stop_all
from tests.integration.cluster import free_port

pytestmark = pytest.mark.asyncio

UNROUTABLE = "10.255.0.1"


async def _start_tunneled_node() -> tuple[casty.Node, str, str]:
    port = free_port()
    advertised = f"{UNROUTABLE}:{port}"
    node = await casty.start(
        f"127.0.0.1:{port}", config=FAST_CONFIG, advertise=advertised
    )
    return node, advertised, f"127.0.0.1:{port}"


async def test_client_reaches_members_through_the_map() -> None:
    node, advertised, real = await _start_tunneled_node()
    client: casty.Client | None = None
    try:
        client = await casty.connect(
            [advertised],
            config=FAST_CONFIG,
            address_map=lambda addr: real if addr == advertised else addr,
        )
        assert {m.addr for m in client.members()} == {advertised}
        assert await client.actor(Counter, "tunneled").add(2) == 2
        assert await client.service(Fetcher).slow(0.0, 3) == 6
    finally:
        if client is not None:
            await client.close()
        await stop_all([node])


async def test_client_without_the_map_cannot_dial() -> None:
    node, advertised, _ = await _start_tunneled_node()
    try:
        with pytest.raises((casty.CastyError, OSError)):
            await casty.connect([advertised], config=FAST_CONFIG)
    finally:
        await stop_all([node])
