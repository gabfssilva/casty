# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportMissingImports=false, reportUnknownArgumentType=false
from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import cyclopts

from casty.client import ClusterClient
from casty.cluster import ShardEnvelope

from app.entities import (
    Get,
    GetCounter,
    Increment,
    Put,
)

logger = logging.getLogger("casty.aws_client")

CASTY_PORT = 25520
LOCAL_BASE_PORT = 25520
NUM_SHARDS = 20


def build_address_map(
    node_count: int,
    casty_port: int = CASTY_PORT,
    local_base_port: int = LOCAL_BASE_PORT,
) -> dict[tuple[str, int], tuple[str, int]]:
    """Map each node's private address to a localhost tunnel port.

    Assumes manual SSH tunnels: ``-L {local_base_port+i}:10.0.1.{10+i}:{casty_port}``
    """
    return {
        (f"10.0.1.{10 + i}", casty_port): ("127.0.0.1", local_base_port + i)
        for i in range(node_count)
    }


@asynccontextmanager
async def cluster_connection(
    *,
    node_count: int,
    casty_port: int = CASTY_PORT,
    local_base_port: int = LOCAL_BASE_PORT,
    system_name: str,
) -> AsyncIterator[ClusterClient]:
    """Yield a ClusterClient that routes through pre-existing SSH tunnels.

    Requires manual tunnels set up beforehand::

        ssh -N \\
          -L 25520:10.0.1.10:25520 \\
          -L 25521:10.0.1.11:25520 \\
          ...
          -i ~/.ssh/key ec2-user@<public-ip>
    """
    address_map = build_address_map(node_count, casty_port, local_base_port)
    contact_points = [(f"10.0.1.{10 + i}", casty_port) for i in range(node_count)]

    async with ClusterClient(
        contact_points=contact_points,
        system_name=system_name,
        client_host="127.0.0.1",
        client_port=0,
        address_map=address_map,
    ) as client:
        await asyncio.sleep(2.0)
        yield client


app = cyclopts.App(
    name="casty-client",
    help="CLI client for Casty AWS cluster via SSH tunnel.",
)
counter_app = cyclopts.App(name="counter", help="Counter entity operations.")
kv_app = cyclopts.App(name="kv", help="KV entity operations.")
app.command(counter_app)
app.command(kv_app)


@counter_app.command(name="increment")
async def counter_increment(
    entity_id: str,
    *,
    amount: int = 1,
    node_count: int = 3,
    casty_port: int = CASTY_PORT,
    system_name: str = "aws-cluster",
) -> None:
    """Increment a counter entity."""
    async with cluster_connection(
        node_count=node_count,
        casty_port=casty_port,
        system_name=system_name,
    ) as client:
        ref = client.entity_ref("counters", num_shards=NUM_SHARDS)
        ref.tell(ShardEnvelope(entity_id, Increment(amount=amount)))
        await asyncio.sleep(0.1)
        print(f"Incremented '{entity_id}' by {amount}")


@counter_app.command(name="get")
async def counter_get(
    entity_id: str,
    *,
    node_count: int = 3,
    casty_port: int = CASTY_PORT,
    system_name: str = "aws-cluster",
) -> None:
    """Get a counter value."""
    async with cluster_connection(
        node_count=node_count,
        casty_port=casty_port,
        system_name=system_name,
    ) as client:
        ref = client.entity_ref("counters", num_shards=NUM_SHARDS)
        value = await client.ask(
            ref,
            lambda r, eid=entity_id: ShardEnvelope(eid, GetCounter(reply_to=r)),
            timeout=5.0,
        )
        print(f"{entity_id} = {value}")


@kv_app.command(name="put")
async def kv_put(
    entity_id: str,
    key: str,
    value: str,
    *,
    node_count: int = 3,
    casty_port: int = CASTY_PORT,
    system_name: str = "aws-cluster",
) -> None:
    """Set a KV entry."""
    async with cluster_connection(
        node_count=node_count,
        casty_port=casty_port,
        system_name=system_name,
    ) as client:
        ref = client.entity_ref("kv", num_shards=NUM_SHARDS)
        ref.tell(ShardEnvelope(entity_id, Put(key=key, value=value)))
        await asyncio.sleep(0.1)
        print(f"Set '{entity_id}'.'{key}' = '{value}'")


@kv_app.command(name="get")
async def kv_get(
    entity_id: str,
    key: str,
    *,
    node_count: int = 3,
    casty_port: int = CASTY_PORT,
    system_name: str = "aws-cluster",
) -> None:
    """Get a KV entry."""
    async with cluster_connection(
        node_count=node_count,
        casty_port=casty_port,
        system_name=system_name,
    ) as client:
        ref = client.entity_ref("kv", num_shards=NUM_SHARDS)
        result = await client.ask(
            ref,
            lambda r, eid=entity_id, k=key: ShardEnvelope(eid, Get(key=k, reply_to=r)),
            timeout=5.0,
        )
        print(f"{entity_id}.{key} = {result}")


if __name__ == "__main__":
    app()
