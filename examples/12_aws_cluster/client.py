# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportMissingImports=false, reportUnknownArgumentType=false
from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
import asyncssh  # type: ignore[import-untyped]
import cyclopts

from casty.client import ClusterClient
from casty.sharding import ShardEnvelope

from app.entities import (
    Get,
    GetCounter,
    Increment,
    Put,
)

logger = logging.getLogger("casty.aws_client")

CLIENT_PORT = 5100
NUM_SHARDS = 20


@asynccontextmanager
async def cluster_connection(
    *,
    ssh_host: str,
    ssh_user: str,
    ssh_key: Path | None,
    seeds: list[str],
    casty_port: int,
    system_name: str,
) -> AsyncIterator[ClusterClient]:
    """Open SSH tunnels and yield a connected ClusterClient.

    Uses ``known_hosts=None`` — this is an example script, not production
    code.  Do NOT copy without adding host-key verification.
    """
    client_keys = [str(ssh_key)] if ssh_key else []
    async with asyncssh.connect(
        ssh_host,
        username=ssh_user,
        client_keys=client_keys,
        known_hosts=None,
    ) as conn:
        address_map: dict[tuple[str, int], tuple[str, int]] = {}
        listeners = []
        try:
            for seed_ip in seeds:
                listener = await conn.forward_local_port(
                    "127.0.0.1", 0, seed_ip, casty_port
                )
                listeners.append(listener)
                local_port = listener.get_port()
                address_map[(seed_ip, casty_port)] = ("127.0.0.1", local_port)
                logger.info("Forward tunnel: 127.0.0.1:%d → %s:%d", local_port, seed_ip, casty_port)

            reverse_listener = await conn.forward_remote_port(
                "0.0.0.0", 0, "127.0.0.1", CLIENT_PORT
            )
            listeners.append(reverse_listener)
            reverse_port = reverse_listener.get_port()
            advertised_host = seeds[0]
            logger.info("Reverse tunnel: %s:%d → 127.0.0.1:%d", advertised_host, reverse_port, CLIENT_PORT)

            contact_points = [(ip, casty_port) for ip in seeds]

            async with ClusterClient(
                contact_points=contact_points,
                system_name=system_name,
                client_host="127.0.0.1",
                client_port=CLIENT_PORT,
                advertised_host=advertised_host,
                advertised_port=reverse_port,
                address_map=address_map,
            ) as client:
                await asyncio.sleep(2.0)
                yield client
        finally:
            for listener in listeners:
                listener.close()


app = cyclopts.App(
    name="casty-client",
    help="CLI client for Casty AWS cluster via SSH tunnel.",
)
counter_app = cyclopts.App(name="counter", help="Counter entity operations.")
kv_app = cyclopts.App(name="kv", help="KV entity operations.")
app.command(counter_app)
app.command(kv_app)


def generate_seeds(node_count: int) -> list[str]:
    return [f"10.0.1.{10 + i}" for i in range(node_count)]


@counter_app.command(name="increment")
async def counter_increment(
    entity_id: str,
    *,
    ssh_host: str,
    amount: int = 1,
    node_count: int = 3,
    ssh_user: str = "ec2-user",
    ssh_key: Path | None = None,
    casty_port: int = 25520,
    system_name: str = "aws-cluster",
) -> None:
    """Increment a counter entity."""
    async with cluster_connection(
        ssh_host=ssh_host,
        ssh_user=ssh_user,
        ssh_key=ssh_key,
        seeds=generate_seeds(node_count),
        casty_port=casty_port,
        system_name=system_name,
    ) as client:
        ref = client.entity_ref("counters", num_shards=NUM_SHARDS)
        ref.tell(ShardEnvelope(entity_id, Increment(amount=amount)))
        # Fire-and-forget: short delay so the message reaches the TCP buffer
        # before the connection tears down.  Not a delivery guarantee.
        await asyncio.sleep(0.1)
        print(f"Incremented '{entity_id}' by {amount}")


@counter_app.command(name="get")
async def counter_get(
    entity_id: str,
    *,
    ssh_host: str,
    node_count: int = 3,
    ssh_user: str = "ec2-user",
    ssh_key: Path | None = None,
    casty_port: int = 25520,
    system_name: str = "aws-cluster",
) -> None:
    """Get a counter value."""
    async with cluster_connection(
        ssh_host=ssh_host,
        ssh_user=ssh_user,
        ssh_key=ssh_key,
        seeds=generate_seeds(node_count),
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
    ssh_host: str,
    node_count: int = 3,
    ssh_user: str = "ec2-user",
    ssh_key: Path | None = None,
    casty_port: int = 25520,
    system_name: str = "aws-cluster",
) -> None:
    """Set a KV entry."""
    async with cluster_connection(
        ssh_host=ssh_host,
        ssh_user=ssh_user,
        ssh_key=ssh_key,
        seeds=generate_seeds(node_count),
        casty_port=casty_port,
        system_name=system_name,
    ) as client:
        ref = client.entity_ref("kv", num_shards=NUM_SHARDS)
        ref.tell(ShardEnvelope(entity_id, Put(key=key, value=value)))
        # Fire-and-forget: see comment in counter_increment
        await asyncio.sleep(0.1)
        print(f"Set '{entity_id}'.'{key}' = '{value}'")


@kv_app.command(name="get")
async def kv_get(
    entity_id: str,
    key: str,
    *,
    ssh_host: str,
    node_count: int = 3,
    ssh_user: str = "ec2-user",
    ssh_key: Path | None = None,
    casty_port: int = 25520,
    system_name: str = "aws-cluster",
) -> None:
    """Get a KV entry."""
    async with cluster_connection(
        ssh_host=ssh_host,
        ssh_user=ssh_user,
        ssh_key=ssh_key,
        seeds=generate_seeds(node_count),
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
