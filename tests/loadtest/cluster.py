from __future__ import annotations

import asyncio
import time
from typing import Any

import httpx


async def get_cluster_status(
    client: httpx.AsyncClient,
    node_urls: str | list[str],
) -> dict[str, Any]:
    """Query cluster status, trying multiple nodes on failure."""
    urls = [node_urls] if isinstance(node_urls, str) else node_urls
    last_exc: Exception | None = None
    for url in urls:
        try:
            response = await client.get(f"{url}/cluster/status", timeout=5.0)
            response.raise_for_status()
            return response.json()
        except httpx.HTTPError as exc:
            last_exc = exc
    raise last_exc or RuntimeError("No node URLs provided")


def find_leader_ip(status: dict[str, Any]) -> str | None:
    up_members = [m["address"] for m in status["members"] if m["status"] == "up"]
    if not up_members:
        return None
    up_members.sort()
    leader_address = up_members[0]
    return leader_address.split(":")[0]


async def wait_for_healthy(
    client: httpx.AsyncClient,
    node_urls: list[str],
    expected_members: int,
    timeout: float = 120.0,
) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            for url in node_urls:
                status = await get_cluster_status(client, url)
                up_count = sum(1 for m in status["members"] if m["status"] == "up")
                if up_count < expected_members:
                    break
            else:
                return
        except (httpx.HTTPError, KeyError):
            pass
        await asyncio.sleep(2.0)
    msg = f"Cluster did not converge to {expected_members} members within {timeout}s"
    raise TimeoutError(msg)


async def discover_node_ips(client: httpx.AsyncClient, url: str) -> list[str]:
    status = await get_cluster_status(client, url)
    ips = sorted({m["address"].split(":")[0] for m in status["members"]})
    if not ips:
        msg = f"No members found via {url}/cluster/status"
        raise RuntimeError(msg)
    return ips


async def wait_for_recovery(
    client: httpx.AsyncClient,
    node_urls: list[str],
    expected_members: int,
    timeout: float = 60.0,
) -> float:
    start = time.monotonic()
    await wait_for_healthy(client, node_urls, expected_members, timeout)
    return time.monotonic() - start
