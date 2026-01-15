"""Test utilities for Casty tests."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from typing import TYPE_CHECKING, Awaitable

if TYPE_CHECKING:
    from casty import ActorSystem


async def retry_until(
    condition: Callable[[], bool | Awaitable[bool]],
    *,
    timeout: float = 10.0,
    interval: float = 0.1,
    message: str = "Condition not met within timeout",
) -> None:
    """Wait until a condition is met, with timeout.

    Args:
        condition: A callable that returns True when the condition is met
        timeout: Maximum time to wait in seconds
        interval: Time between checks in seconds
        message: Error message if timeout is reached

    Raises:
        TimeoutError: If condition is not met within timeout

    Example:
        await retry_until(
            lambda: len(nodes) == expected,
            timeout=10.0,
            message="Nodes not ready",
        )
    """
    start = time.time()
    while time.time() - start < timeout:
        result = condition()

        if asyncio.iscoroutine(result):
            result = await result

        if result:
            return

        await asyncio.sleep(interval)
    raise TimeoutError(message)


async def wait_for_cluster_formation(
    nodes: list[ActorSystem],
    *,
    timeout: float = 10.0,
    interval: float = 0.5,
) -> None:
    """Wait until all nodes have consistent hash ring with full node IDs.

    This ensures all nodes have:
    1. The expected number of nodes in their hash ring
    2. All node IDs have UUIDs (not seed IDs like 127.0.0.1:8001)

    Args:
        nodes: List of distributed actor systems
        timeout: Maximum time to wait
        interval: Time between checks
    """
    num_nodes = len(nodes)

    def check_formation() -> bool:
        for node in nodes:
            if not hasattr(node, "_router") or not node._router:
                print(f"  Node {node} has no router")
                return False

            ring_size = len(node._router.hash_ring.nodes)
            ring_ids = list(node._router.hash_ring.nodes)
            print(f"  Node {node._node_id}: ring_size={ring_size}, expected={num_nodes}, ids={ring_ids}")

            if ring_size != num_nodes:
                return False

            # Ensure all node IDs have UUIDs (not seed IDs)
            for node_id in node._router.hash_ring.nodes:
                if "-" not in node_id:
                    print(f"    -> seed ID detected: {node_id}")
                    return False

        return True

    await retry_until(
        check_formation,
        timeout=timeout,
        interval=interval,
        message=f"Cluster formation not complete: expected {num_nodes} nodes with full IDs",
    )

def next_free_port() -> int:
    import socket

    with socket.socket() as s:
        s.bind(('', 0))
        return s.getsockname()[1]

def free_ports(count: int) -> list[int]:
    return [next_free_port() for _ in range(count)]
