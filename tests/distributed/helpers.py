"""Helper functions for distributed tests (non-fixtures)."""

import asyncio
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from casty import DistributedActorSystem


async def wait_for_leader(
    nodes: list["DistributedActorSystem"],
    *,
    timeout: float = 5.0,
    poll_interval: float = 0.1,
) -> "DistributedActorSystem":
    """
    Poll until exactly one leader emerges.

    Returns the leader node.
    Raises TimeoutError if no leader elected within timeout.
    """
    elapsed = 0.0
    while elapsed < timeout:
        leaders = [n for n in nodes if n.is_leader]
        if len(leaders) == 1:
            return leaders[0]
        await asyncio.sleep(poll_interval)
        elapsed += poll_interval

    raise TimeoutError(f"No leader elected within {timeout}s")


async def wait_for_discovery(
    node: "DistributedActorSystem",
    expected_peers: int,
    *,
    timeout: float = 2.0,
    poll_interval: float = 0.1,
) -> None:
    """
    Wait until node has discovered expected number of peers.

    Raises TimeoutError if discovery doesn't complete within timeout.
    """
    elapsed = 0.0
    while elapsed < timeout:
        if len(node._cluster.known_nodes) >= expected_peers:
            return
        await asyncio.sleep(poll_interval)
        elapsed += poll_interval

    raise TimeoutError(
        f"Node only discovered {len(node._cluster.known_nodes)}/{expected_peers} peers"
    )


def assert_single_leader(nodes: list["DistributedActorSystem"]) -> None:
    """Assert exactly one node is leader and others agree on leader_id."""
    leaders = [n for n in nodes if n.is_leader]
    assert len(leaders) == 1, f"Expected 1 leader, got {len(leaders)}"

    leader = leaders[0]
    for node in nodes:
        if not node.is_leader:
            assert node.leader_id == leader.node_id, (
                f"Follower {node.node_id[:8]} disagrees on leader"
            )
