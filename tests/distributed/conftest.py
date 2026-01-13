"""Fixtures for distributed actor system tests."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator, Awaitable, Callable
from dataclasses import dataclass

import pytest

from casty import DistributedActorSystem


@dataclass
class NodeHandle:
    """Wrapper for a running distributed node."""

    system: DistributedActorSystem
    task: asyncio.Task[None]
    port: int

    async def shutdown(self) -> None:
        """Cleanly shut down node and cancel serve task."""
        await self.system.shutdown()
        self.task.cancel()
        try:
            await self.task
        except asyncio.CancelledError:
            pass


# Type alias for factory functions
NodeFactory = Callable[..., Awaitable[NodeHandle]]
ClusterFactory = Callable[[int], Awaitable[list[NodeHandle]]]


@pytest.fixture
async def distributed_node(
    get_port: Callable[[], int],
) -> AsyncGenerator[NodeFactory, None]:
    """
    Factory fixture that creates distributed nodes with automatic cleanup.

    Usage:
        node = await distributed_node()
        node = await distributed_node(seeds=["127.0.0.1:20001"])
        node = await distributed_node(expected_cluster_size=3)
    """
    nodes: list[NodeHandle] = []

    async def _create_node(
        *,
        seeds: list[str] | None = None,
        expected_cluster_size: int = 3,
    ) -> NodeHandle:
        port = get_port()
        system = DistributedActorSystem(
            "127.0.0.1",
            port,
            seeds=seeds,
            expected_cluster_size=expected_cluster_size,
        )

        task = asyncio.create_task(system.serve())
        await asyncio.sleep(0.1)  # Let server start

        handle = NodeHandle(system=system, task=task, port=port)
        nodes.append(handle)
        return handle

    yield _create_node

    # Cleanup all created nodes
    for node in reversed(nodes):
        await node.shutdown()


@pytest.fixture
async def two_node_cluster(
    distributed_node: NodeFactory,
) -> AsyncGenerator[tuple[NodeHandle, NodeHandle], None]:
    """
    Pre-configured two-node cluster (server + client).

    Returns (server, client) where client is connected to server.
    """
    server = await distributed_node(expected_cluster_size=2)
    client = await distributed_node(
        seeds=[f"127.0.0.1:{server.port}"],
        expected_cluster_size=2,
    )

    yield server, client


@pytest.fixture
async def n_node_cluster(
    get_port: Callable[[], int],
) -> AsyncGenerator[ClusterFactory, None]:
    """
    Factory for creating N-node clusters with proper cleanup.

    Usage:
        nodes = await n_node_cluster(5)
    """
    all_nodes: list[NodeHandle] = []

    async def _create_cluster(n: int) -> list[NodeHandle]:
        base_port = get_port()
        for _ in range(n - 1):
            get_port()  # Reserve ports

        nodes: list[NodeHandle] = []
        for i in range(n):
            seeds = [f"127.0.0.1:{base_port}"] if i > 0 else []
            system = DistributedActorSystem(
                "127.0.0.1",
                base_port + i,
                seeds=seeds,
                expected_cluster_size=n,
            )

            task = asyncio.create_task(system.serve())
            await asyncio.sleep(0.05)  # Stagger startup

            handle = NodeHandle(system=system, task=task, port=base_port + i)
            nodes.append(handle)
            all_nodes.append(handle)

        return nodes

    yield _create_cluster

    # Cleanup
    for node in reversed(all_nodes):
        await node.shutdown()
