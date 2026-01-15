"""Development utilities for Casty clusters."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager

from casty import ActorSystem
from casty.chaos import ChaosConfig, ChaosProxy, StartProxy
from casty.cluster.config import ClusterConfig
from casty.cluster.clustered_system import ClusteredActorSystem


def _next_free_port() -> int:
    """Find a free port on localhost."""
    import socket

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@asynccontextmanager
async def DevelopmentCluster(
    nodes: int,
    *,
    node_id_prefix: str = "node",
    chaos: ChaosConfig | None = None,
    cluster_config: ClusterConfig | None = None,
):
    """Create a local development cluster with N nodes.

    This is a convenience helper for testing and development. It creates
    a cluster of N nodes on localhost with automatic port assignment.

    Args:
        nodes: Number of nodes in the cluster (1-N)
        node_id_prefix: Prefix for node IDs. Generates "{prefix}-0", "{prefix}-1", etc.
        chaos: Optional chaos configuration. If provided, proxies are created
               between nodes to simulate network conditions (latency, loss, etc).
        cluster_config: Optional cluster configuration. If not provided:
               - With chaos: uses ClusterConfig.production() (longer timeouts)
               - Without chaos: uses ClusterConfig.development() (fast timeouts)

    Yields:
        Tuple of ClusteredActorSystem instances

    Example:
        ```python
        from casty.cluster import DevelopmentCluster
        from casty.chaos import ChaosConfig

        # Without chaos
        async with DevelopmentCluster(3) as (sys1, sys2, sys3):
            counters = await sys1.spawn(Counter, name="counters", sharded=True)
            await counters["c1"].send(Increment(5))

        # With simulated network latency (uses production timeouts automatically)
        async with DevelopmentCluster(3, chaos=ChaosConfig.slow_network()) as cluster:
            # All inter-node traffic has ~100ms latency
            sys1, sys2, sys3 = cluster
            ...

        # With custom node IDs:
        async with DevelopmentCluster(2, node_id_prefix="worker") as (w1, w2):
            assert w1.node_id == "worker-0"
            assert w2.node_id == "worker-1"
        ```
    """
    # Determine base config based on chaos mode
    if cluster_config is not None:
        base_config = cluster_config
    elif chaos is not None:
        # With chaos, use production timeouts (longer) to tolerate latency
        base_config = ClusterConfig.production()
    else:
        # No chaos, use development timeouts (fast)
        base_config = ClusterConfig.development()

    if chaos is None:
        # No chaos - direct connections
        async with _create_cluster_direct(nodes, node_id_prefix, base_config) as systems:
            yield systems
    else:
        # With chaos - proxied connections
        async with _create_cluster_with_chaos(nodes, node_id_prefix, chaos, base_config) as systems:
            yield systems


@asynccontextmanager
async def _create_cluster_direct(
    nodes: int,
    node_id_prefix: str,
    base_config: ClusterConfig,
):
    """Create cluster with direct connections (no chaos)."""
    head_port = _next_free_port()

    # Configure head node
    head_config = (
        base_config
        .with_bind_address("127.0.0.1", head_port)
        .with_node_id(f"{node_id_prefix}-0")
    )
    head = ClusteredActorSystem(head_config)

    if nodes == 1:
        async with ActorSystem.all(head) as systems:
            yield systems
        return

    # Configure other nodes
    others = []
    for i in range(1, nodes):
        node_config = (
            base_config
            .with_bind_address("127.0.0.1", _next_free_port())
            .with_node_id(f"{node_id_prefix}-{i}")
            .with_seeds([f"127.0.0.1:{head_port}"])
        )
        others.append(ClusteredActorSystem(node_config))

    async with ActorSystem.all(head, *others) as systems:
        yield systems


@asynccontextmanager
async def _create_cluster_with_chaos(
    nodes: int,
    node_id_prefix: str,
    chaos: ChaosConfig,
    base_config: ClusterConfig,
):
    """Create cluster with chaos proxies between nodes.

    Architecture:
        Node 0 (:real_port_0) ← Proxy 0 (:proxy_port_0)
        Node 1 (:real_port_1) ← Proxy 1 (:proxy_port_1)
        ...

        Seeds point to proxy ports, so all inter-node traffic
        goes through the chaos proxies.
    """
    # We need a helper system to manage the proxies
    proxy_system = ActorSystem()
    await proxy_system.start()

    try:
        # Allocate all ports upfront (node ports + proxy ports)
        node_ports: list[int] = [_next_free_port() for _ in range(nodes)]
        proxy_ports: list[int] = [_next_free_port() for _ in range(nodes)]

        # Create a proxy for each node
        for node_port, proxy_port in zip(node_ports, proxy_ports):
            proxy = await proxy_system.spawn(ChaosProxy)
            await proxy.send(
                StartProxy(
                    listen_port=proxy_port,
                    target_host="127.0.0.1",
                    target_port=node_port,
                    chaos=chaos,
                )
            )

        # Small delay to let proxies bind
        await asyncio.sleep(0.05)

        # Create the head node - advertise the PROXY port so others connect through it
        head_config = (
            base_config
            .with_bind_address("127.0.0.1", node_ports[0])
            .with_node_id(f"{node_id_prefix}-0")
            .with_advertise_address("127.0.0.1", proxy_ports[0])
        )
        head = ClusteredActorSystem(head_config)

        if nodes == 1:
            async with ActorSystem.all(head) as systems:
                yield systems
            return

        # Create other nodes - they connect to head's PROXY port
        # and advertise their own PROXY port
        head_proxy_port = proxy_ports[0]
        others = []
        for i in range(1, nodes):
            node_config = (
                base_config
                .with_bind_address("127.0.0.1", node_ports[i])
                .with_node_id(f"{node_id_prefix}-{i}")
                .with_seeds([f"127.0.0.1:{head_proxy_port}"])
                .with_advertise_address("127.0.0.1", proxy_ports[i])
            )
            others.append(ClusteredActorSystem(node_config))

        async with ActorSystem.all(head, *others) as systems:
            yield systems

    finally:
        await proxy_system.shutdown()
