from __future__ import annotations

from contextlib import asynccontextmanager

from .clustered_system import ClusteredActorSystem
from .config import ClusterConfig


def _next_free_port() -> int:
    import socket

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@asynccontextmanager
async def DevelopmentCluster(
    nodes: int = 1,
    *,
    node_id_prefix: str = "node",
):
    head_port = _next_free_port()
    base_config = ClusterConfig.development()

    head = ClusteredActorSystem(
        ClusterConfig(
            bind_host="127.0.0.1",
            bind_port=head_port,
            node_id=f"{node_id_prefix}-0",
            protocol_period=base_config.protocol_period,
            ping_timeout=base_config.ping_timeout,
            ping_req_timeout=base_config.ping_req_timeout,
            suspicion_mult=base_config.suspicion_mult,
        )
    )

    if nodes == 1:
        async with head:
            yield (head,)
        return

    others = []
    for i in range(1, nodes):
        node = ClusteredActorSystem(
            ClusterConfig(
                bind_host="127.0.0.1",
                bind_port=_next_free_port(),
                node_id=f"{node_id_prefix}-{i}",
                seeds=[f"127.0.0.1:{head_port}"],
                protocol_period=base_config.protocol_period,
                ping_timeout=base_config.ping_timeout,
                ping_req_timeout=base_config.ping_req_timeout,
                suspicion_mult=base_config.suspicion_mult,
            )
        )
        others.append(node)

    async with head:
        for other in others:
            await other.start()

        try:
            yield (head, *others)
        finally:
            for other in reversed(others):
                await other.shutdown()
