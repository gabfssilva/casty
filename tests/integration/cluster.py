"""Test host wiring Server + Pool + Membership — the same wiring the Node
(etapa 4) does in production. Real TCP on localhost."""

from __future__ import annotations

import asyncio
import socket
import uuid
from collections.abc import Sequence

from casty.config import MembershipConfig, TransportConfig
from casty.membership.service import Membership
from casty.membership.table import Member, ViewEvent
from casty.transport.connection import LocalNode
from casty.transport.pool import Pool
from casty.transport.server import Server


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
        assert isinstance(port, int)
        return port


FAST_MEMBERSHIP = MembershipConfig(
    shuffle_interval=0.2,
    suspicion_timeout=0.6,
    anti_entropy_interval=0.5,
    graft_timeout=0.2,
    sweep_interval=0.05,
    join_timeout=5.0,
)

FAST_TRANSPORT = TransportConfig(
    connect_timeout=2.0,
    request_timeout=2.0,
    keepalive_interval=0.5,
    keepalive_timeout=0.5,
    reconnect_base=0.05,
    reconnect_max=0.5,
)


class ClusterNode:
    def __init__(
        self,
        membership: Membership,
        pool: Pool,
        server: Server,
        member: Member,
    ) -> None:
        self.membership = membership
        self.pool = pool
        self.server = server
        self.member = member
        self.events: list[ViewEvent] = []
        membership.subscribe(self.events.append)

    @classmethod
    async def start(
        cls,
        *,
        cluster: str = "test-cluster",
        membership_config: MembershipConfig = FAST_MEMBERSHIP,
        transport_config: TransportConfig = FAST_TRANSPORT,
    ) -> ClusterNode:
        port = free_port()
        addr = f"127.0.0.1:{port}"
        local = LocalNode(node_id=uuid.uuid4(), cluster_name=cluster, listen_addr=addr)
        member = Member(node_id=local.node_id, addr=addr)
        membership = Membership(member, membership_config)
        pool = Pool(
            local=local,
            config=transport_config,
            on_control=membership.handle_control,
            on_close=membership.handle_close,
        )
        membership.pool = pool
        server = await Server.start(
            "127.0.0.1",
            port,
            local=local,
            config=transport_config,
            on_connection=membership.handle_connection,
            on_control=membership.handle_control,
            on_close=membership.handle_close,
        )
        membership.start()
        return cls(membership, pool, server, member)

    async def join(self, seeds: Sequence[str]) -> None:
        await self.membership.join(seeds)

    async def leave(self) -> None:
        await self.membership.leave()
        self.server.stop_accepting()
        await self.pool.close()
        await self.server.close()

    async def kill(self) -> None:
        """Abrupt death: no leave broadcast, connections just drop."""
        await self.membership.stop()
        self.server.stop_accepting()
        await self.pool.close()
        await self.server.close()

    def view(self) -> frozenset[Member]:
        return self.membership.alive_members()


async def start_cluster(n: int, **kwargs: object) -> list[ClusterNode]:
    nodes = [await ClusterNode.start() for _ in range(n)]
    seed_addr = nodes[0].member.addr
    for node in nodes[1:]:
        await node.join([seed_addr])
    return nodes


async def wait_converged(
    nodes: Sequence[ClusterNode],
    expected: set[Member],
    *,
    timeout: float = 10.0,
) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if all(set(node.view()) == expected for node in nodes):
            return
        await asyncio.sleep(0.05)
    views = {node.member.addr: sorted(m.addr for m in node.view()) for node in nodes}
    raise AssertionError(f"views did not converge to {sorted(m.addr for m in expected)}: {views}")
