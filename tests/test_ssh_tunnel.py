"""Integration test: ClusterClient routing through a real SSH forward tunnel.

Starts an in-process asyncssh server, creates a forward tunnel from a local
port to the cluster's TCP port, and verifies that ClusterClient can route
ShardEnvelope messages end-to-end through the tunnel.
"""

from __future__ import annotations

import asyncio
import os
import tempfile
from dataclasses import dataclass
from typing import Any

import asyncssh
import pytest

from casty import ActorRef, Behavior, Behaviors
from casty.client.client import ClusterClient
from casty.cluster.system import ClusteredActorSystem
from casty.cluster.envelope import ShardEnvelope


@dataclass(frozen=True)
class Ping:
    reply_to: ActorRef[str]


type EchoMsg = Ping


def echo_entity(entity_id: str) -> Behavior[EchoMsg]:
    async def receive(_ctx: Any, msg: Any) -> Any:
        match msg:
            case Ping(reply_to=reply_to):
                reply_to.tell(f"pong-{entity_id}")
                return Behaviors.same()
            case _:
                return Behaviors.same()

    return Behaviors.receive(receive)


class TunnelSSHServer(asyncssh.SSHServer):
    """Minimal SSH server that allows TCP forwarding."""

    def begin_auth(self, username: str) -> bool:
        return False

    def connection_requested(
        self, dest_host: str, dest_port: int, orig_host: str, orig_port: int
    ) -> bool:
        return True


@pytest.mark.timeout(30)
async def test_client_through_ssh_forward_tunnel() -> None:
    """Messages flow through a real SSH forward tunnel to a cluster node."""
    key = asyncssh.generate_private_key("ssh-rsa")
    with tempfile.NamedTemporaryFile(suffix=".key", delete=False) as f:
        f.write(key.export_private_key())
        key_file = f.name

    try:
        # 1. Start SSH server
        ssh_server = await asyncssh.create_server(
            TunnelSSHServer,
            "127.0.0.1",
            0,
            server_host_keys=[key_file],
        )
        ssh_port = ssh_server.sockets[0].getsockname()[1]

        # 2. Start cluster node
        async with ClusteredActorSystem(
            name="tunnel-cluster",
            host="127.0.0.1",
            port=0,
            node_id="node-1",
        ) as system:
            cluster_port = system.self_node.port

            system.spawn(
                Behaviors.sharded(entity_factory=echo_entity, num_shards=10),
                "echo",
            )
            await asyncio.sleep(0.3)

            # 3. Create forward tunnel: local_port -> SSH -> cluster_port
            async with asyncssh.connect(
                "127.0.0.1",
                ssh_port,
                known_hosts=None,
            ) as ssh_conn:
                listener = await ssh_conn.forward_local_port(
                    "", 0, "127.0.0.1", cluster_port
                )
                tunnel_port = listener.get_port()

                # 4. ClusterClient routing through the tunnel
                # The contact point is a fake address that gets mapped to the
                # tunnel endpoint. The topology snapshot will report the real
                # node address ("127.0.0.1", cluster_port), which also needs
                # mapping through the tunnel.
                async with ClusterClient(
                    contact_points=[("10.99.99.1", 25520)],
                    system_name="tunnel-cluster",
                    address_map={
                        ("10.99.99.1", 25520): ("127.0.0.1", tunnel_port),
                        ("127.0.0.1", cluster_port): (
                            "127.0.0.1",
                            tunnel_port,
                        ),
                    },
                ) as client:
                    await asyncio.sleep(1.5)

                    echo_ref = client.entity_ref("echo", num_shards=10)
                    await asyncio.sleep(0.5)
                    result = await client.ask(
                        echo_ref,
                        lambda r: ShardEnvelope("test-entity", Ping(reply_to=r)),
                        timeout=5.0,
                    )
                    assert result == "pong-test-entity"

        ssh_server.close()
    finally:
        os.unlink(key_file)


@pytest.mark.timeout(30)
async def test_client_multiple_asks_through_tunnel() -> None:
    """Multiple ask() calls succeed through the SSH tunnel."""
    key = asyncssh.generate_private_key("ssh-rsa")
    with tempfile.NamedTemporaryFile(suffix=".key", delete=False) as f:
        f.write(key.export_private_key())
        key_file = f.name

    try:
        ssh_server = await asyncssh.create_server(
            TunnelSSHServer,
            "127.0.0.1",
            0,
            server_host_keys=[key_file],
        )
        ssh_port = ssh_server.sockets[0].getsockname()[1]

        async with ClusteredActorSystem(
            name="tunnel-cluster",
            host="127.0.0.1",
            port=0,
            node_id="node-1",
        ) as system:
            cluster_port = system.self_node.port

            system.spawn(
                Behaviors.sharded(entity_factory=echo_entity, num_shards=10),
                "echo",
            )
            await asyncio.sleep(0.3)

            async with asyncssh.connect(
                "127.0.0.1",
                ssh_port,
                known_hosts=None,
            ) as ssh_conn:
                listener = await ssh_conn.forward_local_port(
                    "", 0, "127.0.0.1", cluster_port
                )
                tunnel_port = listener.get_port()

                async with ClusterClient(
                    contact_points=[("10.99.99.1", 25520)],
                    system_name="tunnel-cluster",
                    address_map={
                        ("10.99.99.1", 25520): ("127.0.0.1", tunnel_port),
                        ("127.0.0.1", cluster_port): (
                            "127.0.0.1",
                            tunnel_port,
                        ),
                    },
                ) as client:
                    await asyncio.sleep(1.5)

                    echo_ref = client.entity_ref("echo", num_shards=10)
                    await asyncio.sleep(0.5)

                    for entity_id in ("alice", "bob", "carol"):
                        eid = entity_id
                        result = await client.ask(
                            echo_ref,
                            lambda r, e=eid: ShardEnvelope(e, Ping(reply_to=r)),
                            timeout=5.0,
                        )
                        assert result == f"pong-{entity_id}"

        ssh_server.close()
    finally:
        os.unlink(key_file)
