"""Tests for TLS/mTLS support."""

import asyncio
import ssl

import pytest
import trustme

from casty import DistributedActorSystem

pytestmark = pytest.mark.slow


@pytest.fixture
def mtls_contexts():
    """Create mTLS contexts using in-memory certificates."""
    ca = trustme.CA()

    # Server identity (used by both nodes in mTLS)
    server_cert = ca.issue_cert("localhost", "127.0.0.1")

    # Server context (for accepting connections)
    server_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    server_cert.configure_cert(server_ctx)
    ca.configure_trust(server_ctx)
    server_ctx.verify_mode = ssl.CERT_REQUIRED

    # Client context (for outgoing connections)
    client_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    server_cert.configure_cert(client_ctx)
    ca.configure_trust(client_ctx)
    client_ctx.check_hostname = False

    return server_ctx, client_ctx


class TestTLS:
    """Tests for TLS connections between nodes."""

    async def test_two_nodes_connect_with_tls(self, get_port, mtls_contexts) -> None:
        """Two nodes can establish mTLS connection."""
        server_ctx, client_ctx = mtls_contexts
        port1 = get_port()
        port2 = get_port()

        # Node 1 (server)
        node1 = DistributedActorSystem(
            "127.0.0.1", port1,
            expected_cluster_size=2,
            ssl_context=server_ctx,
        )

        # Node 2 (connects to node1)
        node2 = DistributedActorSystem(
            "127.0.0.1", port2,
            seeds=[f"127.0.0.1:{port1}"],
            expected_cluster_size=2,
            ssl_context=client_ctx,
        )

        task1 = asyncio.create_task(node1.serve())
        await asyncio.sleep(0.1)

        task2 = asyncio.create_task(node2.serve())
        await asyncio.sleep(0.5)

        try:
            # Verify connection was established
            assert len(node1._cluster.known_nodes) == 1
            assert len(node2._cluster.known_nodes) == 1
        finally:
            await node1.shutdown()
            await node2.shutdown()
            task1.cancel()
            task2.cancel()

    async def test_node_without_tls_cannot_connect_to_tls_node(self, get_port, mtls_contexts) -> None:
        """A non-TLS node cannot connect to a TLS-enabled node."""
        server_ctx, _ = mtls_contexts
        port1 = get_port()
        port2 = get_port()

        # Node 1 with TLS
        node1 = DistributedActorSystem(
            "127.0.0.1", port1,
            expected_cluster_size=2,
            ssl_context=server_ctx,
        )

        # Node 2 without TLS tries to connect
        node2 = DistributedActorSystem(
            "127.0.0.1", port2,
            seeds=[f"127.0.0.1:{port1}"],
            expected_cluster_size=2,
            ssl_context=None,  # No TLS
        )

        task1 = asyncio.create_task(node1.serve())
        await asyncio.sleep(0.1)

        task2 = asyncio.create_task(node2.serve())
        await asyncio.sleep(0.5)

        try:
            # Connection should fail - no peers discovered
            assert len(node1._cluster.known_nodes) == 0
            assert len(node2._cluster.known_nodes) == 0
        finally:
            await node1.shutdown()
            await node2.shutdown()
            task1.cancel()
            task2.cancel()
