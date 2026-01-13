"""Tests for distributed system lifecycle and shutdown."""

import pytest

from casty import DistributedActorSystem

pytestmark = pytest.mark.slow


class TestShutdownDistributed:
    """Tests for distributed shutdown."""

    async def test_shutdown_closes_server(self, distributed_node) -> None:
        node = await distributed_node(expected_cluster_size=1)

        # Shutdown is handled by fixture, but we test server state
        await node.system.shutdown()

        assert node.system._server is not None
        assert not node.system._server.is_serving()

    async def test_shutdown_closes_peer_connections(self, two_node_cluster) -> None:
        _server, client = two_node_cluster

        assert len(client.system._cluster.peers) > 0

        await client.system.shutdown()
        assert len(client.system._cluster.peers) == 0


class TestContextManager:
    """Tests for async context manager protocol."""

    async def test_context_manager_starts_server(self, get_port) -> None:
        """Context manager should start server automatically."""
        async with DistributedActorSystem("127.0.0.1", get_port()) as system:
            assert system._server is not None
            assert system._server.is_serving()

    async def test_context_manager_shuts_down_on_exit(self, get_port) -> None:
        """Context manager should shut down system on exit."""
        system = DistributedActorSystem("127.0.0.1", get_port())

        async with system:
            assert system._running is True

        assert system._running is False
        assert not system._server.is_serving()

    async def test_context_manager_cleans_up_on_exception(self, get_port) -> None:
        """Context manager should clean up even if exception occurs."""
        system = DistributedActorSystem("127.0.0.1", get_port())

        with pytest.raises(ValueError):
            async with system:
                assert system._running is True
                raise ValueError("Test error")

        assert system._running is False
        assert not system._server.is_serving()
