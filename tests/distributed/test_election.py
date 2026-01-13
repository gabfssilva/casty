"""Tests for Raft-lite leader election."""

import pytest

from .helpers import assert_single_leader, wait_for_leader

pytestmark = pytest.mark.slow


class TestLeaderElection:
    """Tests for Raft-lite leader election."""

    async def test_single_node_becomes_leader(self, distributed_node) -> None:
        """A single node with expected_cluster_size=1 becomes leader."""
        node = await distributed_node(expected_cluster_size=1)

        leader = await wait_for_leader([node.system], timeout=3.0)

        assert leader is node.system

    async def test_three_nodes_elect_one_leader(self, n_node_cluster) -> None:
        """Three nodes form cluster and elect exactly one leader."""
        nodes = await n_node_cluster(3)
        systems = [n.system for n in nodes]

        await wait_for_leader(systems, timeout=3.0)
        assert_single_leader(systems)


class TestScalability:
    """Tests for cluster scalability with N nodes and leader election."""

    @pytest.mark.parametrize("num_nodes", [3, 5, 7])
    async def test_n_nodes_elect_single_leader(self, n_node_cluster, num_nodes: int) -> None:
        """N nodes form cluster and elect exactly one leader."""
        nodes = await n_node_cluster(num_nodes)
        systems = [n.system for n in nodes]

        await wait_for_leader(systems, timeout=5.0)
        assert_single_leader(systems)
