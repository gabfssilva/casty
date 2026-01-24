import pytest
from unittest.mock import Mock
from casty.cluster.shard import ShardCoordinator


@pytest.fixture
def mock_hash_ring():
    ring = Mock()
    ring.get_n_nodes = Mock(return_value=["node-1", "node-2", "node-3"])
    return ring


@pytest.fixture
def mock_membership():
    membership = Mock()
    membership.is_alive = Mock(side_effect=lambda n: n != "node-1")
    return membership


def test_is_leader_when_first_alive(mock_hash_ring, mock_membership):
    coord = ShardCoordinator(
        node_id="node-2",
        hash_ring=mock_hash_ring,
        membership=mock_membership,
    )
    assert coord.is_leader("actor-123") is True


def test_is_not_leader_when_not_first_alive(mock_hash_ring, mock_membership):
    coord = ShardCoordinator(
        node_id="node-3",
        hash_ring=mock_hash_ring,
        membership=mock_membership,
    )
    assert coord.is_leader("actor-123") is False


def test_get_leader_id_returns_first_alive(mock_hash_ring, mock_membership):
    coord = ShardCoordinator(
        node_id="node-3",
        hash_ring=mock_hash_ring,
        membership=mock_membership,
    )
    assert coord.get_leader_id("actor-123") == "node-2"


def test_get_replica_ids_excludes_leader(mock_hash_ring, mock_membership):
    coord = ShardCoordinator(
        node_id="node-2",
        hash_ring=mock_hash_ring,
        membership=mock_membership,
    )
    replicas = coord.get_replica_ids("actor-123")
    assert "node-2" not in replicas
    assert "node-3" in replicas


def test_get_responsible_nodes(mock_hash_ring, mock_membership):
    coord = ShardCoordinator(
        node_id="node-2",
        hash_ring=mock_hash_ring,
        membership=mock_membership,
    )
    nodes = coord.get_responsible_nodes("actor-123", replicas=3)
    assert nodes == ["node-1", "node-2", "node-3"]


def test_all_nodes_dead_raises():
    ring = Mock()
    ring.get_n_nodes = Mock(return_value=["node-1", "node-2"])
    membership = Mock()
    membership.is_alive = Mock(return_value=False)

    coord = ShardCoordinator(
        node_id="node-3",
        hash_ring=ring,
        membership=membership,
    )

    with pytest.raises(RuntimeError, match="No alive nodes"):
        coord.get_leader_id("actor-123")
