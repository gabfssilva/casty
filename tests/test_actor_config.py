import pytest
from casty.actor_config import ActorReplicationConfig, WriteQuorum


def test_write_quorum_async():
    config = ActorReplicationConfig(clustered=True, replicas=3, write_quorum="async")
    assert config.write_quorum == "async"


def test_write_quorum_int():
    config = ActorReplicationConfig(clustered=True, replicas=3, write_quorum=2)
    assert config.write_quorum == 2


def test_write_quorum_quorum():
    config = ActorReplicationConfig(clustered=True, replicas=3, write_quorum="quorum")
    assert config.write_quorum == "quorum"


def test_write_quorum_all():
    config = ActorReplicationConfig(clustered=True, replicas=3, write_quorum="all")
    assert config.write_quorum == "all"


def test_replicas_implies_clustered():
    config = ActorReplicationConfig(replicas=3)
    assert config.clustered is True


def test_default_replicas_when_clustered():
    config = ActorReplicationConfig(clustered=True)
    assert config.replicas == 2


def test_default_write_quorum():
    config = ActorReplicationConfig(clustered=True)
    assert config.write_quorum == "async"


def test_resolve_quorum_value():
    config = ActorReplicationConfig(clustered=True, replicas=5, write_quorum="quorum")
    assert config.resolve_write_quorum() == 3  # (5 // 2) + 1


def test_resolve_quorum_all():
    config = ActorReplicationConfig(clustered=True, replicas=3, write_quorum="all")
    assert config.resolve_write_quorum() == 3


def test_resolve_quorum_async():
    config = ActorReplicationConfig(clustered=True, replicas=3, write_quorum="async")
    assert config.resolve_write_quorum() == 0


def test_resolve_quorum_int():
    config = ActorReplicationConfig(clustered=True, replicas=3, write_quorum=2)
    assert config.resolve_write_quorum() == 2


def test_replicas_must_be_positive():
    with pytest.raises(ValueError, match="replicas must be > 0"):
        ActorReplicationConfig(replicas=0)


def test_replicas_must_be_positive_negative():
    with pytest.raises(ValueError, match="replicas must be > 0"):
        ActorReplicationConfig(replicas=-1)
