import pytest


def test_routing_enum_values():
    from casty.cluster.replication import Routing

    assert Routing.LEADER.value == "leader"
    assert Routing.ANY.value == "any"
    assert Routing.LOCAL_FIRST.value == "local"


def test_replication_config_defaults():
    from casty.cluster.replication import ReplicationConfig, Routing

    config = ReplicationConfig()

    assert config.factor == 3
    assert config.write_quorum == 2
    assert config.routing == Routing.LEADER


def test_replication_config_custom():
    from casty.cluster.replication import ReplicationConfig, Routing

    config = ReplicationConfig(
        factor=5,
        write_quorum=3,
        routing=Routing.ANY,
    )

    assert config.factor == 5
    assert config.write_quorum == 3
    assert config.routing == Routing.ANY


def test_replicated_decorator_attaches_config():
    from casty.cluster.replication import replicated, ReplicationConfig, Routing

    @replicated(factor=3, write_quorum=2, routing=Routing.LEADER)
    async def my_actor():
        pass

    assert hasattr(my_actor, "__replication__")
    config = my_actor.__replication__
    assert isinstance(config, ReplicationConfig)
    assert config.factor == 3
    assert config.write_quorum == 2
    assert config.routing == Routing.LEADER


def test_replicated_decorator_default_values():
    from casty.cluster.replication import replicated, Routing

    @replicated()
    async def my_actor():
        pass

    config = my_actor.__replication__
    assert config.factor == 3
    assert config.write_quorum == 2
    assert config.routing == Routing.LEADER
