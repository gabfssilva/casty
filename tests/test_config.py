from __future__ import annotations

import re
from pathlib import Path

import pytest

from casty.config import (
    ActorConfig,
    CastyConfig,
    CompressionConfig,
    FailureDetectorConfig,
    GossipConfig,
    HeartbeatConfig,
    MailboxConfig,
    SerializationConfig,
    ShardingConfig,
    SupervisionConfig,
    discover_config,
    load_config,
)
from casty.core.replication import ReplicationConfig


# ---------------------------------------------------------------------------
# Config dataclass defaults
# ---------------------------------------------------------------------------


class TestMailboxConfig:
    def test_defaults(self) -> None:
        cfg = MailboxConfig()
        assert cfg.capacity is None
        assert cfg.strategy == "drop_new"

    def test_custom(self) -> None:
        cfg = MailboxConfig(capacity=500, strategy="reject")
        assert cfg.capacity == 500
        assert cfg.strategy == "reject"

    def test_frozen(self) -> None:
        cfg = MailboxConfig()
        with pytest.raises(AttributeError):
            cfg.capacity = 42  # type: ignore[misc]


class TestSupervisionConfig:
    def test_defaults(self) -> None:
        cfg = SupervisionConfig()
        assert cfg.strategy == "restart"
        assert cfg.max_restarts == 3
        assert cfg.within_seconds == 60.0

    def test_custom(self) -> None:
        cfg = SupervisionConfig(strategy="stop", max_restarts=10, within_seconds=30.0)
        assert cfg.strategy == "stop"
        assert cfg.max_restarts == 10
        assert cfg.within_seconds == 30.0


class TestShardingConfig:
    def test_defaults(self) -> None:
        cfg = ShardingConfig()
        assert cfg.num_shards == 256

    def test_custom(self) -> None:
        cfg = ShardingConfig(num_shards=1024)
        assert cfg.num_shards == 1024


class TestFailureDetectorConfig:
    def test_defaults(self) -> None:
        cfg = FailureDetectorConfig()
        assert cfg.threshold == 8.0
        assert cfg.max_sample_size == 200
        assert cfg.min_std_deviation_ms == 100.0
        assert cfg.acceptable_heartbeat_pause_ms == 0.0
        assert cfg.first_heartbeat_estimate_ms == 1000.0


class TestGossipConfig:
    def test_defaults(self) -> None:
        cfg = GossipConfig()
        assert cfg.interval == 1.0


class TestHeartbeatConfig:
    def test_defaults(self) -> None:
        cfg = HeartbeatConfig()
        assert cfg.interval == 0.5
        assert cfg.availability_check_interval == 2.0


# ---------------------------------------------------------------------------
# ActorConfig
# ---------------------------------------------------------------------------


class TestActorConfig:
    def test_stores_raw_dicts(self) -> None:
        cfg = ActorConfig(
            pattern=re.compile("orders"),
            mailbox_overrides={"capacity": 100},
            supervision_overrides={},
            sharding_overrides={"num_shards": 512},
            replication_overrides={"replicas": 3},
        )
        assert cfg.mailbox_overrides == {"capacity": 100}
        assert cfg.sharding_overrides == {"num_shards": 512}


# ---------------------------------------------------------------------------
# CastyConfig defaults
# ---------------------------------------------------------------------------


class TestCastyConfigDefaults:
    def test_all_defaults(self) -> None:
        cfg = CastyConfig()
        assert cfg.system_name == "casty"
        assert cfg.cluster is None
        assert cfg.transport.max_pending_per_path == 64
        assert isinstance(cfg.gossip, GossipConfig)
        assert isinstance(cfg.heartbeat, HeartbeatConfig)
        assert isinstance(cfg.failure_detector, FailureDetectorConfig)
        assert isinstance(cfg.defaults_mailbox, MailboxConfig)
        assert isinstance(cfg.defaults_supervision, SupervisionConfig)
        assert isinstance(cfg.defaults_sharding, ShardingConfig)
        assert isinstance(cfg.defaults_replication, ReplicationConfig)
        assert cfg.actors == ()


# ---------------------------------------------------------------------------
# resolve_actor
# ---------------------------------------------------------------------------


class TestResolveActor:
    def test_no_match_returns_defaults(self) -> None:
        cfg = CastyConfig()
        resolved = cfg.resolve_actor("unknown-actor")
        assert resolved.mailbox == MailboxConfig()
        assert resolved.supervision == SupervisionConfig()
        assert resolved.sharding == ShardingConfig()
        assert resolved.replication == ReplicationConfig()

    def test_exact_match(self) -> None:
        actor = ActorConfig(
            pattern=re.compile(r"^orders$"),
            mailbox_overrides={},
            supervision_overrides={},
            sharding_overrides={"num_shards": 1024},
            replication_overrides={"replicas": 5},
        )
        cfg = CastyConfig(actors=(actor,))
        resolved = cfg.resolve_actor("orders")
        assert resolved.sharding.num_shards == 1024
        assert resolved.replication.replicas == 5
        # Non-overridden fields keep defaults
        assert resolved.mailbox == MailboxConfig()
        assert resolved.supervision == SupervisionConfig()

    def test_regex_match(self) -> None:
        actor = ActorConfig(
            pattern=re.compile(r"child-\d+"),
            mailbox_overrides={"capacity": 50},
            supervision_overrides={},
            sharding_overrides={},
            replication_overrides={},
        )
        cfg = CastyConfig(actors=(actor,))
        resolved = cfg.resolve_actor("child-42")
        assert resolved.mailbox.capacity == 50
        assert resolved.mailbox.strategy == "drop_new"

    def test_regex_no_partial_match(self) -> None:
        actor = ActorConfig(
            pattern=re.compile(r"child-\d+"),
            mailbox_overrides={"capacity": 50},
            supervision_overrides={},
            sharding_overrides={},
            replication_overrides={},
        )
        cfg = CastyConfig(actors=(actor,))
        # Should NOT match partial strings
        assert cfg.resolve_actor("child-42-extra").mailbox == MailboxConfig()
        assert cfg.resolve_actor("prefix-child-42").mailbox == MailboxConfig()

    def test_first_match_wins(self) -> None:
        first = ActorConfig(
            pattern=re.compile(r"worker-\d+"),
            mailbox_overrides={"capacity": 100},
            supervision_overrides={},
            sharding_overrides={},
            replication_overrides={},
        )
        second = ActorConfig(
            pattern=re.compile(r"worker-.*"),
            mailbox_overrides={"capacity": 999},
            supervision_overrides={},
            sharding_overrides={},
            replication_overrides={},
        )
        cfg = CastyConfig(actors=(first, second))
        resolved = cfg.resolve_actor("worker-1")
        assert resolved.mailbox.capacity == 100

    def test_partial_override_merges_with_defaults(self) -> None:
        actor = ActorConfig(
            pattern=re.compile(r"^myactor$"),
            mailbox_overrides={"capacity": 100},
            supervision_overrides={},
            sharding_overrides={},
            replication_overrides={},
        )
        cfg_with_actor = CastyConfig(
            defaults_mailbox=MailboxConfig(capacity=5000, strategy="reject"),
            actors=(actor,),
        )
        resolved = cfg_with_actor.resolve_actor("myactor")
        assert resolved.mailbox.capacity == 100
        assert resolved.mailbox.strategy == "reject"

    def test_custom_defaults_used_when_no_match(self) -> None:
        cfg = CastyConfig(
            defaults_mailbox=MailboxConfig(capacity=2000, strategy="drop_oldest"),
            defaults_supervision=SupervisionConfig(strategy="stop"),
        )
        resolved = cfg.resolve_actor("anything")
        assert resolved.mailbox.capacity == 2000
        assert resolved.mailbox.strategy == "drop_oldest"
        assert resolved.supervision.strategy == "stop"


# ---------------------------------------------------------------------------
# TOML parsing — load_config(path)
# ---------------------------------------------------------------------------


class TestLoadConfigFull:
    def test_full_config(self, tmp_path: Path) -> None:
        toml_file = tmp_path / "casty.toml"
        toml_file.write_text("""\
[system]
name = "my-app"

[transport]
max_pending_per_path = 128

[defaults.mailbox]
capacity = 5000
strategy = "reject"

[defaults.supervision]
strategy = "stop"

[defaults.sharding]
num_shards = 512

[defaults.replication]
replicas = 3

[cluster]
host = "10.0.0.1"
port = 25520
seed_nodes = ["10.0.0.2:25520"]
roles = ["worker"]

[cluster.gossip]
interval = 2.0

[cluster.heartbeat]
interval = 1.0
availability_check_interval = 4.0

[cluster.failure_detector]
threshold = 12.0

[actors.orders]
sharding = { num_shards = 1024 }
replication = { replicas = 5 }

[actors.my-worker]
mailbox = { capacity = 100, strategy = "drop_oldest" }

[actors."child-\\\\d+"]
mailbox = { capacity = 50 }
""")
        cfg = load_config(toml_file)

        # System
        assert cfg.system_name == "my-app"

        # Transport
        assert cfg.transport.max_pending_per_path == 128

        # Defaults
        assert cfg.defaults_mailbox.capacity == 5000
        assert cfg.defaults_mailbox.strategy == "reject"
        assert cfg.defaults_supervision.strategy == "stop"
        assert cfg.defaults_sharding.num_shards == 512
        assert cfg.defaults_replication.replicas == 3

        # Cluster
        assert cfg.cluster is not None
        assert cfg.cluster.host == "10.0.0.1"
        assert cfg.cluster.port == 25520
        assert cfg.cluster.seed_nodes == (("10.0.0.2", 25520),)
        assert cfg.cluster.roles == frozenset({"worker"})

        # Cluster subsections
        assert cfg.gossip.interval == 2.0
        assert cfg.heartbeat.interval == 1.0
        assert cfg.heartbeat.availability_check_interval == 4.0
        assert cfg.failure_detector.threshold == 12.0

        # Actors
        assert len(cfg.actors) == 3

    def test_minimal_config(self, tmp_path: Path) -> None:
        toml_file = tmp_path / "casty.toml"
        toml_file.write_text("")
        cfg = load_config(toml_file)
        assert cfg.system_name == "casty"
        assert cfg.cluster is None
        assert cfg.actors == ()

    def test_file_not_found_raises(self, tmp_path: Path) -> None:
        with pytest.raises(FileNotFoundError):
            load_config(tmp_path / "nonexistent.toml")

    def test_seed_node_parsing(self, tmp_path: Path) -> None:
        toml_file = tmp_path / "casty.toml"
        toml_file.write_text("""\
[cluster]
host = "localhost"
port = 25520
seed_nodes = ["10.0.0.1:25520", "10.0.0.2:25521"]
""")
        cfg = load_config(toml_file)
        assert cfg.cluster is not None
        assert cfg.cluster.seed_nodes == (
            ("10.0.0.1", 25520),
            ("10.0.0.2", 25521),
        )

    def test_cluster_no_seed_nodes(self, tmp_path: Path) -> None:
        toml_file = tmp_path / "casty.toml"
        toml_file.write_text("""\
[cluster]
host = "localhost"
port = 25520
""")
        cfg = load_config(toml_file)
        assert cfg.cluster is not None
        assert cfg.cluster.seed_nodes == ()

    def test_cluster_no_roles(self, tmp_path: Path) -> None:
        toml_file = tmp_path / "casty.toml"
        toml_file.write_text("""\
[cluster]
host = "localhost"
port = 25520
""")
        cfg = load_config(toml_file)
        assert cfg.cluster is not None
        assert cfg.cluster.roles == frozenset()


# ---------------------------------------------------------------------------
# Actor pattern matching from TOML
# ---------------------------------------------------------------------------


class TestActorPatternFromToml:
    def test_exact_name_becomes_anchored_regex(self, tmp_path: Path) -> None:
        toml_file = tmp_path / "casty.toml"
        toml_file.write_text("""\
[actors.orders]
sharding = { num_shards = 1024 }
""")
        cfg = load_config(toml_file)
        resolved = cfg.resolve_actor("orders")
        assert resolved.sharding.num_shards == 1024
        # Should not match partial
        resolved_other = cfg.resolve_actor("orders-extra")
        assert resolved_other.sharding.num_shards == 256  # default

    def test_regex_pattern_from_toml(self, tmp_path: Path) -> None:
        toml_file = tmp_path / "casty.toml"
        toml_file.write_text("""\
[actors."child-\\\\d+"]
mailbox = { capacity = 50 }
""")
        cfg = load_config(toml_file)
        resolved = cfg.resolve_actor("child-123")
        assert resolved.mailbox.capacity == 50

    def test_resolve_actor_full_integration(self, tmp_path: Path) -> None:
        toml_file = tmp_path / "casty.toml"
        toml_file.write_text("""\
[defaults.mailbox]
capacity = 5000
strategy = "reject"

[actors.orders]
sharding = { num_shards = 1024 }
replication = { replicas = 5 }

[actors.my-worker]
mailbox = { capacity = 100, strategy = "drop_oldest" }
""")
        cfg = load_config(toml_file)

        # orders: sharding/replication overridden, mailbox from defaults
        orders = cfg.resolve_actor("orders")
        assert orders.sharding.num_shards == 1024
        assert orders.replication.replicas == 5
        assert orders.mailbox.capacity == 5000
        assert orders.mailbox.strategy == "reject"

        # my-worker: mailbox overridden
        worker = cfg.resolve_actor("my-worker")
        assert worker.mailbox.capacity == 100
        assert worker.mailbox.strategy == "drop_oldest"

        # unknown: all defaults
        unknown = cfg.resolve_actor("unknown")
        assert unknown.mailbox.capacity == 5000
        assert unknown.mailbox.strategy == "reject"
        assert unknown.sharding.num_shards == 256


# ---------------------------------------------------------------------------
# Auto-discovery
# ---------------------------------------------------------------------------


class TestDiscoverConfig:
    def test_finds_in_cwd(self, tmp_path: Path) -> None:
        toml_file = tmp_path / "casty.toml"
        toml_file.write_text('[system]\nname = "found-cwd"')
        result = discover_config(tmp_path)
        assert result == toml_file

    def test_walks_up(self, tmp_path: Path) -> None:
        toml_file = tmp_path / "casty.toml"
        toml_file.write_text('[system]\nname = "found-parent"')
        child = tmp_path / "a" / "b" / "c"
        child.mkdir(parents=True)
        result = discover_config(child)
        assert result == toml_file

    def test_not_found_returns_none(self, tmp_path: Path) -> None:
        child = tmp_path / "isolated"
        child.mkdir()
        result = discover_config(child)
        assert result is None

    def test_load_config_no_args_auto_discovery(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        toml_file = tmp_path / "casty.toml"
        toml_file.write_text('[system]\nname = "auto-discovered"')
        monkeypatch.chdir(tmp_path)
        cfg = load_config()
        assert cfg.system_name == "auto-discovered"

    def test_load_config_no_args_no_file_returns_defaults(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.chdir(tmp_path)
        cfg = load_config()
        assert cfg.system_name == "casty"
        assert cfg.cluster is None


# ---------------------------------------------------------------------------
# ActorSystem + Config integration
# ---------------------------------------------------------------------------


class TestActorSystemWithConfig:
    async def test_system_accepts_config(self, tmp_path: Path) -> None:
        from casty import ActorSystem

        toml_file = tmp_path / "casty.toml"
        toml_file.write_text(
            '[system]\nname = "configured"\n\n'
            "[actors.my-actor]\n"
            'mailbox = { capacity = 10, strategy = "reject" }\n'
        )
        config = load_config(toml_file)

        async with ActorSystem(name=config.system_name, config=config) as system:
            assert system.name == "configured"

    async def test_config_none_is_default(self) -> None:
        from casty import ActorSystem

        async with ActorSystem() as system:
            assert system.name == "casty-system"


# ---------------------------------------------------------------------------
# ClusteredActorSystem + Config integration
# ---------------------------------------------------------------------------


class TestClusteredSystemFromConfig:
    async def test_from_config(self, tmp_path: Path) -> None:
        from casty.cluster.system import ClusteredActorSystem

        toml_file = tmp_path / "casty.toml"
        toml_file.write_text(
            '[system]\nname = "cluster-test"\n\n'
            "[cluster]\n"
            'host = "127.0.0.1"\n'
            "port = 0\n"
            "seed_nodes = []\n"
        )
        config = load_config(toml_file)
        system = ClusteredActorSystem.from_config(config)
        assert system.name == "cluster-test"

    async def test_from_config_no_cluster_raises(self, tmp_path: Path) -> None:
        from casty.cluster.system import ClusteredActorSystem

        toml_file = tmp_path / "casty.toml"
        toml_file.write_text('[system]\nname = "no-cluster"\n')
        config = load_config(toml_file)
        with pytest.raises(ValueError, match="no \\[cluster\\] section"):
            ClusteredActorSystem.from_config(config)

    async def test_from_config_with_overrides(self, tmp_path: Path) -> None:
        from casty.cluster.system import ClusteredActorSystem

        toml_file = tmp_path / "casty.toml"
        toml_file.write_text(
            '[system]\nname = "override-test"\n\n'
            "[cluster]\n"
            'host = "10.0.0.1"\n'
            "port = 25520\n"
        )
        config = load_config(toml_file)
        system = ClusteredActorSystem.from_config(config, host="127.0.0.1", port=0)
        assert system._host == "127.0.0.1"
        assert system._port == 0


# ---------------------------------------------------------------------------
# CompressionConfig
# ---------------------------------------------------------------------------


class TestCompressionConfig:
    def test_defaults(self) -> None:
        cfg = CompressionConfig()
        assert cfg.algorithm == "zlib"
        assert cfg.level == 6

    def test_custom(self) -> None:
        cfg = CompressionConfig(algorithm="lzma", level=3)
        assert cfg.algorithm == "lzma"
        assert cfg.level == 3

    def test_frozen(self) -> None:
        cfg = CompressionConfig()
        with pytest.raises(AttributeError):
            cfg.level = 9  # type: ignore[misc]


# ---------------------------------------------------------------------------
# SerializationConfig
# ---------------------------------------------------------------------------


class TestSerializationConfig:
    def test_defaults(self) -> None:
        cfg = SerializationConfig()
        assert cfg.serializer == "pickle"
        assert cfg.compression is None

    def test_with_compression(self) -> None:
        cfg = SerializationConfig(
            serializer="json",
            compression=CompressionConfig(algorithm="gzip", level=9),
        )
        assert cfg.serializer == "json"
        assert cfg.compression is not None
        assert cfg.compression.algorithm == "gzip"
        assert cfg.compression.level == 9


# ---------------------------------------------------------------------------
# CastyConfig.serialization
# ---------------------------------------------------------------------------


class TestCastyConfigSerialization:
    def test_default_serialization(self) -> None:
        cfg = CastyConfig()
        assert cfg.serialization.serializer == "pickle"
        assert cfg.serialization.compression is None


# ---------------------------------------------------------------------------
# Serialization from TOML
# ---------------------------------------------------------------------------


class TestSerializationConfigFromToml:
    def test_no_serialization_section(self, tmp_path: Path) -> None:
        toml_file = tmp_path / "casty.toml"
        toml_file.write_text("")
        cfg = load_config(toml_file)
        assert cfg.serialization.serializer == "pickle"
        assert cfg.serialization.compression is None

    def test_serializer_only(self, tmp_path: Path) -> None:
        toml_file = tmp_path / "casty.toml"
        toml_file.write_text('[serialization]\nserializer = "json"\n')
        cfg = load_config(toml_file)
        assert cfg.serialization.serializer == "json"
        assert cfg.serialization.compression is None

    def test_with_compression(self, tmp_path: Path) -> None:
        toml_file = tmp_path / "casty.toml"
        toml_file.write_text(
            "[serialization]\n"
            'serializer = "pickle"\n\n'
            "[serialization.compression]\n"
            'algorithm = "gzip"\n'
            "level = 9\n"
        )
        cfg = load_config(toml_file)
        assert cfg.serialization.serializer == "pickle"
        assert cfg.serialization.compression is not None
        assert cfg.serialization.compression.algorithm == "gzip"
        assert cfg.serialization.compression.level == 9

    def test_compression_defaults(self, tmp_path: Path) -> None:
        toml_file = tmp_path / "casty.toml"
        toml_file.write_text(
            '[serialization]\nserializer = "pickle"\n\n[serialization.compression]\n'
        )
        cfg = load_config(toml_file)
        assert cfg.serialization.compression is not None
        assert cfg.serialization.compression.algorithm == "zlib"
        assert cfg.serialization.compression.level == 6
