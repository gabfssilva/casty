from __future__ import annotations

import re
import tomllib
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Literal

from casty.cluster import ClusterConfig
from casty.replication import ReplicationConfig


__all__ = [
    "ActorConfig",
    "CastyConfig",
    "FailureDetectorConfig",
    "GossipConfig",
    "HeartbeatConfig",
    "MailboxConfig",
    "MailboxStrategy",
    "ResolvedActorConfig",
    "ShardingConfig",
    "SupervisionConfig",
    "discover_config",
    "load_config",
]


type MailboxStrategy = Literal["drop_new", "drop_oldest", "backpressure"]


@dataclass(frozen=True)
class MailboxConfig:
    capacity: int = 1000
    strategy: MailboxStrategy = "drop_new"


@dataclass(frozen=True)
class SupervisionConfig:
    strategy: str = "restart"
    max_restarts: int = 3
    within_seconds: float = 60.0


@dataclass(frozen=True)
class ShardingConfig:
    num_shards: int = 256


@dataclass(frozen=True)
class FailureDetectorConfig:
    threshold: float = 8.0
    max_sample_size: int = 200
    min_std_deviation_ms: float = 100.0
    acceptable_heartbeat_pause_ms: float = 0.0
    first_heartbeat_estimate_ms: float = 1000.0


@dataclass(frozen=True)
class GossipConfig:
    interval: float = 1.0


@dataclass(frozen=True)
class HeartbeatConfig:
    interval: float = 0.5
    availability_check_interval: float = 2.0


@dataclass(frozen=True)
class ActorConfig:
    pattern: re.Pattern[str]
    mailbox_overrides: dict[str, Any]
    supervision_overrides: dict[str, Any]
    sharding_overrides: dict[str, Any]
    replication_overrides: dict[str, Any]


@dataclass(frozen=True)
class ResolvedActorConfig:
    mailbox: MailboxConfig
    supervision: SupervisionConfig
    sharding: ShardingConfig
    replication: ReplicationConfig


@dataclass(frozen=True)
class CastyConfig:
    system_name: str = "casty"
    cluster: ClusterConfig | None = None
    gossip: GossipConfig = field(default_factory=GossipConfig)
    heartbeat: HeartbeatConfig = field(default_factory=HeartbeatConfig)
    failure_detector: FailureDetectorConfig = field(
        default_factory=FailureDetectorConfig
    )
    defaults_mailbox: MailboxConfig = field(default_factory=MailboxConfig)
    defaults_supervision: SupervisionConfig = field(default_factory=SupervisionConfig)
    defaults_sharding: ShardingConfig = field(default_factory=ShardingConfig)
    defaults_replication: ReplicationConfig = field(default_factory=ReplicationConfig)
    actors: tuple[ActorConfig, ...] = ()

    def resolve_actor(self, name: str) -> ResolvedActorConfig:
        mailbox_overrides: dict[str, Any] = {}
        supervision_overrides: dict[str, Any] = {}
        sharding_overrides: dict[str, Any] = {}
        replication_overrides: dict[str, Any] = {}

        for actor in self.actors:
            if actor.pattern.fullmatch(name):
                mailbox_overrides = actor.mailbox_overrides
                supervision_overrides = actor.supervision_overrides
                sharding_overrides = actor.sharding_overrides
                replication_overrides = actor.replication_overrides
                break

        return ResolvedActorConfig(
            mailbox=MailboxConfig(
                **{**asdict(self.defaults_mailbox), **mailbox_overrides}
            ),
            supervision=SupervisionConfig(
                **{**asdict(self.defaults_supervision), **supervision_overrides}
            ),
            sharding=ShardingConfig(
                **{**asdict(self.defaults_sharding), **sharding_overrides}
            ),
            replication=ReplicationConfig(
                **{**asdict(self.defaults_replication), **replication_overrides}
            ),
        )


def parse_seed_node(raw: str) -> tuple[str, int]:
    host, port_str = raw.rsplit(":", maxsplit=1)
    return (host, int(port_str))


def discover_config(start: Path | None = None) -> Path | None:
    current = start or Path.cwd()
    current = current.resolve()
    while True:
        candidate = current / "casty.toml"
        if candidate.is_file():
            return candidate
        parent = current.parent
        if parent == current:
            return None
        current = parent


def load_config(path: Path | None = None) -> CastyConfig:
    if path is None:
        discovered = discover_config()
        if discovered is None:
            return CastyConfig()
        path = discovered

    if not path.exists():
        msg = f"Config file not found: {path}"
        raise FileNotFoundError(msg)

    with path.open("rb") as f:
        raw = tomllib.load(f)

    system_name = raw.get("system", {}).get("name", "casty")

    defaults_raw = raw.get("defaults", {})
    defaults_mailbox = MailboxConfig(**defaults_raw.get("mailbox", {}))
    defaults_supervision = SupervisionConfig(**defaults_raw.get("supervision", {}))
    defaults_sharding = ShardingConfig(**defaults_raw.get("sharding", {}))
    defaults_replication = ReplicationConfig(**defaults_raw.get("replication", {}))

    cluster: ClusterConfig | None = None
    cluster_raw = raw.get("cluster", {})
    gossip = GossipConfig(**cluster_raw.get("gossip", {}))
    heartbeat = HeartbeatConfig(**cluster_raw.get("heartbeat", {}))
    failure_detector = FailureDetectorConfig(**cluster_raw.get("failure_detector", {}))

    if "host" in cluster_raw:
        seed_nodes_raw: list[str] = cluster_raw.get("seed_nodes", [])
        seed_nodes = [parse_seed_node(s) for s in seed_nodes_raw]
        roles_raw: list[str] = cluster_raw.get("roles", [])
        cluster = ClusterConfig(
            host=cluster_raw["host"],
            port=cluster_raw["port"],
            seed_nodes=seed_nodes,
            roles=frozenset(roles_raw),
        )

    actors_raw: dict[str, Any] = raw.get("actors", {})
    actors: list[ActorConfig] = []
    for name, overrides in actors_raw.items():
        pattern = (
            re.compile(f"^{name}$")
            if re.fullmatch(r"[\w-]+", name)
            else re.compile(name)
        )
        actors.append(
            ActorConfig(
                pattern=pattern,
                mailbox_overrides=overrides.get("mailbox", {}),
                supervision_overrides=overrides.get("supervision", {}),
                sharding_overrides=overrides.get("sharding", {}),
                replication_overrides=overrides.get("replication", {}),
            )
        )

    return CastyConfig(
        system_name=system_name,
        cluster=cluster,
        gossip=gossip,
        heartbeat=heartbeat,
        failure_detector=failure_detector,
        defaults_mailbox=defaults_mailbox,
        defaults_supervision=defaults_supervision,
        defaults_sharding=defaults_sharding,
        defaults_replication=defaults_replication,
        actors=tuple(actors),
    )
