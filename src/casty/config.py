"""TOML-based configuration for Casty actor systems.

Provides ``load_config`` / ``discover_config`` for loading ``casty.toml`` and
a hierarchy of frozen dataclasses for mailbox, supervision, sharding, gossip,
heartbeat, failure-detector, and per-actor override settings.
"""

from __future__ import annotations

import re
import tomllib
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Literal

from casty.cluster.cluster import ClusterConfig
from casty.core.replication import ReplicationConfig
from casty.remote.tls import Config as TlsConfig


__all__ = [
    "ActorConfig",
    "CastyConfig",
    "CompressionAlgorithm",
    "CompressionConfig",
    "FailureDetectorConfig",
    "GossipConfig",
    "HeartbeatConfig",
    "MailboxConfig",
    "MailboxStrategy",
    "ResolvedActorConfig",
    "SerializationConfig",
    "SerializerKind",
    "ShardingConfig",
    "SupervisionConfig",
    "TlsConfig",
    "TransportConfig",
    "discover_config",
    "load_config",
]


type MailboxStrategy = Literal["drop_new", "drop_oldest", "reject"]

type CompressionAlgorithm = Literal["zlib", "gzip", "bz2", "lzma", "lz4"]
type SerializerKind = Literal["pickle", "json", "msgpack", "cloudpickle"]


@dataclass(frozen=True)
class CompressionConfig:
    """Compression settings for serialized messages.

    Parameters
    ----------
    algorithm : CompressionAlgorithm
        Compression algorithm: ``"zlib"``, ``"gzip"``, ``"bz2"``, or ``"lzma"``.
    level : int
        Compression level (0-9 for zlib/gzip/bz2, 0-9 for lzma preset).

    Examples
    --------
    >>> CompressionConfig(algorithm="gzip", level=9)
    CompressionConfig(algorithm='gzip', level=9)
    """

    algorithm: CompressionAlgorithm = "zlib"
    level: int = 6


@dataclass(frozen=True)
class SerializationConfig:
    """Serialization settings for inter-node communication.

    Parameters
    ----------
    serializer : SerializerKind
        Serializer implementation: ``"pickle"`` or ``"json"``.
    compression : CompressionConfig | None
        Compression settings. ``None`` means no compression (opt-in).

    Examples
    --------
    >>> SerializationConfig(serializer="pickle", compression=CompressionConfig())
    SerializationConfig(serializer='pickle', compression=CompressionConfig(...))
    """

    serializer: SerializerKind = "pickle"
    compression: CompressionConfig | None = None


@dataclass(frozen=True)
class TransportConfig:
    """Transport-layer tuning.

    Parameters
    ----------
    max_pending_per_path : int
        Maximum queued outbound messages per remote actor path.

    Examples
    --------
    >>> TransportConfig(max_pending_per_path=128)
    TransportConfig(max_pending_per_path=128)
    """

    max_pending_per_path: int = 64


@dataclass(frozen=True)
class MailboxConfig:
    """Default mailbox settings applied to every actor unless overridden.

    Parameters
    ----------
    capacity : int | None
        Maximum number of messages in the mailbox. ``None`` for unbounded.
    strategy : MailboxStrategy
        Overflow strategy: ``"drop_new"``, ``"drop_oldest"``, or
        ``"reject"``.

    Examples
    --------
    >>> MailboxConfig(capacity=500, strategy="drop_oldest")
    MailboxConfig(capacity=500, strategy='drop_oldest')
    """

    capacity: int | None = None
    strategy: MailboxStrategy = "drop_new"


@dataclass(frozen=True)
class SupervisionConfig:
    """Default supervision settings for child actors.

    Parameters
    ----------
    strategy : str
        One of ``"restart"``, ``"stop"``, or ``"escalate"``.
    max_restarts : int
        Maximum restarts allowed within the time window.
    within_seconds : float
        Rolling window (seconds) for restart counting.

    Examples
    --------
    >>> SupervisionConfig(strategy="stop")
    SupervisionConfig(strategy='stop', max_restarts=3, within_seconds=60.0)
    """

    strategy: str = "restart"
    max_restarts: int = 3
    within_seconds: float = 60.0


@dataclass(frozen=True)
class ShardingConfig:
    """Default sharding settings.

    Parameters
    ----------
    num_shards : int
        Number of virtual shards to distribute across the cluster.

    Examples
    --------
    >>> ShardingConfig(num_shards=512)
    ShardingConfig(num_shards=512)
    """

    num_shards: int = 256


@dataclass(frozen=True)
class FailureDetectorConfig:
    """Phi accrual failure detector tuning (Hayashibara et al.).

    Parameters
    ----------
    threshold : float
        Phi value above which a node is considered unreachable.
    max_sample_size : int
        Maximum heartbeat interval samples to retain.
    min_std_deviation_ms : float
        Floor for the standard deviation estimate (ms).
    acceptable_heartbeat_pause_ms : float
        Grace period added to the heartbeat interval estimate (ms).
    first_heartbeat_estimate_ms : float
        Initial heartbeat interval estimate before real samples arrive (ms).

    Examples
    --------
    >>> FailureDetectorConfig(threshold=12.0, max_sample_size=500)
    FailureDetectorConfig(threshold=12.0, max_sample_size=500, ...)
    """

    threshold: float = 8.0
    max_sample_size: int = 200
    min_std_deviation_ms: float = 100.0
    acceptable_heartbeat_pause_ms: float = 0.0
    first_heartbeat_estimate_ms: float = 1000.0


@dataclass(frozen=True)
class GossipConfig:
    """Gossip protocol tuning.

    Parameters
    ----------
    interval : float
        Seconds between gossip rounds.
    fanout : int
        Number of peers contacted per gossip round.

    Examples
    --------
    >>> GossipConfig(interval=0.5, fanout=5)
    GossipConfig(interval=0.5, fanout=5)
    """

    interval: float = 1.0
    fanout: int = 3


@dataclass(frozen=True)
class HeartbeatConfig:
    """Heartbeat exchange tuning.

    Parameters
    ----------
    interval : float
        Seconds between heartbeat sends to each monitored peer.
    availability_check_interval : float
        Seconds between phi-accrual availability checks.

    Examples
    --------
    >>> HeartbeatConfig(interval=1.0, availability_check_interval=5.0)
    HeartbeatConfig(interval=1.0, availability_check_interval=5.0)
    """

    interval: float = 0.5
    availability_check_interval: float = 2.0


@dataclass(frozen=True)
class ActorConfig:
    """Per-actor configuration override matched by name pattern.

    When ``CastyConfig.resolve_actor(name)`` is called, the first
    ``ActorConfig`` whose ``pattern`` matches *name* supplies overrides
    for mailbox, supervision, sharding, and replication settings.

    Parameters
    ----------
    pattern : re.Pattern[str]
        Regex matched against actor names via ``fullmatch``.
    mailbox_overrides : dict[str, Any]
        Fields to override in ``MailboxConfig``.
    supervision_overrides : dict[str, Any]
        Fields to override in ``SupervisionConfig``.
    sharding_overrides : dict[str, Any]
        Fields to override in ``ShardingConfig``.
    replication_overrides : dict[str, Any]
        Fields to override in ``ReplicationConfig``.

    Examples
    --------
    >>> ActorConfig(
    ...     pattern=re.compile("worker-.*"),
    ...     mailbox_overrides={"capacity": 5000},
    ...     supervision_overrides={},
    ...     sharding_overrides={},
    ...     replication_overrides={},
    ... )
    ActorConfig(pattern=..., ...)
    """

    pattern: re.Pattern[str]
    mailbox_overrides: dict[str, Any]
    supervision_overrides: dict[str, Any]
    sharding_overrides: dict[str, Any]
    replication_overrides: dict[str, Any]


@dataclass(frozen=True)
class ResolvedActorConfig:
    """Fully resolved configuration for a single actor.

    Produced by ``CastyConfig.resolve_actor(name)`` by merging per-actor
    overrides on top of the system defaults.

    Parameters
    ----------
    mailbox : MailboxConfig
        Resolved mailbox settings.
    supervision : SupervisionConfig
        Resolved supervision settings.
    sharding : ShardingConfig
        Resolved sharding settings.
    replication : ReplicationConfig
        Resolved replication settings.

    Examples
    --------
    >>> cfg = CastyConfig()
    >>> resolved = cfg.resolve_actor("my-actor")
    >>> resolved.mailbox.capacity
    1000
    """

    mailbox: MailboxConfig
    supervision: SupervisionConfig
    sharding: ShardingConfig
    replication: ReplicationConfig


@dataclass(frozen=True)
class CastyConfig:
    """Top-level configuration container for a Casty actor system.

    Holds system-wide defaults, cluster settings, and per-actor overrides.
    Typically created via ``load_config()`` but can be constructed manually.

    Parameters
    ----------
    system_name : str
        Logical name of the actor system.
    cluster : ClusterConfig | None
        Cluster settings (``None`` for local-only systems).
    transport : TransportConfig
        Transport-layer tuning.
    serialization : SerializationConfig
        Serialization settings for inter-node communication.
    gossip : GossipConfig
        Gossip protocol tuning.
    heartbeat : HeartbeatConfig
        Heartbeat exchange tuning.
    failure_detector : FailureDetectorConfig
        Phi accrual failure detector tuning.
    defaults_mailbox : MailboxConfig
        System-wide default mailbox settings.
    defaults_supervision : SupervisionConfig
        System-wide default supervision settings.
    defaults_sharding : ShardingConfig
        System-wide default sharding settings.
    defaults_replication : ReplicationConfig
        System-wide default replication settings.
    actors : tuple[ActorConfig, ...]
        Per-actor overrides matched by name pattern.

    Examples
    --------
    >>> config = CastyConfig(system_name="my-app")
    >>> config.system_name
    'my-app'

    >>> config = load_config(Path("casty.toml"))
    """

    system_name: str = "casty"
    cluster: ClusterConfig | None = None
    tls: TlsConfig | None = None
    transport: TransportConfig = field(default_factory=TransportConfig)
    serialization: SerializationConfig = field(default_factory=SerializationConfig)
    gossip: GossipConfig = field(default_factory=GossipConfig)
    heartbeat: HeartbeatConfig = field(default_factory=HeartbeatConfig)
    failure_detector: FailureDetectorConfig = field(
        default_factory=FailureDetectorConfig
    )
    defaults_mailbox: MailboxConfig = field(default_factory=MailboxConfig)
    defaults_supervision: SupervisionConfig = field(default_factory=SupervisionConfig)
    defaults_sharding: ShardingConfig = field(default_factory=ShardingConfig)
    defaults_replication: ReplicationConfig = field(default_factory=ReplicationConfig)
    suppress_dead_letters_on_shutdown: bool = False
    actors: tuple[ActorConfig, ...] = ()

    def resolve_actor(self, name: str) -> ResolvedActorConfig:
        """Resolve the effective configuration for an actor by name.

        Merges the first matching ``ActorConfig`` override (if any) on top
        of the system defaults.

        Parameters
        ----------
        name : str
            Actor name to resolve.

        Returns
        -------
        ResolvedActorConfig

        Examples
        --------
        >>> cfg = CastyConfig()
        >>> resolved = cfg.resolve_actor("my-actor")
        >>> resolved.supervision.strategy
        'restart'
        """
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
    """Walk up from *start* (default: cwd) looking for ``casty.toml``.

    Parameters
    ----------
    start : Path | None
        Directory to start searching from.

    Returns
    -------
    Path | None
        Path to the discovered config file, or ``None`` if not found.

    Examples
    --------
    >>> discover_config(Path("/my/project"))
    PosixPath('/my/project/casty.toml')
    """
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
    """Load a ``CastyConfig`` from a TOML file.

    If *path* is ``None``, auto-discovers ``casty.toml`` by walking up from
    the current working directory.  Returns default config if no file is found.

    Parameters
    ----------
    path : Path | None
        Explicit path to a TOML config file.

    Returns
    -------
    CastyConfig

    Raises
    ------
    FileNotFoundError
        If an explicit *path* is given but does not exist.

    Examples
    --------
    >>> config = load_config()
    >>> config = load_config(Path("casty.toml"))
    >>> config.system_name
    'casty'
    """
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

    system_raw = raw.get("system", {})
    system_name = system_raw.get("name", "casty")
    suppress_dead_letters_on_shutdown = system_raw.get(
        "suppress_dead_letters_on_shutdown", False
    )

    transport = TransportConfig(**raw.get("transport", {}))

    serialization_raw = raw.get("serialization", {})
    compression_raw = serialization_raw.get("compression")
    compression = (
        CompressionConfig(**compression_raw) if compression_raw is not None else None
    )
    serialization = SerializationConfig(
        serializer=serialization_raw.get("serializer", "pickle"),
        compression=compression,
    )

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

    tls_raw: dict[str, Any] = cluster_raw.get("tls", {})
    tls: TlsConfig | None = (
        TlsConfig.from_paths(
            certfile=tls_raw["certfile"],
            cafile=tls_raw["cafile"],
            keyfile=tls_raw.get("keyfile"),
        )
        if tls_raw
        else None
    )

    if "host" in cluster_raw:
        seed_nodes_raw: list[str] = cluster_raw.get("seed_nodes", [])
        seed_nodes = tuple(parse_seed_node(s) for s in seed_nodes_raw)
        roles_raw: list[str] = cluster_raw.get("roles", [])
        node_id: str = cluster_raw.get(
            "node_id", f"{cluster_raw['host']}:{cluster_raw['port']}"
        )
        cluster = ClusterConfig(
            host=cluster_raw["host"],
            port=cluster_raw["port"],
            seed_nodes=seed_nodes,
            node_id=node_id,
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
        suppress_dead_letters_on_shutdown=suppress_dead_letters_on_shutdown,
        cluster=cluster,
        tls=tls,
        transport=transport,
        serialization=serialization,
        gossip=gossip,
        heartbeat=heartbeat,
        failure_detector=failure_detector,
        defaults_mailbox=defaults_mailbox,
        defaults_supervision=defaults_supervision,
        defaults_sharding=defaults_sharding,
        defaults_replication=defaults_replication,
        actors=tuple(actors),
    )
