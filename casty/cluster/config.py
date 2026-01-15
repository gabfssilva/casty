"""Cluster configuration.

This module defines the configuration for the Cluster actor,
including network settings, SWIM configuration, and timeouts.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from casty.swim import SwimConfig


@dataclass
class ClusterConfig:
    """Configuration for the Cluster actor.

    Attributes:
        bind_host: Host to bind the TCP server to.
        bind_port: Port to bind the TCP server to (0 = auto-assign).

        node_id: Unique identifier for this node (auto-generated if None).

        seeds: List of seed nodes in "host:port" format for bootstrapping.

        swim_config: Configuration for the SWIM membership protocol.

        connect_timeout: Timeout for establishing connections (seconds).
        handshake_timeout: Timeout for completing handshake (seconds).
        ask_timeout: Default timeout for remote ask operations (seconds).

        max_pending_asks: Maximum number of pending ask requests.
        ask_cleanup_interval: Interval for cleaning up stale requests (seconds).
    """

    # Network
    bind_host: str = "0.0.0.0"
    bind_port: int = 0  # 0 = auto-assign

    # Identity
    node_id: str | None = None

    # Bootstrap
    seeds: list[str] = field(default_factory=list)

    # SWIM configuration
    swim_config: SwimConfig = field(default_factory=SwimConfig)

    # Timeouts
    connect_timeout: float = 5.0
    handshake_timeout: float = 5.0
    ask_timeout: float = 30.0

    # Request tracking
    max_pending_asks: int = 10000
    ask_cleanup_interval: float = 60.0

    @staticmethod
    def development() -> ClusterConfig:
        """Fast settings for testing and development.

        Uses short timeouts and local binding for quick iteration.
        """
        return ClusterConfig(
            bind_host="127.0.0.1",
            bind_port=0,
            swim_config=SwimConfig.development(),
            connect_timeout=2.0,
            handshake_timeout=2.0,
            ask_timeout=10.0,
            ask_cleanup_interval=30.0,
        )

    @staticmethod
    def production() -> ClusterConfig:
        """Balanced settings for production use.

        Uses conservative timeouts suitable for most deployments.
        """
        return ClusterConfig(
            bind_host="0.0.0.0",
            bind_port=7946,
            swim_config=SwimConfig.production(),
            connect_timeout=5.0,
            handshake_timeout=5.0,
            ask_timeout=30.0,
            ask_cleanup_interval=60.0,
        )

    def with_bind_address(self, host: str, port: int) -> ClusterConfig:
        """Return a copy with updated bind address."""
        return ClusterConfig(
            bind_host=host,
            bind_port=port,
            node_id=self.node_id,
            seeds=self.seeds.copy(),
            swim_config=self.swim_config.with_bind_address(host, port),
            connect_timeout=self.connect_timeout,
            handshake_timeout=self.handshake_timeout,
            ask_timeout=self.ask_timeout,
            max_pending_asks=self.max_pending_asks,
            ask_cleanup_interval=self.ask_cleanup_interval,
        )

    def with_seeds(self, seeds: list[str]) -> ClusterConfig:
        """Return a copy with updated seeds.

        Args:
            seeds: List of seed nodes in "host:port" format.
        """
        # Parse seeds for SWIM config
        swim_seeds = []
        for seed in seeds:
            host, port_str = seed.rsplit(":", 1)
            swim_seeds.append((host, int(port_str)))

        return ClusterConfig(
            bind_host=self.bind_host,
            bind_port=self.bind_port,
            node_id=self.node_id,
            seeds=seeds.copy(),
            swim_config=self.swim_config.with_seeds(swim_seeds),
            connect_timeout=self.connect_timeout,
            handshake_timeout=self.handshake_timeout,
            ask_timeout=self.ask_timeout,
            max_pending_asks=self.max_pending_asks,
            ask_cleanup_interval=self.ask_cleanup_interval,
        )

    def with_node_id(self, node_id: str) -> ClusterConfig:
        """Return a copy with updated node ID."""
        return ClusterConfig(
            bind_host=self.bind_host,
            bind_port=self.bind_port,
            node_id=node_id,
            seeds=self.seeds.copy(),
            swim_config=self.swim_config,
            connect_timeout=self.connect_timeout,
            handshake_timeout=self.handshake_timeout,
            ask_timeout=self.ask_timeout,
            max_pending_asks=self.max_pending_asks,
            ask_cleanup_interval=self.ask_cleanup_interval,
        )
