"""Cluster configuration.

This module defines the configuration for the Cluster actor,
including network settings, SWIM configuration, and timeouts.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace

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

    # Advertised address (for NAT/proxy scenarios)
    # If set, this address is announced to other nodes instead of bind address
    advertise_host: str | None = None
    advertise_port: int | None = None

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
        return replace(
            self,
            bind_host=host,
            bind_port=port,
            swim_config=self.swim_config.with_bind_address(host, port),
        )

    def with_seeds(self, seeds: list[str]) -> ClusterConfig:
        """Return a copy with updated seeds.

        Args:
            seeds: List of seed nodes in "host:port" format.
        """
        swim_seeds = [(h, int(p)) for h, p in (s.rsplit(":", 1) for s in seeds)]
        return replace(
            self,
            seeds=seeds.copy(),
            swim_config=self.swim_config.with_seeds(swim_seeds),
        )

    def with_node_id(self, node_id: str) -> ClusterConfig:
        """Return a copy with updated node ID."""
        return replace(self, node_id=node_id)

    def with_advertise_address(self, host: str, port: int) -> ClusterConfig:
        """Return a copy with updated advertise address.

        The advertise address is announced to other nodes instead of the
        bind address. Useful for NAT traversal or proxy scenarios.
        """
        return replace(self, advertise_host=host, advertise_port=port)
