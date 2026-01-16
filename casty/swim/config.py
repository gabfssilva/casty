"""SWIM+ protocol configuration.

This module defines the configuration for the SWIM+ protocol,
including timing, fanout, Lifeguard suspicion, and network settings.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace


@dataclass
class SwimConfig:
    """Configuration for SWIM+ protocol.

    Attributes:
        protocol_period: Base interval between failure detection rounds (seconds).
        ping_timeout: Timeout for direct probe ACK (seconds).
        ping_req_timeout: Timeout for indirect probe ACK (seconds).

        min_protocol_period: Minimum protocol period for adaptation (seconds).
        max_protocol_period: Maximum protocol period for adaptation (seconds).
        adaptation_alpha: EWMA smoothing factor for period adaptation.

        ping_req_fanout: Number of intermediary nodes for indirect probes (K).

        suspicion_alpha: Alpha parameter for min suspicion timeout.
        suspicion_beta: Beta multiplier for max suspicion timeout.
        confirmation_threshold: Threshold for suspicion decay (k).

        max_piggyback_entries: Maximum gossip entries per message.
        gossip_lambda: Lambda parameter for propagation count.

        bind_address: Local address to bind (host, port).
        seeds: List of seed node addresses for bootstrapping.
    """

    # Timing
    protocol_period: float = 1.0
    ping_timeout: float = 0.5
    ping_req_timeout: float = 1.0

    # Period Adaptation
    min_protocol_period: float = 0.1
    max_protocol_period: float = 10.0
    adaptation_alpha: float = 0.2

    # Fanout
    ping_req_fanout: int = 3

    # Lifeguard Suspicion
    suspicion_alpha: float = 5.0
    suspicion_beta: float = 6.0
    confirmation_threshold: int = 5

    # Gossip (λ log n)
    max_piggyback_entries: int = 10
    gossip_lambda: int = 5

    # Network
    bind_address: tuple[str, int] = ("0.0.0.0", 7946)
    seeds: list[tuple[str, int]] = field(default_factory=list)

    @staticmethod
    def development() -> SwimConfig:
        """Fast settings for testing and development.

        Uses short timeouts for quick iteration.
        """
        return SwimConfig(
            protocol_period=0.1,
            ping_timeout=0.05,
            ping_req_timeout=0.1,
            min_protocol_period=0.05,
            max_protocol_period=1.0,
            suspicion_alpha=2.0,
            suspicion_beta=3.0,
            confirmation_threshold=3,
            bind_address=("127.0.0.1", 0),
            seeds=[],
        )

    @staticmethod
    def production() -> SwimConfig:
        """Balanced settings for production use.

        Uses conservative timeouts suitable for most deployments.
        """
        return SwimConfig(
            protocol_period=1.0,
            ping_timeout=0.5,
            ping_req_timeout=1.0,
            min_protocol_period=0.1,
            max_protocol_period=10.0,
            suspicion_alpha=5.0,
            suspicion_beta=6.0,
            confirmation_threshold=5,
            bind_address=("0.0.0.0", 7946),
            seeds=[],
        )

    @staticmethod
    def high_consistency() -> SwimConfig:
        """Settings optimized for fast failure detection.

        More aggressive probing at the cost of higher network overhead.
        """
        return SwimConfig(
            protocol_period=0.5,
            ping_timeout=0.3,
            ping_req_timeout=0.5,
            min_protocol_period=0.1,
            max_protocol_period=5.0,
            ping_req_fanout=5,
            suspicion_alpha=3.0,
            suspicion_beta=4.0,
            confirmation_threshold=3,
            bind_address=("0.0.0.0", 7946),
            seeds=[],
        )

    @staticmethod
    def low_bandwidth() -> SwimConfig:
        """Settings optimized for low network overhead.

        Slower detection but minimal network traffic.
        """
        return SwimConfig(
            protocol_period=2.0,
            ping_timeout=1.0,
            ping_req_timeout=2.0,
            min_protocol_period=0.5,
            max_protocol_period=30.0,
            ping_req_fanout=2,
            suspicion_alpha=8.0,
            suspicion_beta=8.0,
            confirmation_threshold=7,
            max_piggyback_entries=5,
            gossip_lambda=3,
            bind_address=("0.0.0.0", 7946),
            seeds=[],
        )

    def with_bind_address(self, host: str, port: int) -> SwimConfig:
        """Return a copy with updated bind address."""
        return replace(self, bind_address=(host, port))

    def with_seeds(self, seeds: list[tuple[str, int]]) -> SwimConfig:
        """Return a copy with updated seeds."""
        return replace(self, seeds=seeds.copy())
