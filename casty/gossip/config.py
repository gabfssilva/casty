"""
Gossip Protocol Configuration.
"""

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class GossipConfig:
    """Configuration for gossip protocol behavior.

    Attributes:
        gossip_interval: Seconds between push rounds.
        fanout: Number of peers to push to per round.
        max_push_entries: Maximum entries per push message.
        anti_entropy_interval: Seconds between anti-entropy sync rounds.
        message_timeout: Timeout for message responses in seconds.
        max_message_size: Maximum message size in bytes.
        tombstone_ttl: Seconds to keep deleted entries.
        max_entries: Maximum state entries to store.
        reconnect_interval: Seconds between reconnection attempts.
        max_reconnect_attempts: Maximum reconnection attempts before giving up.
    """

    gossip_interval: float = 0.5
    fanout: int = 3
    max_push_entries: int = 10
    anti_entropy_interval: float = 5.0
    message_timeout: float = 2.0
    max_message_size: int = 64 * 1024
    tombstone_ttl: float = 300.0
    max_entries: int = 10000
    reconnect_interval: float = 1.0
    max_reconnect_attempts: int = 10


# Presets for common configurations
class GossipConfigPresets:
    """Preset configurations for common use cases."""

    @staticmethod
    def development() -> GossipConfig:
        """Fast gossip for local development."""
        return GossipConfig(
            gossip_interval=0.2,
            anti_entropy_interval=2.0,
            tombstone_ttl=60.0,
        )

    @staticmethod
    def production() -> GossipConfig:
        """Balanced settings for production."""
        return GossipConfig(
            gossip_interval=0.5,
            fanout=3,
            anti_entropy_interval=5.0,
            tombstone_ttl=300.0,
        )

    @staticmethod
    def high_consistency() -> GossipConfig:
        """More aggressive sync for faster convergence."""
        return GossipConfig(
            gossip_interval=0.2,
            fanout=5,
            anti_entropy_interval=2.0,
            max_push_entries=20,
        )

    @staticmethod
    def low_bandwidth() -> GossipConfig:
        """Conservative settings for limited bandwidth."""
        return GossipConfig(
            gossip_interval=2.0,
            fanout=2,
            anti_entropy_interval=30.0,
            max_push_entries=5,
            max_message_size=16 * 1024,
        )
