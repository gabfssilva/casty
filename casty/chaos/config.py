"""Chaos configuration for network simulation.

Note: This proxy operates at the TCP application layer, so it can only
simulate latency and bandwidth limiting. True packet loss would require
kernel-level tools like tc/netem because TCP ACKs happen automatically
in the kernel before our proxy sees the data.
"""

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ChaosConfig:
    """Configuration for chaos injection.

    This simulates network conditions by adding delays and bandwidth
    limits to TCP traffic passing through the ChaosProxy.

    Note: Packet loss simulation is not supported because TCP guarantees
    delivery through automatic retransmission. At the userspace level,
    we cannot intercept packets before the kernel ACKs them.

    Args:
        delay_ms: Base latency to add (milliseconds).
        jitter_ms: Random variation in latency (±jitter_ms).
        bandwidth_kbps: Bandwidth limit in KB/s (0 = unlimited).
    """

    delay_ms: float = 0
    jitter_ms: float = 0
    bandwidth_kbps: float = 0

    @classmethod
    def slow_network(cls) -> "ChaosConfig":
        """Simulates a slow network with ~100ms latency."""
        return cls(delay_ms=100, jitter_ms=20)

    @classmethod
    def satellite(cls) -> "ChaosConfig":
        """Simulates satellite internet (~600ms RTT)."""
        return cls(delay_ms=300, jitter_ms=30)

    @classmethod
    def intercontinental(cls) -> "ChaosConfig":
        """Simulates intercontinental connection (~150ms RTT)."""
        return cls(delay_ms=75, jitter_ms=10)

    @classmethod
    def lan(cls) -> "ChaosConfig":
        """Simulates lan connection (~25ms RTT)."""
        return cls(delay_ms=10, jitter_ms=5)


    @classmethod
    def mobile_3g(cls) -> "ChaosConfig":
        """Simulates 3G mobile connection (latency + bandwidth limit)."""
        return cls(
            delay_ms=100,
            jitter_ms=40,
            bandwidth_kbps=500,
        )

    @classmethod
    def edge_network(cls) -> "ChaosConfig":
        """Simulates edge/rural connection (high latency, low bandwidth)."""
        return cls(
            delay_ms=150,
            jitter_ms=50,
            bandwidth_kbps=256,
        )
