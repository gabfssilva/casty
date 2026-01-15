"""Chaos proxy messages."""

from dataclasses import dataclass

from .config import ChaosConfig


# =============================================================================
# COMMANDS
# =============================================================================


@dataclass(frozen=True, slots=True)
class StartProxy:
    """Start the chaos proxy.

    Args:
        listen_port: Port to listen on (0 for random).
        target_host: Upstream host to forward to.
        target_port: Upstream port to forward to.
        chaos: Chaos configuration (defaults to no chaos).
    """

    listen_port: int
    target_host: str
    target_port: int
    chaos: ChaosConfig | None = None


@dataclass(frozen=True, slots=True)
class StopProxy:
    """Stop the chaos proxy."""

    pass


@dataclass(frozen=True, slots=True)
class UpdateChaos:
    """Update chaos configuration at runtime."""

    chaos: ChaosConfig


# =============================================================================
# EVENTS
# =============================================================================


@dataclass(frozen=True, slots=True)
class ProxyStarted:
    """Proxy started successfully."""

    host: str
    port: int


@dataclass(frozen=True, slots=True)
class ProxyStopped:
    """Proxy stopped."""

    pass


@dataclass(frozen=True, slots=True)
class ProxyError:
    """Proxy encountered an error."""

    cause: Exception


@dataclass(frozen=True, slots=True)
class ConnectionProxied:
    """A new connection is being proxied."""

    remote_address: tuple[str, int]


@dataclass(frozen=True, slots=True)
class ConnectionClosed:
    """A proxied connection was closed."""

    remote_address: tuple[str, int]
    bytes_sent: int
    bytes_received: int
