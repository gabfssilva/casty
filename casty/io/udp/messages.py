"""UDP transport messages and types."""

from dataclasses import dataclass
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from casty import ActorRef


# ============================================================================
# SERVER MESSAGES
# ============================================================================


@dataclass
class Bind:
    """Bind UDP server to host:port."""
    host: str
    port: int
    backlog: int = 128
    options: Optional[dict] = None


@dataclass
class Unbind:
    """Unbind UDP server."""
    pass


@dataclass
class Listening:
    """Event: Server is now listening."""
    host: str
    port: int


@dataclass
class Bound:
    """Event: Server bound successfully."""
    host: str
    port: int


# ============================================================================
# CLIENT MESSAGES
# ============================================================================


@dataclass
class Connect:
    """Connect UDP client to remote peer."""
    host: str
    port: int


@dataclass
class Disconnect:
    """Disconnect UDP client."""
    pass


@dataclass
class Connected:
    """Event: Client connected to remote peer."""
    remote_addr: tuple[str, int]


@dataclass
class Disconnected:
    """Event: Client disconnected."""
    reason: str = "manual"


# ============================================================================
# HANDLER REGISTRATION
# ============================================================================


@dataclass
class RegisterHandler:
    """Register handler to receive Received events from server."""
    handler: 'ActorRef'  # noqa: F821


# ============================================================================
# SEND/RECEIVE MESSAGES
# ============================================================================


@dataclass
class Send:
    """Send data to connected peer (for client) or buffered send."""
    data: bytes


@dataclass
class SendTo:
    """Send data to specific address (for server or unconnected client)."""
    host: str
    port: int
    data: bytes


@dataclass
class Received:
    """Event: Data received from peer."""
    data: bytes
    from_addr: tuple[str, int]


# ============================================================================
# ERROR MESSAGES
# ============================================================================


@dataclass
class CommandFailed:
    """Event: Command failed."""
    reason: str
    cause: Optional[Exception] = None


# ============================================================================
# INTERNAL - PEER TRACKING
# ============================================================================


@dataclass
class _PeerMessage:
    """Internal: Message from specific peer to handler."""
    data: bytes
    peer_addr: tuple[str, int]
    handler: 'ActorRef'  # noqa: F821
