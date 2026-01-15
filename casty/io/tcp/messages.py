"""
TCP Actor Messages - Akka-style commands and events.

Commands are messages you send TO TCP actors.
Events are messages you receive FROM TCP actors.
"""

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from casty import LocalRef
    from .options import SocketOptions, TLSConfig


# ============================================================================
# COMMANDS - Messages sent TO TCP actors
# ============================================================================


# --- Server Commands ---


@dataclass(frozen=True, slots=True)
class Bind:
    """Bind server to address and start accepting connections.

    Response: Bound on success, CommandFailed on failure.
    """

    host: str
    port: int
    options: "SocketOptions | None" = None
    backlog: int = 100
    tls: "TLSConfig | None" = None


@dataclass(frozen=True, slots=True)
class Unbind:
    """Stop accepting new connections.

    Response: Unbound when complete.
    """

    pass


# --- Client Commands ---


@dataclass(frozen=True, slots=True)
class Connect:
    """Connect to remote host.

    Response: Connected on success, CommandFailed on failure.
    """

    host: str
    port: int
    options: "SocketOptions | None" = None
    tls: "TLSConfig | None" = None


# --- Connection Commands ---


@dataclass(frozen=True, slots=True)
class Register:
    """Register/change handler for this connection.

    Args:
        handler: Actor to receive events from this connection.
        keep_open_on_peer_closed: If True, don't close when peer sends FIN.
        use_resume_writing: If True, suspend writes on failure instead of CommandFailed.
    """

    handler: "LocalRef"
    keep_open_on_peer_closed: bool = False
    use_resume_writing: bool = False


@dataclass(frozen=True, slots=True)
class Write:
    """Send data over connection.

    Args:
        data: Bytes to send.
        ack: If provided, WriteAck with this token is sent when write completes.

    Response: WriteAck if ack provided, CommandFailed on failure.
    """

    data: bytes
    ack: Any = None


@dataclass(frozen=True, slots=True)
class WriteFile:
    """Send file using efficient transfer.

    Uses sendfile() when available for zero-copy transfer.

    Args:
        path: Path to file.
        position: Starting position in file (default 0).
        count: Number of bytes to send (default: entire file from position).
        ack: If provided, WriteAck with this token is sent when complete.

    Response: WriteAck if ack provided, CommandFailed on failure.
    """

    path: str
    position: int = 0
    count: int | None = None
    ack: Any = None


@dataclass(frozen=True, slots=True)
class Close:
    """Graceful close - send pending data, then close.

    Response: Closed when complete.
    """

    pass


@dataclass(frozen=True, slots=True)
class ConfirmedClose:
    """Close and wait for peer acknowledgment.

    Sends FIN, waits for peer's FIN before closing.

    Response: ConfirmedClosed when complete.
    """

    pass


@dataclass(frozen=True, slots=True)
class Abort:
    """Immediate close with TCP RST.

    Discards pending data and sends RST to peer.

    Response: Aborted when complete.
    """

    pass


@dataclass(frozen=True, slots=True)
class SuspendReading:
    """Pause reading from socket (backpressure).

    No more Received events until ResumeReading is sent.
    """

    pass


@dataclass(frozen=True, slots=True)
class ResumeReading:
    """Resume reading from socket."""

    pass


@dataclass(frozen=True, slots=True)
class ResumeWriting:
    """Resume writing after write failure.

    Only relevant when use_resume_writing=True in Register.

    Response: WritingResumed when resumed.
    """

    pass


# ============================================================================
# EVENTS - Messages received FROM TCP actors
# ============================================================================


# --- Server Events ---


@dataclass(frozen=True, slots=True)
class Bound:
    """Server successfully bound to address."""

    host: str
    port: int


@dataclass(frozen=True, slots=True)
class Unbound:
    """Server stopped accepting connections."""

    pass


@dataclass(frozen=True, slots=True)
class Accepted:
    """New connection accepted by server.

    Args:
        connection: ActorRef for the connection actor.
        remote_address: (host, port) of the remote peer.
    """

    connection: "LocalRef"
    remote_address: tuple[str, int]


# --- Client Events ---


@dataclass(frozen=True, slots=True)
class Connected:
    """Successfully connected to remote host.

    Args:
        connection: ActorRef for the connection actor.
        remote_address: (host, port) of the remote peer.
        local_address: (host, port) of the local socket.
    """

    connection: "LocalRef"
    remote_address: tuple[str, int]
    local_address: tuple[str, int]


# --- Connection Events ---


@dataclass(frozen=True, slots=True)
class Received:
    """Data received from connection."""

    data: bytes


@dataclass(frozen=True, slots=True)
class WriteAck:
    """Write operation completed.

    Args:
        token: The ack token provided in the Write command.
    """

    token: Any


@dataclass(frozen=True, slots=True)
class WritingResumed:
    """Writing resumed after suspension."""

    pass


@dataclass(frozen=True, slots=True)
class Closed:
    """Connection closed normally."""

    pass


@dataclass(frozen=True, slots=True)
class ConfirmedClosed:
    """Connection closed with peer confirmation."""

    pass


@dataclass(frozen=True, slots=True)
class Aborted:
    """Connection aborted with RST."""

    pass


@dataclass(frozen=True, slots=True)
class PeerClosed:
    """Peer closed their side of connection (half-close).

    If keep_open_on_peer_closed=True, connection stays open for writing.
    Otherwise, connection will close after this event.
    """

    pass


@dataclass(frozen=True, slots=True)
class ErrorClosed:
    """Connection closed due to error."""

    cause: Exception


@dataclass(frozen=True, slots=True)
class CommandFailed:
    """A command failed to execute.

    Args:
        command: The command that failed.
        cause: The exception that caused the failure.
    """

    command: Any
    cause: Exception


# ============================================================================
# TYPE ALIASES
# ============================================================================

type ServerCommand = Bind | Unbind
type ClientCommand = Connect
type ConnectionCommand = (
    Register
    | Write
    | WriteFile
    | Close
    | ConfirmedClose
    | Abort
    | SuspendReading
    | ResumeReading
    | ResumeWriting
)

type ServerEvent = Bound | Unbound | Accepted | CommandFailed
type ClientEvent = Connected | CommandFailed
type ConnectionEvent = (
    Received
    | WriteAck
    | WritingResumed
    | Closed
    | ConfirmedClosed
    | Aborted
    | PeerClosed
    | ErrorClosed
    | CommandFailed
)
