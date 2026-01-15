"""
QUIC Actor Messages - Commands and events for QUIC transport.

Commands are messages you send TO QUIC actors.
Events are messages you receive FROM QUIC actors.
"""

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from casty import LocalRef
    from .options import QuicOptions, TlsCertificateConfig


# ============================================================================
# COMMANDS - Messages sent TO QUIC actors
# ============================================================================


# --- Server Commands ---


@dataclass(frozen=True, slots=True)
class Bind:
    """Bind QUIC server to address and start accepting connections.

    Response: Bound on success, CommandFailed on failure.
    """

    host: str
    port: int
    certificate: "TlsCertificateConfig"
    options: "QuicOptions | None" = None


@dataclass(frozen=True, slots=True)
class Unbind:
    """Stop accepting new connections.

    Response: Unbound when complete.
    """

    pass


# --- Client Commands ---


@dataclass(frozen=True, slots=True)
class Connect:
    """Connect to remote QUIC endpoint.

    Args:
        host: Remote host.
        port: Remote port.
        server_name: SNI hostname (defaults to host if not provided).
        options: QUIC connection options.
        ca_certs: Path to CA bundle for verification.
        early_data: 0-RTT data to send during handshake (if resuming).
        session_ticket: Session ticket for resumption.

    Response: Connected on success, CommandFailed on failure.
    """

    host: str
    port: int
    server_name: str | None = None
    options: "QuicOptions | None" = None
    ca_certs: str | None = None
    early_data: bytes | None = None
    session_ticket: bytes | None = None


@dataclass(frozen=True, slots=True)
class Reconnect:
    """Attempt connection migration to a new local address.

    Response: Migrated on success, CommandFailed on failure.
    """

    new_local_address: tuple[str, int] | None = None


# --- Connection Commands ---


@dataclass(frozen=True, slots=True)
class Register:
    """Register/change handler for this connection.

    Args:
        handler: Actor to receive events from this connection.
        auto_create_streams: If True, spawn stream actors for incoming streams.
    """

    handler: "LocalRef"
    auto_create_streams: bool = True


@dataclass(frozen=True, slots=True)
class CreateStream:
    """Create a new bidirectional or unidirectional stream.

    Args:
        bidirectional: If True, create bidirectional stream (default).
        handler: Actor to receive stream events (uses connection handler if None).

    Response: StreamCreated on success, CommandFailed on failure.
    """

    bidirectional: bool = True
    handler: "LocalRef | None" = None


@dataclass(frozen=True, slots=True)
class CloseConnection:
    """Gracefully close the QUIC connection.

    Args:
        error_code: Application error code (0 = no error).
        reason: Human-readable reason phrase.

    Response: ConnectionClosed when complete.
    """

    error_code: int = 0
    reason: str = ""


@dataclass(frozen=True, slots=True)
class SendDatagram:
    """Send unreliable datagram (RFC 9221).

    Requires datagram extension to be enabled in options.

    Args:
        data: Datagram payload.
        ack: If provided, DatagramAck with this token is sent when queued.

    Response: DatagramAck if ack provided, CommandFailed on failure.
    """

    data: bytes
    ack: Any = None


# --- Stream Commands ---


@dataclass(frozen=True, slots=True)
class StreamRegister:
    """Register/change handler for this stream."""

    handler: "LocalRef"


@dataclass(frozen=True, slots=True)
class Write:
    """Write data to the stream.

    Args:
        data: Bytes to send.
        ack: If provided, WriteAck with this token is sent when complete.
        end_stream: If True, send FIN after this data.

    Response: WriteAck if ack provided, CommandFailed on failure.
    """

    data: bytes
    ack: Any = None
    end_stream: bool = False


@dataclass(frozen=True, slots=True)
class SuspendReading:
    """Pause reading from stream (backpressure).

    No more Received events until ResumeReading is sent.
    """

    pass


@dataclass(frozen=True, slots=True)
class ResumeReading:
    """Resume reading from stream."""

    pass


@dataclass(frozen=True, slots=True)
class CloseStream:
    """Close the stream gracefully.

    Args:
        error_code: Application error code (0 = no error).

    Response: StreamClosed when complete.
    """

    error_code: int = 0


@dataclass(frozen=True, slots=True)
class ResetStream:
    """Abruptly reset the stream (RESET_STREAM frame).

    Discards pending data and signals error to peer.

    Args:
        error_code: Application error code.

    Response: StreamReset when complete.
    """

    error_code: int = 0


# ============================================================================
# EVENTS - Messages received FROM QUIC actors
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
    """New QUIC connection accepted by server.

    Args:
        connection: ActorRef for the QuicConnection actor.
        remote_address: (host, port) of the remote peer.
        server_name: SNI hostname (if provided by client).
        session_ticket: Resumption ticket (if available).
    """

    connection: "LocalRef"
    remote_address: tuple[str, int]
    server_name: str | None = None
    session_ticket: bytes | None = None


# --- Client Events ---


@dataclass(frozen=True, slots=True)
class Connected:
    """Successfully connected to remote endpoint.

    Args:
        connection: ActorRef for the QuicConnection actor.
        remote_address: (host, port) of the remote peer.
        local_address: (host, port) of the local socket.
        alpn_protocol: Negotiated ALPN protocol.
        early_data_accepted: Whether 0-RTT data was accepted.
        session_ticket: Session ticket for future resumption.
    """

    connection: "LocalRef"
    remote_address: tuple[str, int]
    local_address: tuple[str, int]
    alpn_protocol: str | None = None
    early_data_accepted: bool = False
    session_ticket: bytes | None = None


@dataclass(frozen=True, slots=True)
class Migrated:
    """Connection successfully migrated to new address."""

    new_local_address: tuple[str, int]


# --- Connection Events ---


@dataclass(frozen=True, slots=True)
class StreamCreated:
    """A new stream was created (locally or by peer).

    Args:
        stream: ActorRef for the QuicStream actor.
        stream_id: QUIC stream ID.
        is_unidirectional: True if unidirectional stream.
        initiated_locally: True if we initiated the stream.
    """

    stream: "LocalRef"
    stream_id: int
    is_unidirectional: bool = False
    initiated_locally: bool = True


@dataclass(frozen=True, slots=True)
class DatagramReceived:
    """Unreliable datagram received (RFC 9221)."""

    data: bytes


@dataclass(frozen=True, slots=True)
class DatagramAck:
    """Datagram queued for sending.

    Args:
        token: The ack token provided in the SendDatagram command.
    """

    token: Any


@dataclass(frozen=True, slots=True)
class ConnectionClosed:
    """Connection closed normally."""

    error_code: int = 0
    reason: str = ""


@dataclass(frozen=True, slots=True)
class ConnectionErrorClosed:
    """Connection closed due to error."""

    cause: Exception


@dataclass(frozen=True, slots=True)
class HandshakeCompleted:
    """TLS handshake completed.

    Args:
        alpn_protocol: Negotiated ALPN protocol.
        session_resumed: Whether session was resumed via ticket.
        session_ticket: New session ticket for future resumption.
    """

    alpn_protocol: str | None = None
    session_resumed: bool = False
    session_ticket: bytes | None = None


# --- Stream Events ---


@dataclass(frozen=True, slots=True)
class Received:
    """Data received on stream."""

    data: bytes


@dataclass(frozen=True, slots=True)
class WriteAck:
    """Write operation completed.

    Args:
        token: The ack token provided in the Write command.
    """

    token: Any


@dataclass(frozen=True, slots=True)
class StreamClosed:
    """Stream closed normally."""

    pass


@dataclass(frozen=True, slots=True)
class StreamReset:
    """Stream was reset (by us or peer)."""

    error_code: int


@dataclass(frozen=True, slots=True)
class PeerFinished:
    """Peer finished sending (received FIN).

    If keep_open=True in stream register, stream stays open for writing.
    """

    pass


# --- Error Events ---


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
# INTERNAL MESSAGES
# ============================================================================


@dataclass(frozen=True, slots=True)
class _StreamData:
    """Internal: Data received from QUIC for a stream."""

    stream_id: int
    data: bytes
    end_stream: bool = False


@dataclass(frozen=True, slots=True)
class _StreamReset:
    """Internal: Stream reset by peer."""

    stream_id: int
    error_code: int


@dataclass(frozen=True, slots=True)
class _ConnectionEvent:
    """Internal: Generic connection event from protocol."""

    event: Any


# ============================================================================
# TYPE ALIASES
# ============================================================================

type ServerCommand = Bind | Unbind
type ClientCommand = Connect | Reconnect
type ConnectionCommand = Register | CreateStream | CloseConnection | SendDatagram
type StreamCommand = (
    StreamRegister | Write | SuspendReading | ResumeReading | CloseStream | ResetStream
)

type ServerEvent = Bound | Unbound | Accepted | CommandFailed
type ClientEvent = Connected | Migrated | CommandFailed
type ConnectionEvent = (
    StreamCreated
    | DatagramReceived
    | DatagramAck
    | ConnectionClosed
    | ConnectionErrorClosed
    | HandshakeCompleted
    | CommandFailed
)
type StreamEvent = (
    Received | WriteAck | StreamClosed | StreamReset | PeerFinished | CommandFailed
)
