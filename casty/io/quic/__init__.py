"""
Casty QUIC - Actor-based QUIC transport with stream multiplexing.

QUIC provides a secure, multiplexed transport built on UDP with features like:
- Multiple streams per connection
- 0-RTT fast reconnection
- Connection migration
- Built-in encryption (TLS 1.3)
- Unreliable datagrams (RFC 9221)

Requirements:
    pip install casty[quic]  # or: pip install aioquic>=1.0

Usage:
    from casty.io import quic

    # Server
    server = await system.spawn(quic.Server)
    cert = quic.TlsCertificateConfig(certfile="server.crt", keyfile="server.key")
    await server.send(quic.Bind("0.0.0.0", 4433, certificate=cert))

    # Client
    client = await system.spawn(quic.Client)
    await client.send(quic.Connect("localhost", 4433, server_name="example.com"))

    # Working with streams (after receiving Connected/Accepted)
    await connection.send(quic.CreateStream())  # Creates new stream
    await stream.send(quic.Write(b"Hello QUIC!"))
"""


def _check_aioquic_installed():
    """Check if aioquic is installed and provide helpful error if not."""
    try:
        import aioquic  # noqa: F401

        return True
    except ImportError:
        raise ImportError(
            "aioquic is required for QUIC support. "
            "Install it with: pip install casty[quic] or pip install aioquic>=1.0"
        ) from None


_check_aioquic_installed()

# Actors
from .server import Server
from .client import Client
from .connection import Connection
from .stream import Stream

# Options
from .options import (
    QuicOptions,
    TlsCertificateConfig,
    DEFAULT_OPTIONS,
    LOW_LATENCY_OPTIONS,
    HIGH_THROUGHPUT_OPTIONS,
    DATAGRAM_OPTIONS,
    SERVER_OPTIONS,
)

# Messages - Commands
from .messages import (
    # Server commands
    Bind,
    Unbind,
    # Client commands
    Connect,
    Reconnect,
    # Connection commands
    Register,
    CreateStream,
    CloseConnection,
    SendDatagram,
    # Stream commands
    StreamRegister,
    Write,
    SuspendReading,
    ResumeReading,
    CloseStream,
    ResetStream,
    # Server events
    Bound,
    Unbound,
    Accepted,
    # Client events
    Connected,
    Migrated,
    # Connection events
    StreamCreated,
    DatagramReceived,
    DatagramAck,
    ConnectionClosed,
    ConnectionErrorClosed,
    HandshakeCompleted,
    # Stream events
    Received,
    WriteAck,
    StreamClosed,
    StreamReset,
    PeerFinished,
    # Error events
    CommandFailed,
    # Type aliases
    ServerCommand,
    ClientCommand,
    ConnectionCommand,
    StreamCommand,
    ServerEvent,
    ClientEvent,
    ConnectionEvent,
    StreamEvent,
)

__all__ = [
    # Actors
    "Server",
    "Client",
    "Connection",
    "Stream",
    # Options
    "QuicOptions",
    "TlsCertificateConfig",
    "DEFAULT_OPTIONS",
    "LOW_LATENCY_OPTIONS",
    "HIGH_THROUGHPUT_OPTIONS",
    "DATAGRAM_OPTIONS",
    "SERVER_OPTIONS",
    # Server commands
    "Bind",
    "Unbind",
    # Client commands
    "Connect",
    "Reconnect",
    # Connection commands
    "Register",
    "CreateStream",
    "CloseConnection",
    "SendDatagram",
    # Stream commands
    "StreamRegister",
    "Write",
    "SuspendReading",
    "ResumeReading",
    "CloseStream",
    "ResetStream",
    # Server events
    "Bound",
    "Unbound",
    "Accepted",
    # Client events
    "Connected",
    "Migrated",
    # Connection events
    "StreamCreated",
    "DatagramReceived",
    "DatagramAck",
    "ConnectionClosed",
    "ConnectionErrorClosed",
    "HandshakeCompleted",
    # Stream events
    "Received",
    "WriteAck",
    "StreamClosed",
    "StreamReset",
    "PeerFinished",
    # Error events
    "CommandFailed",
    # Type aliases
    "ServerCommand",
    "ClientCommand",
    "ConnectionCommand",
    "StreamCommand",
    "ServerEvent",
    "ClientEvent",
    "ConnectionEvent",
    "StreamEvent",
]
