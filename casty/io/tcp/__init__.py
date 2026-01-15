"""
Casty TCP - Akka-style TCP actors with full backpressure and close semantics.

Usage:
    from casty.io import tcp

    # Server
    server = await system.spawn(tcp.Server)
    await server.send(tcp.Bind("0.0.0.0", 8080))

    # Client
    client = await system.spawn(tcp.Client)
    await client.send(tcp.Connect("localhost", 8080))
"""

# Actors
from .server import Server
from .client import Client
from .connection import Connection

# Options
from .options import (
    SocketOptions,
    TLSConfig,
    DEFAULT_OPTIONS,
    LOW_LATENCY_OPTIONS,
    HIGH_THROUGHPUT_OPTIONS,
    SERVER_OPTIONS,
)

# Messages - Commands
from .messages import (
    # Server commands
    Bind,
    Unbind,
    # Client commands
    Connect,
    # Connection commands
    Register,
    Write,
    WriteFile,
    Close,
    ConfirmedClose,
    Abort,
    SuspendReading,
    ResumeReading,
    ResumeWriting,
    # Server events
    Bound,
    Unbound,
    Accepted,
    # Client events
    Connected,
    # Connection events
    Received,
    WriteAck,
    WritingResumed,
    Closed,
    ConfirmedClosed,
    Aborted,
    PeerClosed,
    ErrorClosed,
    CommandFailed,
    # Type aliases
    ServerCommand,
    ClientCommand,
    ConnectionCommand,
    ServerEvent,
    ClientEvent,
    ConnectionEvent,
)

__all__ = [
    # Actors
    "Server",
    "Client",
    "Connection",
    # Options
    "SocketOptions",
    "TLSConfig",
    "DEFAULT_OPTIONS",
    "LOW_LATENCY_OPTIONS",
    "HIGH_THROUGHPUT_OPTIONS",
    "SERVER_OPTIONS",
    # Commands
    "Bind",
    "Unbind",
    "Connect",
    "Register",
    "Write",
    "WriteFile",
    "Close",
    "ConfirmedClose",
    "Abort",
    "SuspendReading",
    "ResumeReading",
    "ResumeWriting",
    # Events
    "Bound",
    "Unbound",
    "Accepted",
    "Connected",
    "Received",
    "WriteAck",
    "WritingResumed",
    "Closed",
    "ConfirmedClosed",
    "Aborted",
    "PeerClosed",
    "ErrorClosed",
    "CommandFailed",
    # Type aliases
    "ServerCommand",
    "ClientCommand",
    "ConnectionCommand",
    "ServerEvent",
    "ClientEvent",
    "ConnectionEvent",
]
