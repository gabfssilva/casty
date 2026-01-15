"""UDP transport module - simple datagram-based actor communication.

This module provides UDP server and client actors for lightweight,
connectionless messaging between actors.

Example:
    Server:
        >>> server = await system.spawn(udp.Server)
        >>> await server.send(udp.Bind("127.0.0.1", 9000))

    Client:
        >>> client = await system.spawn(udp.Client)
        >>> await client.send(udp.Connect("127.0.0.1", 9000))
        >>> await client.send(udp.Send(b"Hello UDP"))
"""

from .server import Server
from .client import Client
from .messages import (
    Bind,
    Unbind,
    Bound,
    Connect,
    Disconnect,
    Connected,
    Disconnected,
    RegisterHandler,
    Send,
    SendTo,
    Received,
    CommandFailed,
)

__all__ = [
    # Actors
    "Server",
    "Client",
    # Server messages
    "Bind",
    "Unbind",
    "Bound",
    # Client messages
    "Connect",
    "Disconnect",
    "Connected",
    "Disconnected",
    # Handler registration
    "RegisterHandler",
    # Data messages
    "Send",
    "SendTo",
    "Received",
    # Error
    "CommandFailed",
]
