"""
Transport Layer - Protocol multiplexing over TCP.

This module provides a protocol-agnostic transport layer that multiplexes
multiple protocols over a single TCP port.

Example:
    from casty.transport import TransportMux, ProtocolType, RegisterHandler, Send

    mux = await ctx.spawn(
        TransportMux,
        node_id="node-1",
        bind_address=("0.0.0.0", 7946),
    )

    # Register protocol handlers
    await mux.send(RegisterHandler(ProtocolType.SWIM, swim_ref))
    await mux.send(RegisterHandler(ProtocolType.ACTOR, cluster_ref))

    # Send data
    await mux.send(Send(ProtocolType.SWIM, "node-2", data))
"""

from .messages import (
    # Protocol type enum
    ProtocolType,
    # Commands
    RegisterHandler,
    ConnectTo,
    ConnectToAddress,
    Disconnect,
    Send,
    Broadcast,
    # Events
    Listening,
    PeerConnected,
    PeerDisconnected,
    Incoming,
)

from .mux import TransportMux

__all__ = [
    # Protocol type
    "ProtocolType",
    # Commands
    "RegisterHandler",
    "ConnectTo",
    "ConnectToAddress",
    "Disconnect",
    "Send",
    "Broadcast",
    # Events
    "Listening",
    "PeerConnected",
    "PeerDisconnected",
    "Incoming",
    # Actor
    "TransportMux",
]
