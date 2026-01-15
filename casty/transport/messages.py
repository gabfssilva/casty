"""
Transport Layer Messages.

Commands are messages sent TO TransportMux.
Events are messages sent FROM TransportMux to handlers.
"""

from dataclasses import dataclass
from enum import IntEnum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from casty import LocalRef


class ProtocolType(IntEnum):
    """Protocol discriminator byte.

    Each protocol gets a unique byte that identifies it in the wire format.
    TransportMux uses this to route incoming data to the correct handler.
    """
    SWIM = 0x01       # SWIM failure detection
    ACTOR = 0x02      # Actor messaging (send/ask)
    HANDSHAKE = 0x03  # Node handshake (internal to TransportMux)
    GOSSIP = 0x04     # Gossip state replication


# =============================================================================
# Commands (sent TO TransportMux)
# =============================================================================


@dataclass(frozen=True, slots=True)
class RegisterHandler:
    """Register a handler for a protocol type.

    The handler will receive Incoming messages for this protocol.
    Also receives PeerConnected/PeerDisconnected events.

    Args:
        protocol: Protocol type to handle.
        handler: Actor ref that will receive messages.
    """
    protocol: ProtocolType
    handler: "LocalRef"


@dataclass(frozen=True, slots=True)
class ConnectTo:
    """Request connection to a node.

    TransportMux will establish TCP connection and perform handshake.
    On success, PeerConnected event is sent to all handlers.

    Args:
        node_id: ID of the node to connect to.
        address: (host, port) of the node.
    """
    node_id: str
    address: tuple[str, int]


@dataclass(frozen=True, slots=True)
class ConnectToAddress:
    """Request connection to a node by address (for seeds).

    Use this when you only know the address but not the node_id.
    TransportMux will discover the node_id during handshake.

    Args:
        address: (host, port) of the node.
    """
    address: tuple[str, int]


@dataclass(frozen=True, slots=True)
class Disconnect:
    """Disconnect from a node.

    Args:
        node_id: ID of the node to disconnect from.
    """
    node_id: str


@dataclass(frozen=True, slots=True)
class Send:
    """Send data to a specific node.

    TransportMux will frame the data with the protocol byte and send.

    Args:
        protocol: Protocol type for framing.
        to_node: Target node ID.
        data: Raw payload bytes (will be framed by TransportMux).
    """
    protocol: ProtocolType
    to_node: str
    data: bytes


@dataclass(frozen=True, slots=True)
class Broadcast:
    """Broadcast data to all connected nodes.

    Args:
        protocol: Protocol type for framing.
        data: Raw payload bytes.
        exclude: Node IDs to exclude from broadcast.
    """
    protocol: ProtocolType
    data: bytes
    exclude: tuple[str, ...] = ()


# =============================================================================
# Events (sent FROM TransportMux)
# =============================================================================


@dataclass(frozen=True, slots=True)
class Listening:
    """TransportMux is bound and listening.

    Sent to parent actor when TCP server is ready.

    Args:
        address: (host, port) that the server is listening on.
    """
    address: tuple[str, int]


@dataclass(frozen=True, slots=True)
class PeerConnected:
    """A peer connected and completed handshake.

    Sent to all registered handlers.

    Args:
        node_id: ID of the connected peer.
        address: (host, port) of the peer.
    """
    node_id: str
    address: tuple[str, int]


@dataclass(frozen=True, slots=True)
class PeerDisconnected:
    """A peer disconnected.

    Sent to all registered handlers.

    Args:
        node_id: ID of the disconnected peer.
        reason: Reason for disconnection.
    """
    node_id: str
    reason: str


@dataclass(frozen=True, slots=True)
class Incoming:
    """Incoming protocol data from a peer.

    Sent to the handler registered for this protocol.

    Args:
        from_node: Node ID of the sender.
        from_address: (host, port) of the sender.
        data: Raw payload bytes (without framing).
    """
    from_node: str
    from_address: tuple[str, int]
    data: bytes
