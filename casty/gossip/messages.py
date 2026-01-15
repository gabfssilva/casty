"""
Gossip Protocol Messages - Commands, Events, and Wire Protocol.

Commands are messages you send TO gossip actors.
Events are messages you receive FROM gossip actors.
Wire messages are used for network communication between nodes.
"""

from dataclasses import dataclass, field
from enum import Enum, auto
from typing import TYPE_CHECKING, Any

from .state import StateDigest, StateEntry
from .versioning import VectorClock

if TYPE_CHECKING:
    from casty import LocalRef


# ============================================================================
# COMMANDS - Messages sent TO gossip actors
# ============================================================================


@dataclass(frozen=True, slots=True)
class Publish:
    """Command to publish state to the cluster.

    Args:
        key: Hierarchical key string.
        value: Data to publish (will be stored as-is).
        ttl: Optional time-to-live in seconds.
    """

    key: str
    value: bytes
    ttl: float | None = None


@dataclass(frozen=True, slots=True)
class Delete:
    """Command to delete a key from the cluster.

    Creates a tombstone that propagates through gossip.

    Args:
        key: Key to delete.
    """

    key: str


@dataclass(frozen=True, slots=True)
class Subscribe:
    """Command to subscribe to key pattern changes.

    Args:
        pattern: Glob-style pattern (supports * and **).
        subscriber: ActorRef to receive StateChanged events.
    """

    pattern: str
    subscriber: "LocalRef"


@dataclass(frozen=True, slots=True)
class Unsubscribe:
    """Command to unsubscribe from key pattern.

    Args:
        pattern: Pattern to unsubscribe from.
        subscriber: ActorRef that was subscribed.
    """

    pattern: str
    subscriber: "LocalRef"


@dataclass(frozen=True, slots=True)
class GetState:
    """Query current state (ask pattern).

    Args:
        key: Specific key to get, or None for all state.

    Returns:
        StateEntry | None if key specified, dict[str, StateEntry] if key is None.
    """

    key: str | None = None


@dataclass(frozen=True, slots=True)
class GetPeers:
    """Query current peer list (ask pattern).

    Returns:
        list[str] of connected peer node IDs.
    """

    pass


# ============================================================================
# EVENTS - Messages received FROM gossip actors
# ============================================================================


@dataclass(frozen=True, slots=True)
class StateChanged:
    """Event emitted to subscribers when state changes.

    Args:
        key: Key that changed.
        value: New value, or None if deleted.
        version: Vector clock of the change.
        source: "local" or node_id of remote origin.
    """

    key: str
    value: bytes | None
    version: VectorClock
    source: str


@dataclass(frozen=True, slots=True)
class PeerJoined:
    """Event when a new peer joins the cluster.

    Args:
        node_id: ID of the peer that joined.
        address: (host, port) of the peer.
    """

    node_id: str
    address: tuple[str, int]


@dataclass(frozen=True, slots=True)
class PeerLeft:
    """Event when a peer leaves the cluster.

    Args:
        node_id: ID of the peer that left.
        reason: Why the peer left ("disconnect", "timeout", "shutdown").
    """

    node_id: str
    reason: str = "disconnect"


@dataclass(frozen=True, slots=True)
class GossipStarted:
    """Event when gossip manager has started and is ready."""

    node_id: str
    address: tuple[str, int]


@dataclass(frozen=True, slots=True)
class GossipStopped:
    """Event when gossip manager is shutting down."""

    node_id: str


# ============================================================================
# INTERNAL ACTOR MESSAGES
# ============================================================================


@dataclass(frozen=True, slots=True)
class LocalPut:
    """Internal: Put a value locally."""

    key: str
    value: bytes
    ttl: float | None = None


@dataclass(frozen=True, slots=True)
class LocalDelete:
    """Internal: Delete a key locally."""

    key: str


@dataclass(frozen=True, slots=True)
class ApplyRemoteUpdates:
    """Internal: Apply updates received from remote peer."""

    entries: tuple[StateEntry, ...]
    source: str


@dataclass(frozen=True, slots=True)
class GetDigest:
    """Internal: Get current state digest."""

    pass


@dataclass(frozen=True, slots=True)
class GetEntries:
    """Internal: Get specific entries by keys."""

    keys: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class GetPendingUpdates:
    """Internal: Get pending updates for push."""

    max_count: int


@dataclass(frozen=True, slots=True)
class ComputeDiff:
    """Internal: Compute difference with remote digest."""

    remote_digest: StateDigest


@dataclass(frozen=True, slots=True)
class AddPeer:
    """Internal: Add a peer to connect to."""

    address: tuple[str, int]
    is_seed: bool = False
    node_id: str | None = None


@dataclass(frozen=True, slots=True)
class RemovePeer:
    """Internal: Remove a peer."""

    node_id: str


@dataclass(frozen=True, slots=True)
class GossipRound:
    """Internal: Trigger a gossip push round."""

    pass


@dataclass(frozen=True, slots=True)
class AntiEntropyRound:
    """Internal: Trigger an anti-entropy sync round."""

    pass


@dataclass(frozen=True, slots=True)
class PeerConnected:
    """Internal: Peer connection established."""

    node_id: str
    address: tuple[str, int]


@dataclass(frozen=True, slots=True)
class PeerDisconnected:
    """Internal: Peer connection lost."""

    node_id: str
    reason: str = "disconnect"


@dataclass(frozen=True, slots=True)
class SendPush:
    """Internal: Send push to peer."""

    updates: tuple[StateEntry, ...]


@dataclass(frozen=True, slots=True)
class StartSync:
    """Internal: Start anti-entropy sync with peer."""

    digest: StateDigest


@dataclass(frozen=True, slots=True)
class Reconnect:
    """Internal: Attempt to reconnect to peer."""

    pass


@dataclass(frozen=True, slots=True)
class IncomingUpdates:
    """Internal: Updates received from peer."""

    updates: tuple[StateEntry, ...]
    source: str


@dataclass(frozen=True, slots=True)
class BroadcastUpdates:
    """Internal: Broadcast updates to peers."""

    updates: tuple[StateEntry, ...]


@dataclass(frozen=True, slots=True)
class IncomingGossipData:
    """Internal: Raw gossip data received from cluster transport.

    Used when GossipManager runs in embedded mode (sharing TCP port with Cluster).
    """

    from_node: str
    data: bytes


@dataclass(frozen=True, slots=True)
class OutgoingGossipData:
    """Internal: Gossip data to send via cluster transport.

    Used when GossipManager runs in embedded mode (sharing TCP port with Cluster).
    """

    to_node: str
    data: bytes


# ============================================================================
# WIRE PROTOCOL MESSAGES
# ============================================================================


class WireMessageType(Enum):
    """Wire message types for framing."""

    HANDSHAKE = 0x01
    HANDSHAKE_ACK = 0x02
    PUSH = 0x10
    PUSH_ACK = 0x11
    SYNC_REQUEST = 0x20
    SYNC_RESPONSE = 0x21
    SYNC_DATA = 0x22
    PING = 0x30
    PONG = 0x31


@dataclass(frozen=True, slots=True)
class Handshake:
    """Initial handshake when connecting to peer.

    Args:
        node_id: Our node ID.
        address: Our (host, port) for others to connect.
    """

    node_id: str
    address: tuple[str, int]


@dataclass(frozen=True, slots=True)
class HandshakeAck:
    """Handshake acknowledgment.

    Args:
        node_id: Peer's node ID.
        address: Peer's (host, port).
        known_peers: List of known peer addresses.
    """

    node_id: str
    address: tuple[str, int]
    known_peers: tuple[tuple[str, int], ...] = ()


@dataclass(frozen=True, slots=True)
class GossipPush:
    """Push state updates to peer.

    Args:
        sender_id: Node ID of sender.
        sender_address: (host, port) of sender.
        updates: State entries to push.
    """

    sender_id: str
    sender_address: tuple[str, int]
    updates: tuple[StateEntry, ...]


@dataclass(frozen=True, slots=True)
class GossipPushAck:
    """Acknowledge receipt of push.

    Args:
        sender_id: Node ID of acknowledging node.
        received_count: Number of entries received.
        digest: Optional digest for anti-entropy hint.
    """

    sender_id: str
    received_count: int
    digest: StateDigest | None = None


@dataclass(frozen=True, slots=True)
class SyncRequest:
    """Request state synchronization.

    Args:
        sender_id: Node ID of requester.
        sender_address: (host, port) of requester.
        digest: Our current state digest.
    """

    sender_id: str
    sender_address: tuple[str, int]
    digest: StateDigest


@dataclass(frozen=True, slots=True)
class SyncResponse:
    """Response to sync request.

    Args:
        sender_id: Node ID of responder.
        entries_for_you: Entries the requester needs.
        keys_i_need: Keys we need from requester.
    """

    sender_id: str
    entries_for_you: tuple[StateEntry, ...]
    keys_i_need: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class SyncData:
    """Send requested state entries.

    Args:
        sender_id: Node ID of sender.
        entries: Requested entries.
    """

    sender_id: str
    entries: tuple[StateEntry, ...]


@dataclass(frozen=True, slots=True)
class Ping:
    """Ping for liveness check.

    Args:
        sender_id: Node ID of sender.
        sequence: Sequence number for matching response.
    """

    sender_id: str
    sequence: int


@dataclass(frozen=True, slots=True)
class Pong:
    """Pong response to ping.

    Args:
        sender_id: Node ID of responder.
        sequence: Sequence number from ping.
    """

    sender_id: str
    sequence: int


# ============================================================================
# TYPE ALIASES
# ============================================================================

type GossipCommand = Publish | Delete | Subscribe | Unsubscribe | GetState | GetPeers

type GossipEvent = (
    StateChanged | PeerJoined | PeerLeft | GossipStarted | GossipStopped
)

type WireMessage = (
    Handshake
    | HandshakeAck
    | GossipPush
    | GossipPushAck
    | SyncRequest
    | SyncResponse
    | SyncData
    | Ping
    | Pong
)
