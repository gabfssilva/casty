"""SWIM+ protocol messages.

This module defines all messages used in the SWIM+ protocol:
- Internal messages (actor-to-actor communication)
- Wire protocol messages (TCP communication between nodes)
- Gossip entries (piggybacked on protocol messages)
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from casty import LocalRef
    from .state import MemberState


# =============================================================================
# Enums
# =============================================================================


class GossipType(Enum):
    """Type of gossip entry for membership updates."""

    ALIVE = "alive"
    SUSPECT = "suspect"
    DEAD = "dead"
    JOIN = "join"
    LEAVE = "leave"


# =============================================================================
# Gossip Entries (Piggyback)
# =============================================================================


@dataclass(frozen=True)
class GossipEntry:
    """Entry for piggybacking membership updates on protocol messages."""

    type: GossipType
    node_id: str
    incarnation: int
    address: tuple[str, int] | None
    timestamp: float
    local_seq: int  # For deduplication


# =============================================================================
# Wire Protocol Messages (TCP)
# =============================================================================


@dataclass(frozen=True)
class Ping:
    """Direct probe message."""

    source: str
    source_incarnation: int
    sequence: int
    piggyback: tuple[GossipEntry, ...]


@dataclass(frozen=True)
class Ack:
    """Response to Ping."""

    source: str
    source_incarnation: int
    sequence: int
    piggyback: tuple[GossipEntry, ...]


@dataclass(frozen=True)
class PingReq:
    """Indirect probe request."""

    source: str
    target: str
    sequence: int


@dataclass(frozen=True)
class IndirectAck:
    """Ack forwarded through intermediary."""

    original_source: str
    target: str
    sequence: int
    target_incarnation: int


@dataclass(frozen=True)
class Nack:
    """Buddy notification (Lifeguard).

    Sent directly to suspected node to give it a chance to refute.
    """

    suspected: str
    reporters: tuple[str, ...]
    first_suspected_at: float


# =============================================================================
# Internal Messages (Actor-to-Actor)
# =============================================================================


# --- Coordinator Messages ---


@dataclass(frozen=True)
class Tick:
    """Timer tick for next protocol round."""

    pass


@dataclass(frozen=True)
class ProbeResult:
    """Result of a probe (direct or indirect)."""

    target: str
    success: bool
    incarnation: int | None
    latency_ms: float


@dataclass(frozen=True)
class StartProbe:
    """Command to start probing a target."""

    target: str
    target_address: tuple[str, int]
    sequence: int


# --- Suspicion Messages (Lifeguard) ---


@dataclass(frozen=True)
class StartSuspicion:
    """Start suspicion timer for a member."""

    node_id: str
    reporter: str


@dataclass(frozen=True)
class ConfirmSuspicion:
    """Independent confirmation of suspicion."""

    node_id: str
    reporter: str


@dataclass(frozen=True)
class SuspicionTimeout:
    """Suspicion timeout expired - declare dead."""

    node_id: str


@dataclass(frozen=True)
class Refute:
    """Member refuted its own suspicion."""

    node_id: str
    incarnation: int


@dataclass(frozen=True)
class CancelSuspicion:
    """Cancel ongoing suspicion for a member."""

    node_id: str


# --- Adaptive Timer Messages ---


@dataclass(frozen=True)
class RecordProbeResult:
    """Record probe result for EWMA calculation."""

    success: bool
    latency_ms: float


@dataclass(frozen=True)
class PeriodAdjusted:
    """New protocol period calculated."""

    new_period: float


@dataclass(frozen=True)
class GetCurrentPeriod:
    """Query current protocol period."""

    pass


# --- Member Messages ---


@dataclass(frozen=True)
class UpdateMemberState:
    """Update member state from gossip."""

    node_id: str
    state: str  # "alive", "suspected", "dead"
    incarnation: int
    address: tuple[str, int] | None
    timestamp: float


@dataclass(frozen=True)
class GetMemberState:
    """Query member state."""

    node_id: str


@dataclass(frozen=True)
class GetAllMembers:
    """Query all members."""

    pass


# --- Membership Events (External) ---


@dataclass(frozen=True)
class MemberJoined:
    """A new member joined the cluster."""

    node_id: str
    address: tuple[str, int]


@dataclass(frozen=True)
class MemberLeft:
    """A member left the cluster (graceful)."""

    node_id: str


@dataclass(frozen=True)
class MemberSuspected:
    """A member is suspected of failure."""

    node_id: str


@dataclass(frozen=True)
class MemberFailed:
    """A member has been declared dead."""

    node_id: str


@dataclass(frozen=True)
class MemberAlive:
    """A suspected member proved alive."""

    node_id: str


@dataclass(frozen=True)
class MemberStateChanged:
    """Internal: Member state transition event."""

    node_id: str
    old_state: "MemberState"
    new_state: "MemberState"
    incarnation: int


# --- Public API Messages ---


@dataclass(frozen=True)
class Subscribe:
    """Subscribe to membership events."""

    subscriber: LocalRef


@dataclass(frozen=True)
class Unsubscribe:
    """Unsubscribe from membership events."""

    subscriber: LocalRef


@dataclass(frozen=True)
class GetMembers:
    """Query current membership list."""

    pass


@dataclass(frozen=True)
class Leave:
    """Gracefully leave the cluster."""

    pass


@dataclass(frozen=True)
class Join:
    """Join the cluster (connect to seeds)."""

    pass


# --- Connection Messages ---


@dataclass(frozen=True)
class PeerConnected:
    """A peer connection was established."""

    node_id: str
    address: tuple[str, int]


@dataclass(frozen=True)
class PeerDisconnected:
    """A peer connection was lost."""

    node_id: str


@dataclass(frozen=True)
class IncomingMessage:
    """Message received from network."""

    from_node: str
    from_address: tuple[str, int]
    message: Ping | Ack | PingReq | IndirectAck | Nack


# Type alias for all wire protocol messages
WireMessage = Ping | Ack | PingReq | IndirectAck | Nack

# Type alias for all membership events
MembershipEvent = MemberJoined | MemberLeft | MemberSuspected | MemberFailed | MemberAlive
