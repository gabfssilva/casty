"""WAL entry types for replication.

This module defines the data structures used for WAL-based replication,
where state changes are captured and replicated instead of raw messages.
"""

from __future__ import annotations

import time
import zlib
from dataclasses import dataclass
from typing import Any

import msgpack


@dataclass(frozen=True, slots=True)
class ReplicationWALEntry:
    """A WAL entry representing a state change to be replicated.

    When a replicated actor processes a message that changes state,
    a ReplicationWALEntry is created with the resulting state. This entry
    is then replicated to other nodes, which apply the state directly
    via set_state() instead of re-processing the original message.

    Attributes:
        sequence: Monotonic sequence number per entity (for ordering/gap detection)
        entity_type: The actor type name (e.g., "counters")
        entity_id: The entity identifier (e.g., "counter-1")
        state: msgpack-serialized state dict (result of get_state())
        message_type: Original message type name (for debugging/metrics)
        node_id: Node that processed the message and generated this entry
        timestamp: When the entry was created (Unix timestamp)
        checksum: CRC32 checksum of state bytes for integrity verification
    """

    sequence: int
    entity_type: str
    entity_id: str
    state: bytes
    message_type: str
    node_id: str
    timestamp: float
    checksum: int

    @classmethod
    def create(
        cls,
        sequence: int,
        entity_type: str,
        entity_id: str,
        state_dict: dict[str, Any],
        message_type: str,
        node_id: str,
    ) -> ReplicationWALEntry:
        """Create a WAL entry from a state dict.

        Args:
            sequence: Monotonic sequence number
            entity_type: Actor type name
            entity_id: Entity identifier
            state_dict: State dictionary from actor.get_state()
            message_type: Original message type name
            node_id: Node that generated this entry

        Returns:
            A new ReplicationWALEntry with serialized state and checksum
        """
        state_bytes = msgpack.packb(state_dict, use_bin_type=True)
        checksum = zlib.crc32(state_bytes) & 0xFFFFFFFF

        return cls(
            sequence=sequence,
            entity_type=entity_type,
            entity_id=entity_id,
            state=state_bytes,
            message_type=message_type,
            node_id=node_id,
            timestamp=time.time(),
            checksum=checksum,
        )

    def get_state_dict(self) -> dict[str, Any]:
        """Deserialize and return the state dict.

        Returns:
            The state dictionary that can be passed to actor.set_state()

        Raises:
            ValueError: If checksum verification fails
        """
        # Verify checksum
        computed = zlib.crc32(self.state) & 0xFFFFFFFF
        if computed != self.checksum:
            raise ValueError(
                f"Checksum mismatch for WAL entry {self.entity_type}:{self.entity_id} "
                f"seq={self.sequence}: expected {self.checksum}, got {computed}"
            )

        return msgpack.unpackb(self.state, raw=False)

    def to_bytes(self) -> bytes:
        """Serialize the entire entry for wire transmission.

        Returns:
            msgpack-serialized bytes of the full entry
        """
        return msgpack.packb(
            {
                "sequence": self.sequence,
                "entity_type": self.entity_type,
                "entity_id": self.entity_id,
                "state": self.state,
                "message_type": self.message_type,
                "node_id": self.node_id,
                "timestamp": self.timestamp,
                "checksum": self.checksum,
            },
            use_bin_type=True,
        )

    @classmethod
    def from_bytes(cls, data: bytes) -> ReplicationWALEntry:
        """Deserialize a WAL entry from wire format.

        Args:
            data: msgpack-serialized entry bytes

        Returns:
            Deserialized ReplicationWALEntry
        """
        d = msgpack.unpackb(data, raw=False)
        return cls(
            sequence=d["sequence"],
            entity_type=d["entity_type"],
            entity_id=d["entity_id"],
            state=d["state"],
            message_type=d["message_type"],
            node_id=d["node_id"],
            timestamp=d["timestamp"],
            checksum=d["checksum"],
        )


class WALGapError(Exception):
    """Raised when a replica detects a gap in WAL sequence.

    This indicates the replica missed some entries and needs to
    request a catch-up from the primary.
    """

    def __init__(self, expected: int, received: int) -> None:
        self.expected = expected
        self.received = received
        super().__init__(
            f"WAL sequence gap: expected {expected}, received {received}"
        )
