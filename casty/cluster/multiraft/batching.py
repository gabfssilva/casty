"""Message batching for Multi-Raft optimization."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal


@dataclass(frozen=True, slots=True)
class BatchedMessage:
    """A single Raft message within a batch."""

    group_id: int
    msg_type: Literal[
        "vote_request",
        "vote_response",
        "append_entries_req",
        "append_entries_res",
        "install_snapshot_req",
        "install_snapshot_res",
        "heartbeat",  # Legacy, converted to append_entries_req
    ]
    data: dict[str, Any]


@dataclass
class MessageBatcher:
    """Batches Raft messages for multiple groups into a single network message.

    This reduces network overhead from O(nodes^2 * groups) to O(nodes^2).
    """

    _pending_heartbeats: dict[str, list[BatchedMessage]] = field(default_factory=dict)
    _pending_append_entries: dict[str, list[BatchedMessage]] = field(default_factory=dict)

    def add_heartbeat(self, peer_id: str, group_id: int, data: dict[str, Any]) -> None:
        """Add a heartbeat for batching."""
        if peer_id not in self._pending_heartbeats:
            self._pending_heartbeats[peer_id] = []
        self._pending_heartbeats[peer_id].append(
            BatchedMessage(
                group_id=group_id,
                msg_type="append_entries_req",
                data=data,
            )
        )

    def add_append_entries(self, peer_id: str, group_id: int, data: dict[str, Any]) -> None:
        """Add an AppendEntries for batching."""
        if peer_id not in self._pending_append_entries:
            self._pending_append_entries[peer_id] = []
        self._pending_append_entries[peer_id].append(
            BatchedMessage(
                group_id=group_id,
                msg_type="append_entries_req",
                data=data,
            )
        )

    def get_batched_heartbeats(self, peer_id: str) -> list[BatchedMessage]:
        """Get and clear pending heartbeats for a peer."""
        return self._pending_heartbeats.pop(peer_id, [])

    def get_batched_append_entries(self, peer_id: str) -> list[BatchedMessage]:
        """Get and clear pending append entries for a peer."""
        return self._pending_append_entries.pop(peer_id, [])

    def get_all_heartbeats(self) -> dict[str, list[BatchedMessage]]:
        """Get and clear all pending heartbeats."""
        result = dict(self._pending_heartbeats)
        self._pending_heartbeats.clear()
        return result

    def get_all_append_entries(self) -> dict[str, list[BatchedMessage]]:
        """Get and clear all pending append entries."""
        result = dict(self._pending_append_entries)
        self._pending_append_entries.clear()
        return result

    def clear(self) -> None:
        """Clear all pending messages."""
        self._pending_heartbeats.clear()
        self._pending_append_entries.clear()

    @staticmethod
    def serialize_batch(messages: list[BatchedMessage]) -> list[dict[str, Any]]:
        """Serialize a batch of messages for network transmission."""
        return [
            {
                "group_id": msg.group_id,
                "msg_type": msg.msg_type,
                "data": msg.data,
            }
            for msg in messages
        ]

    @staticmethod
    def deserialize_batch(data: list[dict[str, Any]]) -> list[BatchedMessage]:
        """Deserialize a batch of messages from network."""
        return [
            BatchedMessage(
                group_id=item["group_id"],
                msg_type=item["msg_type"],
                data=item["data"],
            )
            for item in data
        ]
