"""Raft snapshot mixin for log compaction."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Callable, Coroutine

from ..raft_types import (
    InstallSnapshotRequest,
    InstallSnapshotResponse,
    Snapshot,
)
from ..serialize import deserialize, serialize
from ..transport import MessageType, Transport, WireMessage

if TYPE_CHECKING:
    from ..distributed import ClusterState

log = logging.getLogger(__name__)


class SnapshotMixin:
    """Raft snapshot for log compaction and catching up slow followers."""

    _cluster: ClusterState
    _current_snapshot: Snapshot | None
    _snapshot_callback: Callable[[], Coroutine[Any, Any, bytes]] | None
    _restore_callback: Callable[[bytes], Coroutine[Any, Any, None]] | None

    def _init_snapshot(self) -> None:
        """Initialize snapshot state."""
        self._current_snapshot = None
        self._snapshot_callback = None
        self._restore_callback = None

    def set_snapshot_callbacks(
        self,
        snapshot_cb: Callable[[], Coroutine[Any, Any, bytes]],
        restore_cb: Callable[[bytes], Coroutine[Any, Any, None]],
    ) -> None:
        """Set callbacks for creating and restoring snapshots."""
        self._snapshot_callback = snapshot_cb
        self._restore_callback = restore_cb

    async def create_snapshot(self) -> Snapshot | None:
        """Create a snapshot of current state machine state."""
        if not self._snapshot_callback:
            return None

        async with self._cluster.election_lock:
            last_applied = self._cluster.raft_log.last_applied
            if last_applied == 0:
                return None

            entry = self._cluster.raft_log.get_entry(last_applied)
            if not entry:
                return None

            last_term = entry.term

        # Get state machine snapshot outside lock
        data = await self._snapshot_callback()

        snapshot = Snapshot(
            last_included_index=last_applied,
            last_included_term=last_term,
            data=data,
        )

        # Store and persist snapshot
        self._current_snapshot = snapshot
        await self._persist_snapshot(snapshot)

        # Compact log
        async with self._cluster.election_lock:
            self._cluster.raft_log.install_snapshot(last_applied, last_term)

        await self._persist_log()

        log.info(f"Created snapshot at index {last_applied}")
        return snapshot

    async def _send_install_snapshot(
        self, peer_id: str, transport: Transport
    ) -> None:
        """Send InstallSnapshot RPC to a lagging follower."""
        if not self._current_snapshot:
            return

        async with self._cluster.election_lock:
            if self._cluster.state != "leader":
                return
            term = self._cluster.term

        request = InstallSnapshotRequest(
            term=term,
            leader_id=self._cluster.node_id,
            last_included_index=self._current_snapshot.last_included_index,
            last_included_term=self._current_snapshot.last_included_term,
            data=self._current_snapshot.data,
            done=True,  # We send complete snapshot in one chunk
        )

        request_data = {
            "term": request.term,
            "leader_id": request.leader_id,
            "last_included_index": request.last_included_index,
            "last_included_term": request.last_included_term,
            "data": request.data.hex(),
            "done": request.done,
        }

        try:
            msg = WireMessage(
                msg_type=MessageType.INSTALL_SNAPSHOT_REQ,
                target_name=self._cluster.node_id,
                payload=serialize(request_data),
            )
            await transport.send(msg)
            log.debug(f"Sent snapshot to {peer_id[:8]}")
        except Exception as e:
            log.debug(f"Failed to send snapshot to {peer_id[:8]}: {e}")

    async def _handle_install_snapshot_request(
        self, transport: Transport, msg: WireMessage
    ) -> None:
        """Handle incoming InstallSnapshot RPC from leader."""
        data = deserialize(msg.payload, {})

        term = data["term"]
        leader_id = data["leader_id"]
        last_included_index = data["last_included_index"]
        last_included_term = data["last_included_term"]
        snapshot_data = bytes.fromhex(data["data"])

        async with self._cluster.election_lock:
            # Reply immediately if term < currentTerm
            if term < self._cluster.term:
                response = InstallSnapshotResponse(
                    term=self._cluster.term,
                    success=False,
                    follower_id=self._cluster.node_id,
                )
                await self._send_install_snapshot_response(transport, response)
                return

            # Update term if needed
            if term > self._cluster.term:
                self._cluster.term = term
                self._cluster.voted_for = None

            # Recognize leader
            self._cluster.state = "follower"
            self._cluster.leader_id = leader_id
            self._reset_election_timeout()

            current_term = self._cluster.term

        # Restore state machine from snapshot (outside lock)
        if self._restore_callback:
            try:
                await self._restore_callback(snapshot_data)
            except Exception as e:
                log.error(f"Failed to restore snapshot: {e}")
                response = InstallSnapshotResponse(
                    term=current_term,
                    success=False,
                    follower_id=self._cluster.node_id,
                )
                await self._send_install_snapshot_response(transport, response)
                return

        # Update snapshot and log state
        self._current_snapshot = Snapshot(
            last_included_index=last_included_index,
            last_included_term=last_included_term,
            data=snapshot_data,
        )

        async with self._cluster.election_lock:
            self._cluster.raft_log.install_snapshot(
                last_included_index, last_included_term
            )

        # Persist everything
        await self._persist_snapshot(self._current_snapshot)
        await self._persist_state()
        await self._persist_log()

        response = InstallSnapshotResponse(
            term=current_term,
            success=True,
            follower_id=self._cluster.node_id,
        )
        await self._send_install_snapshot_response(transport, response)
        log.info(f"Installed snapshot at index {last_included_index}")

    async def _send_install_snapshot_response(
        self, transport: Transport, response: InstallSnapshotResponse
    ) -> None:
        """Send InstallSnapshot response to leader."""
        response_data = {
            "term": response.term,
            "success": response.success,
            "follower_id": response.follower_id,
        }
        msg = WireMessage(
            msg_type=MessageType.INSTALL_SNAPSHOT_RES,
            target_name=response.follower_id,
            payload=serialize(response_data),
        )
        try:
            await transport.send(msg)
        except Exception as e:
            log.debug(f"Failed to send InstallSnapshot response: {e}")

    async def _handle_install_snapshot_response(self, msg: WireMessage) -> None:
        """Handle InstallSnapshot response from follower."""
        data = deserialize(msg.payload, {})
        term = data["term"]
        success = data["success"]
        follower_id = data["follower_id"]

        async with self._cluster.election_lock:
            # Step down if follower has higher term
            if term > self._cluster.term:
                self._cluster.term = term
                self._cluster.state = "follower"
                self._cluster.voted_for = None
                self._cluster.leader_id = None
                return

            if self._cluster.state != "leader":
                return

            if success and self._current_snapshot:
                # Update next_index and match_index
                self._cluster.next_index[follower_id] = (
                    self._current_snapshot.last_included_index + 1
                )
                self._cluster.match_index[follower_id] = (
                    self._current_snapshot.last_included_index
                )
                log.debug(f"Follower {follower_id[:8]} caught up via snapshot")

    def _should_send_snapshot(self, peer_id: str) -> bool:
        """Check if we should send a snapshot instead of AppendEntries."""
        if not self._current_snapshot:
            return False

        next_idx = self._cluster.next_index.get(peer_id, 1)
        first_log_index = (
            self._cluster.raft_log.entries[0].index
            if self._cluster.raft_log.entries
            else self._current_snapshot.last_included_index + 1
        )

        # Need snapshot if next_index is before our first log entry
        return next_idx < first_log_index

