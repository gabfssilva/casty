"""Raft log replication mixin."""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any, Callable, Coroutine

from ..raft_types import (
    AppendEntriesRequest,
    AppendEntriesResponse,
    LogEntry,
)
from ..serialize import deserialize, serialize
from ..transport import MessageType, Transport, WireMessage

if TYPE_CHECKING:
    from ..distributed import ClusterState

log = logging.getLogger(__name__)


class LogReplicationMixin:
    """Raft log replication for strong consistency."""

    _cluster: ClusterState
    _running: bool
    _apply_callback: Callable[[Any], Coroutine[Any, Any, None]] | None

    def _init_log_replication(self) -> None:
        """Initialize log replication state."""
        self._apply_callback = None

    def set_apply_callback(
        self, callback: Callable[[Any], Coroutine[Any, Any, None]]
    ) -> None:
        """Set callback for applying committed commands to state machine."""
        self._apply_callback = callback

    async def append_command(self, command: Any) -> bool:
        """Append a new command to the log (leader only).

        Returns True if the command was successfully replicated to a majority.
        """
        async with self._cluster.election_lock:
            if self._cluster.state != "leader":
                return False

            # Create new log entry
            new_index = self._cluster.raft_log.last_log_index + 1
            entry = LogEntry(
                term=self._cluster.term,
                index=new_index,
                command=command,
            )
            self._cluster.raft_log.append(entry)

            # Initialize replication tracking for this entry
            peers_snapshot = list(self._cluster.known_nodes.items())
            term = self._cluster.term

        # Persist log before responding
        await self._persist_log()

        # Trigger immediate replication
        await self._replicate_to_followers(peers_snapshot, term)

        # Wait for majority acknowledgment
        return await self._wait_for_commit(new_index, timeout=5.0)

    async def _wait_for_commit(self, index: int, timeout: float = 5.0) -> bool:
        """Wait until the given index is committed."""
        elapsed = 0.0
        poll_interval = 0.05
        while elapsed < timeout:
            if self._cluster.raft_log.commit_index >= index:
                return True
            await asyncio.sleep(poll_interval)
            elapsed += poll_interval
        return False

    async def _replicate_to_followers(
        self, peers: list[tuple[str, Transport]], term: int
    ) -> None:
        """Send AppendEntries to all followers."""
        for peer_id, transport in peers:
            asyncio.create_task(
                self._send_append_entries_to_peer(peer_id, transport, term)
            )

    async def _send_append_entries_to_peer(
        self, peer_id: str, transport: Transport, term: int
    ) -> None:
        """Send AppendEntries RPC to a single peer."""
        async with self._cluster.election_lock:
            if self._cluster.state != "leader" or self._cluster.term != term:
                return

            next_idx = self._cluster.next_index.get(peer_id, 1)
            prev_log_index = next_idx - 1
            prev_log_term = self._cluster.raft_log.get_term_at(prev_log_index)

            # Get entries to send
            entries = self._cluster.raft_log.get_entries_from(next_idx)
            entries_tuple = tuple(entries)

            request = AppendEntriesRequest(
                term=term,
                leader_id=self._cluster.node_id,
                prev_log_index=prev_log_index,
                prev_log_term=prev_log_term,
                entries=entries_tuple,
                leader_commit=self._cluster.raft_log.commit_index,
            )

        try:
            msg = WireMessage(
                msg_type=MessageType.APPEND_ENTRIES_REQ,
                target_name=self._cluster.node_id,
                payload=serialize(self._append_entries_to_dict(request)),
            )
            await transport.send(msg)
        except Exception as e:
            log.debug(f"Failed to send AppendEntries to {peer_id[:8]}: {e}")

    def _append_entries_to_dict(self, req: AppendEntriesRequest) -> dict[str, Any]:
        """Convert AppendEntriesRequest to dict for serialization."""
        return {
            "term": req.term,
            "leader_id": req.leader_id,
            "prev_log_index": req.prev_log_index,
            "prev_log_term": req.prev_log_term,
            "entries": [
                {"term": e.term, "index": e.index, "command": e.command}
                for e in req.entries
            ],
            "leader_commit": req.leader_commit,
        }

    def _dict_to_append_entries(self, data: dict[str, Any]) -> AppendEntriesRequest:
        """Convert dict to AppendEntriesRequest."""
        entries = tuple(
            LogEntry(term=e["term"], index=e["index"], command=e["command"])
            for e in data["entries"]
        )
        return AppendEntriesRequest(
            term=data["term"],
            leader_id=data["leader_id"],
            prev_log_index=data["prev_log_index"],
            prev_log_term=data["prev_log_term"],
            entries=entries,
            leader_commit=data["leader_commit"],
        )

    async def _handle_append_entries_request(
        self, transport: Transport, msg: WireMessage
    ) -> None:
        """Handle incoming AppendEntries RPC from leader."""
        data = deserialize(msg.payload, {})
        request = self._dict_to_append_entries(data)

        async with self._cluster.election_lock:
            # Reply false if term < currentTerm
            if request.term < self._cluster.term:
                response = AppendEntriesResponse(
                    term=self._cluster.term,
                    success=False,
                    follower_id=self._cluster.node_id,
                    match_index=0,
                )
                await self._send_append_entries_response(transport, response)
                return

            # Update term if leader has higher term
            if request.term > self._cluster.term:
                self._cluster.term = request.term
                self._cluster.voted_for = None

            # Recognize leader
            self._cluster.state = "follower"
            self._cluster.leader_id = request.leader_id
            self._reset_election_timeout()

            # Consistency check: verify log matches at prev_log_index
            if request.prev_log_index > 0:
                prev_term = self._cluster.raft_log.get_term_at(request.prev_log_index)
                if prev_term != request.prev_log_term:
                    # Log doesn't match - reject
                    response = AppendEntriesResponse(
                        term=self._cluster.term,
                        success=False,
                        follower_id=self._cluster.node_id,
                        match_index=0,
                    )
                    await self._send_append_entries_response(transport, response)
                    return

            # Delete conflicting entries and append new ones
            if request.entries:
                for entry in request.entries:
                    existing = self._cluster.raft_log.get_entry(entry.index)
                    if existing and existing.term != entry.term:
                        # Conflict - truncate from this point
                        self._cluster.raft_log.truncate_from(entry.index)
                    if not existing or existing.term != entry.term:
                        self._cluster.raft_log.append(entry)

            # Update commit index
            if request.leader_commit > self._cluster.raft_log.commit_index:
                last_new_index = (
                    request.entries[-1].index if request.entries else request.prev_log_index
                )
                self._cluster.raft_log.commit_index = min(
                    request.leader_commit, last_new_index
                )

            match_index = self._cluster.raft_log.last_log_index
            term = self._cluster.term

        # Persist state before responding
        await self._persist_state()
        await self._persist_log()

        # Apply committed entries
        await self._apply_committed_entries()

        response = AppendEntriesResponse(
            term=term,
            success=True,
            follower_id=self._cluster.node_id,
            match_index=match_index,
        )
        await self._send_append_entries_response(transport, response)

    async def _send_append_entries_response(
        self, transport: Transport, response: AppendEntriesResponse
    ) -> None:
        """Send AppendEntries response to leader."""
        response_data = {
            "term": response.term,
            "success": response.success,
            "follower_id": response.follower_id,
            "match_index": response.match_index,
        }
        msg = WireMessage(
            msg_type=MessageType.APPEND_ENTRIES_RES,
            target_name=response.follower_id,
            payload=serialize(response_data),
        )
        try:
            await transport.send(msg)
        except Exception as e:
            log.debug(f"Failed to send AppendEntries response: {e}")

    async def _handle_append_entries_response(self, msg: WireMessage) -> None:
        """Handle AppendEntries response from follower."""
        data = deserialize(msg.payload, {})
        term = data["term"]
        success = data["success"]
        follower_id = data["follower_id"]
        match_index = data["match_index"]

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

            if success:
                # Update next_index and match_index for follower
                self._cluster.next_index[follower_id] = match_index + 1
                self._cluster.match_index[follower_id] = match_index

                # Try to advance commit_index
                self._try_advance_commit_index()
            else:
                # Decrement next_index and retry
                current_next = self._cluster.next_index.get(follower_id, 1)
                self._cluster.next_index[follower_id] = max(1, current_next - 1)

        # Persist and apply if commit advanced
        await self._persist_state()
        await self._apply_committed_entries()

        # Retry replication if failed
        if not success:
            transport = self._cluster.known_nodes.get(follower_id)
            if transport:
                await self._send_append_entries_to_peer(
                    follower_id, transport, self._cluster.term
                )

    def _try_advance_commit_index(self) -> None:
        """Try to advance commit_index based on match_index values."""
        # Find the highest N such that:
        # - N > commit_index
        # - A majority of match_index[i] >= N
        # - log[N].term == currentTerm
        for n in range(
            self._cluster.raft_log.last_log_index,
            self._cluster.raft_log.commit_index,
            -1,
        ):
            entry = self._cluster.raft_log.get_entry(n)
            if not entry or entry.term != self._cluster.term:
                continue

            # Count replications (including self)
            count = 1  # Self
            for match_idx in self._cluster.match_index.values():
                if match_idx >= n:
                    count += 1

            # Check if majority
            total = self._cluster.expected_size
            if count > total // 2:
                self._cluster.raft_log.commit_index = n
                log.debug(f"Advanced commit_index to {n}")
                break

    async def _apply_committed_entries(self) -> None:
        """Apply committed but not yet applied entries to state machine."""
        while self._cluster.raft_log.last_applied < self._cluster.raft_log.commit_index:
            next_apply = self._cluster.raft_log.last_applied + 1
            entry = self._cluster.raft_log.get_entry(next_apply)
            if entry and self._apply_callback:
                try:
                    await self._apply_callback(entry.command)
                except Exception as e:
                    log.error(f"Failed to apply command at index {next_apply}: {e}")
            self._cluster.raft_log.last_applied = next_apply

    async def _send_heartbeats_with_entries(self) -> None:
        """Send heartbeats (empty AppendEntries) to all followers."""
        async with self._cluster.election_lock:
            if self._cluster.state != "leader":
                return
            peers_snapshot = list(self._cluster.known_nodes.items())
            term = self._cluster.term

        for peer_id, transport in peers_snapshot:
            await self._send_append_entries_to_peer(peer_id, transport, term)

