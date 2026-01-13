"""Self-contained Raft group implementation."""

from __future__ import annotations

import asyncio
import logging
import random
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Coroutine, Literal

from ..raft_types import (
    LogEntry,
    RaftLog,
    Snapshot,
    SingletonCommand,
)

log = logging.getLogger(__name__)

# Election timing defaults
HEARTBEAT_INTERVAL = 0.3
ELECTION_TIMEOUT_MIN = 1.0
ELECTION_TIMEOUT_MAX = 2.0


@dataclass
class RaftGroupState:
    """Persistent and volatile state for a single Raft group."""

    group_id: int
    node_id: str
    expected_size: int = 3
    # Timing
    heartbeat_interval: float = HEARTBEAT_INTERVAL
    election_timeout_min: float = ELECTION_TIMEOUT_MIN
    election_timeout_max: float = ELECTION_TIMEOUT_MAX
    # Persistent state
    term: int = 0
    voted_for: str | None = None
    raft_log: RaftLog = field(default_factory=RaftLog)
    # Volatile state
    state: Literal["follower", "candidate", "leader"] = "follower"
    leader_id: str | None = None
    last_heartbeat: float = field(default_factory=time.time)
    election_timeout: float = 0.0
    last_heartbeat_sent: float = 0.0
    votes_received: set[str] = field(default_factory=set)
    # Leader volatile state
    next_index: dict[str, int] = field(default_factory=dict)
    match_index: dict[str, int] = field(default_factory=dict)
    # Snapshot
    current_snapshot: Snapshot | None = None

    def __post_init__(self) -> None:
        """Initialize election timeout with random jitter."""
        if self.election_timeout == 0.0:
            self.election_timeout = random.uniform(
                self.election_timeout_min, self.election_timeout_max
            )

    @property
    def quorum_size(self) -> int:
        """Minimum nodes needed for quorum (majority)."""
        return (self.expected_size // 2) + 1


class RaftGroup:
    """Self-contained Raft consensus group.

    Encapsulates all Raft state and logic for ONE group.
    Multiple RaftGroup instances can run independently.
    """

    def __init__(
        self,
        group_id: int,
        node_id: str,
        expected_size: int = 3,
        *,
        heartbeat_interval: float = HEARTBEAT_INTERVAL,
        election_timeout_min: float = ELECTION_TIMEOUT_MIN,
        election_timeout_max: float = ELECTION_TIMEOUT_MAX,
    ) -> None:
        self._state = RaftGroupState(
            group_id=group_id,
            node_id=node_id,
            expected_size=expected_size,
            heartbeat_interval=heartbeat_interval,
            election_timeout_min=election_timeout_min,
            election_timeout_max=election_timeout_max,
        )
        self._lock = asyncio.Lock()
        self._apply_callback: Callable[[Any], Coroutine[Any, Any, None]] | None = None
        self._singleton_apply_callback: Callable[[SingletonCommand], Coroutine[Any, Any, None]] | None = None
        self._snapshot_callback: Callable[[], Coroutine[Any, Any, bytes]] | None = None
        self._restore_callback: Callable[[bytes], Coroutine[Any, Any, None]] | None = None
        self._running = True

    @property
    def group_id(self) -> int:
        return self._state.group_id

    @property
    def node_id(self) -> str:
        return self._state.node_id

    @property
    def term(self) -> int:
        return self._state.term

    @property
    def voted_for(self) -> str | None:
        return self._state.voted_for

    @property
    def is_leader(self) -> bool:
        return self._state.state == "leader"

    @property
    def leader_id(self) -> str | None:
        return self._state.leader_id

    @property
    def commit_index(self) -> int:
        return self._state.raft_log.commit_index

    @property
    def last_applied(self) -> int:
        return self._state.raft_log.last_applied

    @property
    def last_log_index(self) -> int:
        return self._state.raft_log.last_log_index

    @property
    def last_log_term(self) -> int:
        return self._state.raft_log.last_log_term

    @property
    def raft_log(self) -> RaftLog:
        return self._state.raft_log

    @property
    def current_snapshot(self) -> Snapshot | None:
        return self._state.current_snapshot

    @property
    def lock(self) -> asyncio.Lock:
        return self._lock

    @property
    def state(self) -> RaftGroupState:
        return self._state

    def stop(self) -> None:
        """Stop the group (for shutdown)."""
        self._running = False

    def set_apply_callback(
        self, callback: Callable[[Any], Coroutine[Any, Any, None]]
    ) -> None:
        """Set callback for applying committed commands to state machine."""
        self._apply_callback = callback

    def set_singleton_apply_callback(
        self, callback: Callable[[SingletonCommand], Coroutine[Any, Any, None]]
    ) -> None:
        """Set callback for applying singleton commands."""
        self._singleton_apply_callback = callback

    def set_snapshot_callbacks(
        self,
        snapshot_cb: Callable[[], Coroutine[Any, Any, bytes]],
        restore_cb: Callable[[bytes], Coroutine[Any, Any, None]],
    ) -> None:
        """Set callbacks for creating and restoring snapshots."""
        self._snapshot_callback = snapshot_cb
        self._restore_callback = restore_cb

    # ---- Election Methods ----

    def _has_quorum(self, known_nodes_count: int) -> bool:
        """Check if we can see enough nodes for quorum."""
        visible = known_nodes_count + 1  # +1 for self
        return visible >= self._state.quorum_size

    def _reset_election_timeout(self) -> None:
        """Reset election timeout with random jitter."""
        self._state.last_heartbeat = time.time()
        self._state.election_timeout = random.uniform(
            self._state.election_timeout_min,
            self._state.election_timeout_max,
        )

    def _election_timed_out(self) -> bool:
        """Check if election timeout has elapsed."""
        return time.time() - self._state.last_heartbeat > self._state.election_timeout

    def _cluster_fully_discovered(self, known_nodes_count: int) -> bool:
        """Check if we've discovered all expected cluster members."""
        discovered = known_nodes_count + 1  # +1 for self
        return discovered >= self._state.expected_size

    async def start_election(self, known_nodes_count: int) -> dict[str, Any] | None:
        """Start an election. Returns vote request data or None if cannot start.

        Caller is responsible for sending vote requests to peers.
        """
        async with self._lock:
            # For first election (term 0), require full cluster discovery
            if self._state.term == 0 and not self._cluster_fully_discovered(known_nodes_count):
                log.debug(
                    f"Group {self._state.group_id}: Node {self._state.node_id[:8]}: "
                    f"Waiting for cluster discovery ({known_nodes_count + 1}/{self._state.expected_size})"
                )
                self._reset_election_timeout()
                return None

            if not self._has_quorum(known_nodes_count):
                log.debug(
                    f"Group {self._state.group_id}: Node {self._state.node_id[:8]}: "
                    f"No quorum, can't start election"
                )
                self._reset_election_timeout()
                return None

            self._state.term += 1
            self._state.state = "candidate"
            self._state.voted_for = self._state.node_id
            self._state.votes_received = {self._state.node_id}  # Vote for self
            self._reset_election_timeout()

            vote_data = {
                "group_id": self._state.group_id,
                "term": self._state.term,
                "candidate_id": self._state.node_id,
                "last_log_index": self._state.raft_log.last_log_index,
                "last_log_term": self._state.raft_log.last_log_term,
            }

        log.info(
            f"Group {self._state.group_id}: Node {self._state.node_id[:8]}: "
            f"Starting election for term {vote_data['term']}"
        )
        return vote_data

    def _is_candidate_log_up_to_date(
        self, candidate_last_index: int, candidate_last_term: int
    ) -> bool:
        """Check if candidate's log is at least as up-to-date as ours."""
        my_last = (self._state.raft_log.last_log_term, self._state.raft_log.last_log_index)
        candidate_last = (candidate_last_term, candidate_last_index)
        return candidate_last >= my_last

    async def handle_vote_request(
        self, data: dict[str, Any]
    ) -> dict[str, Any]:
        """Handle incoming vote request. Returns vote response data."""
        candidate_term = data["term"]
        candidate_id = data["candidate_id"]
        candidate_last_log_index = data.get("last_log_index", 0)
        candidate_last_log_term = data.get("last_log_term", 0)

        async with self._lock:
            vote_granted = False

            # If candidate's term is higher, update our term and become follower
            if candidate_term > self._state.term:
                self._state.term = candidate_term
                self._state.state = "follower"
                self._state.voted_for = None
                self._state.leader_id = None

            # Grant vote if conditions are met
            log_ok = self._is_candidate_log_up_to_date(
                candidate_last_log_index, candidate_last_log_term
            )

            if (
                candidate_term >= self._state.term
                and self._state.voted_for in (None, candidate_id)
                and log_ok
            ):
                vote_granted = True
                self._state.voted_for = candidate_id
                self._reset_election_timeout()
                log.debug(
                    f"Group {self._state.group_id}: Node {self._state.node_id[:8]}: "
                    f"Voted for {candidate_id[:8]} in term {candidate_term}"
                )
            elif not log_ok:
                log.debug(
                    f"Group {self._state.group_id}: Node {self._state.node_id[:8]}: "
                    f"Rejected vote for {candidate_id[:8]} (log not up-to-date)"
                )

            return {
                "group_id": self._state.group_id,
                "term": self._state.term,
                "vote_granted": vote_granted,
                "voter_id": self._state.node_id,
            }

    async def handle_vote_response(self, data: dict[str, Any]) -> bool:
        """Handle incoming vote response. Returns True if became leader."""
        term = data["term"]
        vote_granted = data["vote_granted"]
        voter_id = data["voter_id"]

        async with self._lock:
            if self._state.state != "candidate":
                return False

            # If response has higher term, step down
            if term > self._state.term:
                self._state.term = term
                self._state.state = "follower"
                self._state.voted_for = None
                self._state.leader_id = None
                return False

            if vote_granted and term == self._state.term:
                self._state.votes_received.add(voter_id)
                log.debug(
                    f"Group {self._state.group_id}: Node {self._state.node_id[:8]}: "
                    f"Got vote from {voter_id[:8]}, total: {len(self._state.votes_received)}"
                )
                return self._check_election_won()

        return False

    def _check_election_won(self) -> bool:
        """Check if we have won the election. Returns True if just became leader."""
        if self._state.state != "candidate":
            return False

        if len(self._state.votes_received) >= self._state.quorum_size:
            self._state.state = "leader"
            self._state.leader_id = self._state.node_id
            log.info(
                f"Group {self._state.group_id}: Node {self._state.node_id[:8]}: "
                f"Became LEADER with {len(self._state.votes_received)}/{self._state.expected_size} votes "
                f"(term {self._state.term})"
            )
            return True
        return False

    async def on_become_leader(self, peer_ids: list[str]) -> None:
        """Initialize leader state when becoming leader."""
        async with self._lock:
            last_log_index = self._state.raft_log.last_log_index
            for peer_id in peer_ids:
                self._state.next_index[peer_id] = last_log_index + 1
                self._state.match_index[peer_id] = 0

    # ---- Log Replication Methods ----

    async def append_command(self, command: Any) -> int | None:
        """Append a new command to the log (leader only).

        Returns the new entry's index, or None if not leader.
        """
        async with self._lock:
            if self._state.state != "leader":
                return None

            new_index = self._state.raft_log.last_log_index + 1
            entry = LogEntry(
                term=self._state.term,
                index=new_index,
                command=command,
            )
            self._state.raft_log.append(entry)
            # Try to advance commit index (for single-node clusters)
            self._try_advance_commit_index()

        # Apply committed entries (important for single-node clusters)
        await self.apply_committed_entries()
        return new_index

    def get_append_entries_for_peer(self, peer_id: str) -> dict[str, Any] | None:
        """Get AppendEntries data to send to a peer. Returns None if not leader."""
        if self._state.state != "leader":
            return None

        next_idx = self._state.next_index.get(peer_id, 1)
        prev_log_index = next_idx - 1
        prev_log_term = self._state.raft_log.get_term_at(prev_log_index)
        entries = self._state.raft_log.get_entries_from(next_idx)

        return {
            "group_id": self._state.group_id,
            "term": self._state.term,
            "leader_id": self._state.node_id,
            "prev_log_index": prev_log_index,
            "prev_log_term": prev_log_term,
            "entries": [
                {"term": e.term, "index": e.index, "command": e.command}
                for e in entries
            ],
            "leader_commit": self._state.raft_log.commit_index,
        }

    async def handle_append_entries_request(
        self, data: dict[str, Any]
    ) -> dict[str, Any]:
        """Handle incoming AppendEntries RPC. Returns response data."""
        term = data["term"]
        leader_id = data["leader_id"]
        prev_log_index = data["prev_log_index"]
        prev_log_term = data["prev_log_term"]
        entries_data = data.get("entries", [])
        leader_commit = data["leader_commit"]

        entries = tuple(
            LogEntry(term=e["term"], index=e["index"], command=e["command"])
            for e in entries_data
        )

        async with self._lock:
            # Reply false if term < currentTerm
            if term < self._state.term:
                return {
                    "group_id": self._state.group_id,
                    "term": self._state.term,
                    "success": False,
                    "follower_id": self._state.node_id,
                    "match_index": 0,
                }

            # Update term if leader has higher term
            if term > self._state.term:
                self._state.term = term
                self._state.voted_for = None

            # Recognize leader
            self._state.state = "follower"
            self._state.leader_id = leader_id
            self._reset_election_timeout()

            # Consistency check: verify log matches at prev_log_index
            if prev_log_index > 0:
                prev_term = self._state.raft_log.get_term_at(prev_log_index)
                if prev_term != prev_log_term:
                    return {
                        "group_id": self._state.group_id,
                        "term": self._state.term,
                        "success": False,
                        "follower_id": self._state.node_id,
                        "match_index": 0,
                    }

            # Delete conflicting entries and append new ones
            if entries:
                for entry in entries:
                    existing = self._state.raft_log.get_entry(entry.index)
                    if existing and existing.term != entry.term:
                        self._state.raft_log.truncate_from(entry.index)
                    if not existing or existing.term != entry.term:
                        self._state.raft_log.append(entry)

            # Update commit index
            if leader_commit > self._state.raft_log.commit_index:
                last_new_index = (
                    entries[-1].index if entries else prev_log_index
                )
                self._state.raft_log.commit_index = min(leader_commit, last_new_index)

            return {
                "group_id": self._state.group_id,
                "term": self._state.term,
                "success": True,
                "follower_id": self._state.node_id,
                "match_index": self._state.raft_log.last_log_index,
            }

    async def handle_append_entries_response(
        self, data: dict[str, Any]
    ) -> bool:
        """Handle AppendEntries response. Returns True if retry needed."""
        term = data["term"]
        success = data["success"]
        follower_id = data["follower_id"]
        match_index = data["match_index"]

        async with self._lock:
            # Step down if follower has higher term
            if term > self._state.term:
                self._state.term = term
                self._state.state = "follower"
                self._state.voted_for = None
                self._state.leader_id = None
                return False

            if self._state.state != "leader":
                return False

            if success:
                self._state.next_index[follower_id] = match_index + 1
                self._state.match_index[follower_id] = match_index
                self._try_advance_commit_index()
                return False
            else:
                current_next = self._state.next_index.get(follower_id, 1)
                self._state.next_index[follower_id] = max(1, current_next - 1)
                return True

    def _try_advance_commit_index(self) -> None:
        """Try to advance commit_index based on match_index values."""
        for n in range(
            self._state.raft_log.last_log_index,
            self._state.raft_log.commit_index,
            -1,
        ):
            entry = self._state.raft_log.get_entry(n)
            if not entry or entry.term != self._state.term:
                continue

            count = 1  # Self
            for match_idx in self._state.match_index.values():
                if match_idx >= n:
                    count += 1

            total = self._state.expected_size
            if count > total // 2:
                self._state.raft_log.commit_index = n
                log.debug(
                    f"Group {self._state.group_id}: Advanced commit_index to {n}"
                )
                break

    async def apply_committed_entries(self) -> None:
        """Apply committed but not yet applied entries to state machine."""
        while self._state.raft_log.last_applied < self._state.raft_log.commit_index:
            next_apply = self._state.raft_log.last_applied + 1
            entry = self._state.raft_log.get_entry(next_apply)
            if entry:
                if isinstance(entry.command, SingletonCommand):
                    if self._singleton_apply_callback:
                        try:
                            await self._singleton_apply_callback(entry.command)
                        except Exception as e:
                            log.error(
                                f"Group {self._state.group_id}: "
                                f"Failed to apply singleton command at index {next_apply}: {e}"
                            )
                elif self._apply_callback:
                    try:
                        await self._apply_callback(entry.command)
                    except Exception as e:
                        log.error(
                            f"Group {self._state.group_id}: "
                            f"Failed to apply command at index {next_apply}: {e}"
                        )
            self._state.raft_log.last_applied = next_apply

    async def wait_for_commit(self, index: int, timeout: float = 5.0) -> bool:
        """Wait until the given index is committed."""
        elapsed = 0.0
        poll_interval = 0.05
        while elapsed < timeout:
            if not self._running:
                return False
            if self._state.raft_log.commit_index >= index:
                return True
            await asyncio.sleep(poll_interval)
            elapsed += poll_interval
        return False

    # ---- Snapshot Methods ----

    async def create_snapshot(self) -> Snapshot | None:
        """Create a snapshot of current state machine state."""
        if not self._snapshot_callback:
            return None

        async with self._lock:
            last_applied = self._state.raft_log.last_applied
            if last_applied == 0:
                return None

            entry = self._state.raft_log.get_entry(last_applied)
            if not entry:
                return None

            last_term = entry.term

        data = await self._snapshot_callback()

        snapshot = Snapshot(
            last_included_index=last_applied,
            last_included_term=last_term,
            data=data,
        )

        async with self._lock:
            self._state.current_snapshot = snapshot
            self._state.raft_log.install_snapshot(last_applied, last_term)

        log.info(f"Group {self._state.group_id}: Created snapshot at index {last_applied}")
        return snapshot

    def get_install_snapshot_data(self) -> dict[str, Any] | None:
        """Get InstallSnapshot data. Returns None if no snapshot or not leader."""
        if not self._state.current_snapshot or self._state.state != "leader":
            return None

        return {
            "group_id": self._state.group_id,
            "term": self._state.term,
            "leader_id": self._state.node_id,
            "last_included_index": self._state.current_snapshot.last_included_index,
            "last_included_term": self._state.current_snapshot.last_included_term,
            "data": self._state.current_snapshot.data.hex(),
            "done": True,
        }

    async def handle_install_snapshot_request(
        self, data: dict[str, Any]
    ) -> dict[str, Any]:
        """Handle incoming InstallSnapshot RPC. Returns response data."""
        term = data["term"]
        leader_id = data["leader_id"]
        last_included_index = data["last_included_index"]
        last_included_term = data["last_included_term"]
        snapshot_data = bytes.fromhex(data["data"])

        async with self._lock:
            if term < self._state.term:
                return {
                    "group_id": self._state.group_id,
                    "term": self._state.term,
                    "success": False,
                    "follower_id": self._state.node_id,
                }

            if term > self._state.term:
                self._state.term = term
                self._state.voted_for = None

            self._state.state = "follower"
            self._state.leader_id = leader_id
            self._reset_election_timeout()
            current_term = self._state.term

        # Restore state machine from snapshot (outside lock)
        if self._restore_callback:
            try:
                await self._restore_callback(snapshot_data)
            except Exception as e:
                log.error(f"Group {self._state.group_id}: Failed to restore snapshot: {e}")
                return {
                    "group_id": self._state.group_id,
                    "term": current_term,
                    "success": False,
                    "follower_id": self._state.node_id,
                }

        # Update snapshot and log state
        async with self._lock:
            self._state.current_snapshot = Snapshot(
                last_included_index=last_included_index,
                last_included_term=last_included_term,
                data=snapshot_data,
            )
            self._state.raft_log.install_snapshot(last_included_index, last_included_term)

        log.info(
            f"Group {self._state.group_id}: Installed snapshot at index {last_included_index}"
        )
        return {
            "group_id": self._state.group_id,
            "term": current_term,
            "success": True,
            "follower_id": self._state.node_id,
        }

    async def handle_install_snapshot_response(self, data: dict[str, Any]) -> None:
        """Handle InstallSnapshot response from follower."""
        term = data["term"]
        success = data["success"]
        follower_id = data["follower_id"]

        async with self._lock:
            if term > self._state.term:
                self._state.term = term
                self._state.state = "follower"
                self._state.voted_for = None
                self._state.leader_id = None
                return

            if self._state.state != "leader":
                return

            if success and self._state.current_snapshot:
                self._state.next_index[follower_id] = (
                    self._state.current_snapshot.last_included_index + 1
                )
                self._state.match_index[follower_id] = (
                    self._state.current_snapshot.last_included_index
                )
                log.debug(
                    f"Group {self._state.group_id}: "
                    f"Follower {follower_id[:8]} caught up via snapshot"
                )

    def should_send_snapshot(self, peer_id: str) -> bool:
        """Check if we should send a snapshot instead of AppendEntries."""
        if not self._state.current_snapshot:
            return False

        next_idx = self._state.next_index.get(peer_id, 1)
        first_log_index = (
            self._state.raft_log.entries[0].index
            if self._state.raft_log.entries
            else self._state.current_snapshot.last_included_index + 1
        )

        return next_idx < first_log_index

    # ---- Tick (for election loop) ----

    def should_send_heartbeat(self) -> bool:
        """Check if leader should send heartbeat."""
        if self._state.state != "leader":
            return False
        now = time.time()
        if now - self._state.last_heartbeat_sent >= self._state.heartbeat_interval:
            self._state.last_heartbeat_sent = now
            return True
        return False

    def should_start_election(self) -> bool:
        """Check if should start election."""
        if self._state.state not in ("follower", "candidate"):
            return False
        return self._election_timed_out()

    # ---- State Management ----

    def get_persisted_state(self) -> dict[str, Any]:
        """Get state to persist."""
        return {
            "group_id": self._state.group_id,
            "term": self._state.term,
            "voted_for": self._state.voted_for,
        }

    def get_persisted_log(self) -> dict[str, Any]:
        """Get log to persist."""
        return {
            "group_id": self._state.group_id,
            "entries": [
                {"term": e.term, "index": e.index, "command": e.command}
                for e in self._state.raft_log.entries
            ],
            "commit_index": self._state.raft_log.commit_index,
            "last_applied": self._state.raft_log.last_applied,
        }

    def get_persisted_snapshot(self) -> dict[str, Any] | None:
        """Get snapshot to persist."""
        if not self._state.current_snapshot:
            return None
        return {
            "group_id": self._state.group_id,
            "last_included_index": self._state.current_snapshot.last_included_index,
            "last_included_term": self._state.current_snapshot.last_included_term,
            "data": self._state.current_snapshot.data.hex(),
        }

    def restore_state(self, data: dict[str, Any]) -> None:
        """Restore persisted state."""
        self._state.term = data.get("term", 0)
        self._state.voted_for = data.get("voted_for")

    def restore_log(self, data: dict[str, Any]) -> None:
        """Restore persisted log."""
        entries = [
            LogEntry(term=e["term"], index=e["index"], command=e["command"])
            for e in data.get("entries", [])
        ]
        self._state.raft_log = RaftLog(
            entries=entries,
            commit_index=data.get("commit_index", 0),
            last_applied=data.get("last_applied", 0),
        )

    def restore_snapshot(self, data: dict[str, Any]) -> None:
        """Restore persisted snapshot."""
        self._state.current_snapshot = Snapshot(
            last_included_index=data["last_included_index"],
            last_included_term=data["last_included_term"],
            data=bytes.fromhex(data["data"]),
        )

    def remove_peer(self, peer_id: str) -> None:
        """Remove a peer from tracking."""
        self._state.next_index.pop(peer_id, None)
        self._state.match_index.pop(peer_id, None)
